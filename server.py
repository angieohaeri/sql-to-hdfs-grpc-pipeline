import time
import grpc
from concurrent import futures
import lender_pb2
import lender_pb2_grpc
from sqlalchemy import create_engine, text
import mysql.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs  
import io
import os
import sys
import requests
import math
import pyarrow.compute as pc
import traceback


class LenderServicer(lender_pb2_grpc.LenderServicer):
    def __init__(self):
        try:
            self.client = pafs.HadoopFileSystem(host='boss', port=9000, replication=2, default_block_size=1024*1024)
            self.partition = pafs.HadoopFileSystem(host='boss', port=9000, replication=1, default_block_size=1024*1024)
            print("Connected to HDFS Successfully")
        except Exception as e:
            self.client = None
            print(f"Failed to connect to HDFS: {e}")

        self.nn_url = 'http://boss:9870' 
        self.filepath = sys.argv[0]

    # streams data from MySQL to HDFS 
    def DbToHdfs(self, request, context):
        try:
            conn = None
            for i in range(10):
                try:
                    conn = create_engine("mysql+mysqlconnector://root:abc@mysql:3306/CS544")
                    conn = conn.connect()
                    break
                except Exception as e:
                    print(f"DB connection attempt {i+1} failed: {e}")
                    time.sleep(5)
            else:
                return lender_pb2.StatusString(status="Failed to connect to MySQL after retries")

            query = """
                SELECT *
                FROM loans l
                INNER JOIN loan_types lt ON l.loan_type_id = lt.id
                WHERE loan_amount > 30000 AND loan_amount < 800000
            """
            df = pd.read_sql(query, conn)

            # if len(df) != 426716:
            #     print(f"Warning: row count {len(df)} != 426716")

            table = pa.Table.from_pandas(df)

            if self.client is None:
                return lender_pb2.StatusString(status="HDFS client not initialized")

            hdfs_path = '/hdma-wi-2021.parquet'
            with self.client.open_output_stream(hdfs_path) as outstream:
                pa.parquet.write_table(table, outstream)
            print(f"Uploaded Parquet file to HDFS at: {hdfs_path}")

            return lender_pb2.StatusString(status="Upload to HDFS successful")

        except Exception as e:
            print(f"Error in DbToHdfs: {e}")
            return lender_pb2.StatusString(status=f"Error in DbToHdfs: {e}")

    # retrieves datanode block location of requested data
    def BlockLocations(self, request, context):
        hdfs_path = request.path
        if not hdfs_path:
            return lender_pb2.BlockLocationsResp(block_entries={}, error="File path not provided")

        if self.client is None:
            return lender_pb2.BlockLocationsResp(block_entries={}, error="HDFS client not initialized")

        block_counts = {}
        try:
            hdfs_url = f"{self.nn_url}/webhdfs/v1{hdfs_path}?op=GETFILEBLOCKLOCATIONS"
            response = requests.get(hdfs_url, allow_redirects=True) 
            response.raise_for_status()
            data = response.json()
            print(data)
            if 'BlockLocations' in data and 'BlockLocation' in data['BlockLocations']:
                for block in data['BlockLocations']['BlockLocation']:
                    if 'hosts' in block:
                        for host in block['hosts']:
                            block_counts[host] = block_counts.get(host, 0) + 1
            else:
                return lender_pb2.BlockLocationsResp(
                    block_entries={},
                    error=f"No block location information found for {hdfs_path}. WebHDFS response structure unexpected.")

            return lender_pb2.BlockLocationsResp(
                block_entries=block_counts,
                error="")

        except requests.exceptions.ConnectionError as e: # handle connection error
            return lender_pb2.BlockLocationsResp(
                block_entries={},
                error=f"Failed to connect to NameNode WebHDFS at {self.nn_url}: {e}")
        
        except requests.exceptions.RequestException as e: 
            return lender_pb2.BlockLocationsResp(
                block_entries={},
                error=f"Error retrieving block locations from WebHDFS: {e}. Response: {getattr(e.response, 'text', 'N/A')}")
        
        except Exception as e:
            print(f" Error in BlockLocations: {e}")
            return lender_pb2.BlockLocationsResp(block_entries={}, error = f"Unexpected error in BlockLocations: {e}")

    # calculates avg loan rate per county
    def CalcAvgLoan(self, request, context):
        county_code = request.county_code
        partition_path = f"/partitions/{county_code}.parquet"
        full_path = "/hdma-wi-2021.parquet"

        try:
            if self.partition is None:
                return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="", error = "HDFS client not initialized")


    ## detect datanode failure -> recreate lost county_code.pq file
    
            try:
                with self.partition.open_input_file(partition_path) as f:
                    tbl = pq.read_table(f)
                source = "reuse"

            except FileNotFoundError: 

                hdmaurl = f"{self.nn_url}/webhdfs/v1{full_path}?op=OPEN"
                response = requests.get(hdmaurl, allow_redirects=True)
                response.raise_for_status()

                buffer = io.BytesIO(response.content)
                tbl = pq.read_table(buffer)


                tbl = tbl.filter(pc.equal(tbl["county_code"], county_code))

                if tbl.num_rows == 0:
                    return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="create", error="No data for this county code")

                try:
                    self.partition.create_dir("/partitions")
                except:
                    pass 

                with self.partition.open_output_stream(partition_path) as out_stream:
                    pq.write_table(tbl, out_stream)

                source = "create" # switch source to create if file not found
            
            except OSError as e:
                print(f"A datanode has failed while reading {county_code}.parquet: {e}")

                hdmaurl = f"{self.nn_url}/webhdfs/v1{full_path}?op=OPEN"

                response = requests.get(hdmaurl, allow_redirects=True)
                response.raise_for_status()

                buffer = io.BytesIO(response.content)
                tbl = pq.read_table(buffer)

                tbl = tbl.filter(pc.equal(tbl["county_code"], county_code))

                if tbl.num_rows == 0:
                    return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="create", error="No data for this county code")

                try:
                    self.partition.create_dir("/partitions")
                except:
                    pass 

                with self.partition.open_output_stream(partition_path) as out_stream:
                    pq.write_table(tbl, out_stream)

                source = "recreate" # recreate source

            if tbl.num_rows == 0:
                avg_loan = 0
            else:
                total = pc.sum(tbl["loan_amount"]).as_py()
                avg_loan = math.floor(total / tbl.num_rows)

            return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")

        except Exception as e:
            print(f"Error in CalcAvgLoan: {e}")
            print(traceback.format_exc())
            return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="", error=f"Unexpected error: {e}")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)

    grpc_port = 5000
    server.add_insecure_port(f'[::]:{grpc_port}')
    server.start()
    print(f"gRPC server started on port {grpc_port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.stop(0)


if __name__ == "__main__":
    serve()
