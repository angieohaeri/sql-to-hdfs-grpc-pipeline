# SQL to HDFS gRPC Pipeline: Project Overview
This project implements a distributed data system that ingests generated bank loan data from a MySQL database, stores it in HDFS, and exposes analytics via a gRPC service. The system is containerized with Docker and designed to be fault tolerant under several types of DataNode failures.

The pipeline supports efficient and optimized querying through Parquet partitioning and includes a performance analysis comparing full dataset scans to partition reads.

## Key Features

- SQL data ingestion from MySQL with schema exploration and joins
- HDFS storage using PyArrow and Parquet
- gRPC server exposing data ingestion and analytics APIs
- Block-level analysis using WebHDFS
- Parquet partitioning for query optimization
- Fault tolerance handling DataNode failures gracefully
- Automated performance benchmarking and visualization
  
## Technologies Used

- Python
- MySQL
- Apache HDFS
- PyArrow & Parquet
- gRPC
- Docker & Docker Compose
- Subprocess automation and performance profiling 

## Academic Context

This project was completed as part of a university course on distributed big data systems and data engineering. The implementation and analysis were completed individually.
