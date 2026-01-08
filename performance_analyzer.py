import subprocess
import time
import os
import pandas as pd
import matplotlib.pyplot as plt


ctycodes = [55001, 55003, 55027, 55059, 55133] # sample county codes to measure performance
partitions = "/partitions"
client = "p4-server-1"
cmd = ["docker", "exec", client, "python3", "/client.py"]
output = "outputs"

os.makedirs(output, exist_ok=True)

def remove_partitions():
    subprocess.run(
        ["docker", "exec", client,"hdfs", "dfs","-rm", "-r", "-f", partitions],
        check=True)

def run_client(county_code):
    return subprocess.run(
        cmd + ["CalcAvgLoan", "-c", str(county_code)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True)

# measuring duration of client call
def time_client_call(county_code):
    start = time.monotonic()
    run_client(county_code)
    end = time.monotonic()
    duration = end - start
    return duration

def main():
    create_times = []
    reuse_times = []

    print("Starting performance measurements...")

    for code in ctycodes:
        print(f"Testing county code: {code}")

        remove_partitions()
        print("rm and rf worked")

        create_time = time_client_call(code)
        print(f"Create time: {create_time:.3f}s")
        create_times.append(create_time)

        reuse_time = time_client_call(code)
        print(f"Reuse time: {reuse_time:.3f}s")
        reuse_times.append(reuse_time)

    avg_create = sum(create_times) / len(create_times)
    avg_reuse = sum(reuse_times) / len(reuse_times)

    df = pd.DataFrame([
        {"operation": "create", "time": round(avg_create, 3)},
        {"operation": "reuse", "time": round(avg_reuse, 3)}
    ])
    csv_path = os.path.join(output, "performance_results.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved performance results to {csv_path}")

    # creating performance plot
    plt.figure(figsize = (6, 4))
    plt.bar(df["operation"], df["time"], color = ["blue", "green"])
    plt.ylabel("Average Time (seconds)")
    plt.title("Performance Analysis: Create vs. Reuse")
    plt.tight_layout()

    plot_path = os.path.join(output, "performance_analysis.png")
    plt.savefig(plot_path)
    print(f"Saved performance plot to {plot_path}")

if __name__ == "__main__":
    main()
