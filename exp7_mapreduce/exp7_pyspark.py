"""
Nexus Smart City - Distributed Computing Lab Exp 7 (PySpark Implementation)
Module: Member 1 (Spark MapReduce Engine)

Follows the exact PySpark RDD programming model from Lab Exp 7:
 sc.textFile() -> map() -> reduceByKey() -> save
"""

import os
import sys

def run_spark_job():
    try:
        from pyspark import SparkContext, SparkConf
    except ImportError:
        print("[!] PySpark is not installed. To run this script:")
        print("    pip install pyspark")
        print("[i] Alternatively, you can run 'python exp7_mapreduce.py' which executes")
        print("    the standard streaming MapReduce pipeline without external daemons.")
        return

    base_dir = os.path.dirname(os.path.dirname(__file__))
    input_csv = os.path.join(base_dir, "data", "raw_traffic_sensor_logs.csv")
    
    if not os.path.exists(input_csv):
        print(f"[!] Input file {input_csv} not found. Run data/generate_traffic_logs.py first.")
        return

    conf = SparkConf().setAppName("NexusTrafficFlowMapReduce").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("ERROR")

    print("[*] Reading dataset with Apache Spark RDDs...")
    lines = sc.textFile(input_csv)
    header = lines.first()
    data_lines = lines.filter(lambda row: row != header)

    def parse_record(line):
        parts = line.split(",")
        # origin_id (col 2), dest_id (col 4), vehicle_count (col 6)
        origin = int(parts[2])
        dest = int(parts[4])
        count = int(parts[6])
        return ((origin, dest), count)

    # MapReduce core
    print("[*] Executing Spark Map -> ReduceByKey transformations...")
    flow_counts = (
        data_lines
        .map(parse_record)
        .reduceByKey(lambda a, b: a + b)
    )

    results = flow_counts.collect()
    print(f"[+] Successfully aggregated {len(results)} origin-destination traffic flows.")
    print("Sample Output (Top 5 Aggregated Routes):")
    for route, total in results[:5]:
        print(f"   Route Junction {route[0]} -> Junction {route[1]} : {total:,} vehicles")

    sc.stop()
    print("[+] SparkContext gracefully shut down.")

if __name__ == "__main__":
    run_spark_job()
