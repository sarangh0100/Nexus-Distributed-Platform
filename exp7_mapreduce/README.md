# Module 1: Big Data & MapReduce Processing (Exp 7)

## Responsibilities
- Generates synthetic high-volume telemetry traffic logs.
- Implements MapReduce algorithm to filter, aggregate, and normalize route flow records.
- Outputs the **$N \times N$ Transition Probability Matrix ($T$)** and **Congestion Metrics** consumed by Exp 9 and Exp 10.

## Files
1. `data/generate_traffic_logs.py`: Creates 50,000+ realistic sensor logs with rush hour patterns.
2. `exp7_mapreduce/exp7_mapreduce.py`: Standard Python MapReduce implementation (Map -> Shuffle/Sort -> Reduce).
3. `exp7_mapreduce/exp7_pyspark.py`: PySpark RDD implementation (`sc.textFile`, `map`, `reduceByKey`).

## How to Run Independently
```bash
# Step 1: Generate sensor stream logs
python data/generate_traffic_logs.py

# Step 2: Run MapReduce job
python exp7_mapreduce/exp7_mapreduce.py

# Or run with Spark (optional):
python exp7_mapreduce/exp7_pyspark.py
```

## Produced Outputs (Contract)
- `outputs/transition_matrix.csv`: Normalized $N \times N$ probability matrix.
- `outputs/congestion_metrics.json`: Inflow, outflow, and congestion scores per junction.
- `outputs/exp7_mapreduce_summary.json`: Job timing & throughput metrics.
