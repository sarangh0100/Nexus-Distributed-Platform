# 📘 Member Guide: Krish — Big Data & MapReduce Engine (Exp 7)

**Assigned Role:** Data & MapReduce Engineer  
**Core Responsibility:** Experiment 7 (Hadoop / Apache Spark MapReduce)  
**Deliverable Contracts:** `data/raw_traffic_sensor_logs.csv`, `outputs/transition_matrix.csv`, `outputs/congestion_metrics.json`

---

## 🎯 What Your Module Does

In our distributed platform **Nexus**, you are responsible for the **Big Data Ingestion and Processing layer**. 

A smart city produces millions of IoT GPS sensor pings from vehicles moving through intersections. Running sequential loops on this volume of data is too slow. You implement the **MapReduce distributed paradigm** to:
1. Ingest raw vehicle telemetry logs: `(record_id, timestamp, origin_id, dest_id, vehicle_count, avg_speed_kmh, weather)`.
2. **Map Phase:** Filter noise and emit key-value pairs where key is `(origin_id, dest_id)` and value is `(vehicle_count, speed, count)`.
3. **Shuffle & Sort:** Group records by unique route keys across partitions.
4. **Reduce Phase:** Aggregate total vehicle volume, calculate average speed, and normalize values into an $N \times N$ **Transition Probability Matrix ($T$)** where each row $i$ sums to $1.0$ (representing the probability $P(j|i)$ of a vehicle transitioning from junction $i$ to junction $j$).

Your generated Transition Matrix is the foundational input that **Adityavardhan** uses for MPI parallel matrix multiplication and **Sarang** uses for dynamic city simulation.

---

## 📂 Files You Own

You have full ownership of these files in the repository:
```
DC_Project/
├── data/
│   ├── junctions_metadata.json         # Reference metadata for all 16 junctions
│   ├── generate_traffic_logs.py        # Generates synthetic sensor logs (50k+ rows)
│   └── raw_traffic_sensor_logs.csv     # Generated raw dataset
└── exp7_mapreduce/
    ├── exp7_mapreduce.py               # Pure Python MapReduce (Map -> Shuffle -> Reduce)
    ├── exp7_pyspark.py                 # PySpark implementation matching Lab 7 manual
    └── README.md                       # Your module documentation
```

---

## 🚀 Step-by-Step Instructions

### Step 1: Clone the Repo & Set Up Environment
```bash
git clone https://github.com/<team-repo>/Nexus-Distributed-Platform.git
cd Nexus-Distributed-Platform

# Optional: create a virtual environment
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

### Step 2: Generate the Telemetry Logs
Run the generator script to create 50,000+ realistic traffic pings with rush-hour patterns:
```bash
python data/generate_traffic_logs.py
```
*Verify that `data/raw_traffic_sensor_logs.csv` is created.*

### Step 3: Run the MapReduce Job
Run your core MapReduce pipeline:
```bash
python exp7_mapreduce/exp7_mapreduce.py
```
You will see three distinct phases printed in your terminal:
- `[*] MAP PHASE COMPLETED`: Reads lines, calls mapper, emits `(u, v) -> (count, speed, 1)`.
- `[*] SHUFFLE & SORT COMPLETED`: Groups values by key.
- `[*] REDUCE PHASE COMPLETED`: Calculates aggregated totals and normalizes into `transition_matrix.csv`.

### Step 4: Run the PySpark Version (Optional / For Lab Viva)
If your professor specifically asks to see Apache Spark RDD transformations:
```bash
python exp7_mapreduce/exp7_pyspark.py
```
This script uses `pyspark.SparkContext` with `.map()` and `.reduceByKey()`, exactly matching Page 4 of our Lab 7 manual.

### Step 5: Verify Your Output Contract
Check that your code generated the required files in `outputs/`:
- `outputs/transition_matrix.csv`: A $16 \times 16$ CSV of floating point probabilities.
- `outputs/congestion_metrics.json`: JSON array of junction inflow, outflow, and congestion scores.
- `outputs/exp7_mapreduce_summary.json`: Timing log for the Map, Shuffle, and Reduce phases.

### Step 6: Git Workflow (Record Your Commits)
Make sure your contributions show up on GitHub:
```bash
git checkout -b feature/exp7-mapreduce
# (Make any tweaks, docstring updates, or experiments)
git add exp7_mapreduce/ data/
git commit -m "feat(exp7): complete MapReduce telemetry ingestion and transition matrix builder"
git push origin feature/exp7-mapreduce
```
Then merge or open a pull request into `main`.

---

## 🎓 Viva Questions You Should Be Ready to Answer

1. **"What is the Mapper function doing in your code?"**
   * **Answer:** *"The mapper takes a single line of raw sensor telemetry and extracts the origin junction and destination junction as a composite key `(origin, dest)`. It emits this key along with vehicle count, speed, and a counter as values."*

2. **"What happens during the Shuffle and Sort phase?"**
   * **Answer:** *"The framework groups all values that share the exact same `(origin, dest)` key across partitions. This ensures that when the reducer runs, it receives all historical traffic records for that route in one iterable list."*

3. **"Why use MapReduce instead of a standard SQL `GROUP BY` or Pandas?"**
   * **Answer:** *"In a real smart city, hundreds of thousands of sensors emit gigabytes of data every minute. A single machine's memory cannot hold this data. MapReduce partitions the input into splits that run concurrently across many commodity worker nodes, providing horizontal scalability and fault tolerance."*

4. **"How does your output connect to Krish and Adityavardhan's modules?"**
   * **Answer:** *"My reducer normalizes the route volumes into row probabilities, creating a stochastic Transition Probability Matrix $T$. Adityavardhan uses this matrix in Exp 10 to perform parallel matrix multiplication ($T \times T$) to predict multi-hop congestion propagation, and Sarang uses it in Exp 9 to simulate dynamic city traffic queues."*
