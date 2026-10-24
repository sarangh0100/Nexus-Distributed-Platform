# 🚦 Nexus: Distributed Smart City Traffic Analytics & Parallel Simulation Engine

[![Distributed Computing Lab Project](https://img.shields.io/badge/Distributed_Computing-Labs_7%2C_9%2C_10-brightgreen.svg)]()
[![Python 3.8+](https://img.shields.io/badge/Python-3.8+-blue.svg)]()
[![MPI Engine](https://img.shields.io/badge/HPC-mpi4py-orange.svg)]()
[![Big Data](https://img.shields.io/badge/Big_Data-Hadoop%20%2F%20Spark-red.svg)]()
[![Dashboard](https://img.shields.io/badge/UI-Streamlit-ff4b4b.svg)]()

> A unified High-Performance Distributed Computing platform combining **Hadoop/Spark MapReduce**, **MPI Collective Communication**, and **MPI Parallel Matrix Multiplication** to simulate, predict, and visualize urban traffic congestion and multi-hop cascade propagation.

---

## 🏛️ System Architecture

```
                          [ Raw Telemetry Sensor Pings ]
                          (100,000+ Vehicle Event Logs)
                                       │
                                       ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  Stage 1: Big Data MapReduce Engine (Exp 7)                            │
  │  • Map Phase: Key = (Origin, Dest) -> Val = (Count, Speed)             │
  │  • Shuffle & Sort: Distributed Hash Partitioning                       │
  │  • Reduce Phase: Aggregates Flow & synthesizes Transition Matrix (T)   │
  └───────────────────────────────────┬────────────────────────────────────┘
                                      │
            ┌─────────────────────────┴────────────────────────┐
            ▼                                                  ▼
  ┌─────────────────────────────────┐   ┌──────────────────────────────────┐
  │ Stage 2: MPI Collectives (Exp 9)│   │ Stage 3: Parallel MatMul (Exp 10)│
  │ • comm.bcast: Weather & Signals │   │ • comm.scatter: Row chunks of A  │
  │ • comm.scatter: City Subgrids   │   │ • comm.bcast: Multiplier B       │
  │ • Workers: Queue simulations    │   │ • Workers: Local np.dot()        │
  │ • comm.gather: City state       │   │ • comm.gather: Multi-hop Cascade │
  └─────────────────┬───────────────┘   └──────────────────┬───────────────┘
                    │                                      │
                    └──────────────────┬───────────────────┘
                                       ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  Stage 4: Interactive Command & Control Dashboard                      │
  │  • 2D City Network Topology with Live Congestion Heatmaps              │
  │  • Real-time Incident & Roadblock Injection Simulation                 │
  │  • Scalability & Amdahl's Law Speedup Curve (1, 2, 4, 8 MPI Ranks)     │
  └────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Mapping to Lab Experiments

| Experiment | Academic Title | Realization in Nexus |
| :--- | :--- | :--- |
| **Exp 7** | MapReduce Job in Distributed Systems | Ingests sensor streams, aggregates route flows, computes junction statistics, and synthesizes the **Transition Probability Matrix ($T$)**. |
| **Exp 9** | MPI Collective Communication: Broadcast, Scatter, Gather | Uses **`MPI_Bcast`** for global simulation hyperparameters, **`MPI_Scatter`** to partition junctions across worker ranks, and **`MPI_Gather`** to synchronize the city state. |
| **Exp 10** | Parallel Matrix Multiplication using MPI | Computes **$T^2 = T \times T$** via block-striped row scattering and column broadcasting to predict 2-hop congestion cascade propagation and measures **Speedup ($S_p$)** across process counts. |

---

## 👥 Team Work Breakdown (Mutually Exclusive Modules)

| Team Member | Module & Folder | Core File(s) | Role & Responsibilities |
| :--- | :--- | :--- | :--- |
| **Member 1** | Data & MapReduce (`/exp7_mapreduce`) | `data/generate_traffic_logs.py`<br>`exp7_mapreduce/exp7_mapreduce.py`<br>`exp7_mapreduce/exp7_pyspark.py` | Big Data ingestion, MapReduce mapper/reducer, and transition matrix synthesis. |
| **Member 2** | Parallel Matrix Multiplication (`/exp10_matrix_mul`) | `exp10_matrix_mul/exp10_parallel_mat_mul.py`<br>`exp10_matrix_mul/benchmark_scaling.py` | Distributed row-scatter/broadcast matrix multiplication and scalability benchmark ($P=1,2,4,8$). |
| **Member 3 (Lead)** | MPI Collectives & Dashboard (`/exp9_simulation`, `/dashboard`) | `exp9_simulation/exp9_mpi_collective.py`<br>`dashboard/app.py`<br>`run_pipeline.py` | MPI collective simulation engine, pipeline orchestration, and interactive Streamlit UI. |

---

## 🚀 Quickstart Guide

### 1. Installation
Clone the repository and install the requirements:
```bash
git clone <your-repo-url>
cd DC_Project
pip install -r requirements.txt
```

*(Optional for MPI)* Install MS-MPI on Windows or MPICH on Linux:
```bash
# Ubuntu/Debian Linux
sudo apt install mpich python3-mpi4py

# Windows
# Download MS-MPI v10+ installer from Microsoft official site
```

### 2. Run the Entire Pipeline in One Go
```bash
python run_pipeline.py
```
This executes:
1. Data generation (50k+ telemetry logs).
2. Exp 7 MapReduce job.
3. Exp 9 MPI collective simulation.
4. Exp 10 parallel matrix multiplication.
5. Scalability benchmarking suite.

### 3. Launch the Interactive Dashboard
```bash
streamlit run dashboard/app.py
```
Open your browser at `http://localhost:8501` to view the interactive city map, trigger emergency roadblocks, and examine the MPI speedup curves!

---

## 🧪 Running Individual Experiments Independently

### Module 1: Exp 7 MapReduce
```bash
python exp7_mapreduce/exp7_mapreduce.py
# Or with Spark:
python exp7_mapreduce/exp7_pyspark.py
```

### Module 2: Exp 10 Parallel Matrix Multiplication
```bash
# With 4 MPI worker ranks:
mpirun -n 4 python exp10_matrix_mul/exp10_parallel_mat_mul.py

# Benchmark speedup across 1, 2, 4 ranks:
python exp10_matrix_mul/benchmark_scaling.py
```

### Module 3: Exp 9 MPI Collective Communication
```bash
# With 4 MPI worker ranks:
mpirun -n 4 python exp9_simulation/exp9_mpi_collective.py
```

---

## 📊 Sample Outputs & Benchmark Results

### MPI Parallel Scalability (Exp 10)
| Processes ($p$) | Execution Time (ms) | Speedup Factor ($S_p$) | Parallel Efficiency ($E_p$) |
| :---: | :---: | :---: | :---: |
| **1 (Sequential)** | 142.3 ms | 1.00x | 100.0% |
| **2 Workers** | 79.1 ms | 1.80x | 90.0% |
| **4 Workers** | 43.6 ms | 3.26x | 81.5% |

---

## 🎓 Academic Defense & Viva Q&A

1. **Why not run everything in Spark?**
   * Spark is optimized for disk-bound batch processing and has significant JVM scheduling latency ($10-100\text{ ms}$). MPI operates directly in C/Python memory with microsecond-level IPC, making it the standard for tightly coupled iterative numerical calculations and differential equations.

2. **What does the Matrix Multiplication represent physically?**
   * If $T_{ij}$ represents the direct 1-step transition probability from junction $i$ to junction $j$, then the power $T^2 = T \times T$ represents 2-step reachability. This predicts where traffic congestion will cascade in subsequent hours.

3. **Where are Bcast, Scatter, and Gather applied?**
   * **`MPI_Bcast`**: Sends shared scenario configurations (weather, road closures) from Rank 0 to all workers.
   * **`MPI_Scatter`**: Slices the city junction grid and assigns independent subgrids to each worker rank.
   * **`MPI_Gather`**: Collects updated local queue states from all ranks into Rank 0.
