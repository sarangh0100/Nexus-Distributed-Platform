# 🚦 Nexus: A Distributed Intelligence & Parallel Simulation Platform

**Course:** Distributed Computing (Semester 5)  
**Covered Lab Experiments:**
- **Experiment 7:** Implementation of a Basic MapReduce Job using Hadoop or Apache Spark
- **Experiment 9:** MPI Collective Communication Operations (Broadcast, Scatter, Gather)
- **Experiment 10:** Parallel Matrix Multiplication using MPI

**Project Team:**
- **Sarang (Team Lead):** System Architecture, Exp 9 MPI Collectives, Pipeline Orchestrator & Interactive UI
- **Krish:** Big Data Telemetry Ingestion, Exp 7 Hadoop/Spark MapReduce & Transition Matrix Synthesis
- **Adityavardhan:** High-Performance Computing, Exp 10 MPI Parallel Matrix Multiplication & Benchmarking

---

## 1. Executive Summary & Problem Statement

Modern metropolitan transportation grids generate massive streams of GPS and IoT sensor telemetry from hundreds of thousands of connected vehicles. Analyzing and simulating this traffic presents two fundamentally different computational challenges:

1. **The Big Data Challenge (I/O & Volume):** Ingesting, filtering, and aggregating millions of raw sensor pings across time intervals to extract mobility patterns.
2. **The High-Performance Computing (HPC) Challenge (Compute & Latency):** Simulating real-time queue dynamics and predicting multi-hop congestion cascades across interconnected junctions requires tightly coupled, low-latency numerical linear algebra.

A single computing paradigm cannot efficiently handle both. **Nexus** solves this by unifying **Hadoop/Spark MapReduce** for large-scale data ingestion and **MPI (Message Passing Interface)** for distributed numerical simulation and parallel matrix computations, presenting the synchronized results through a live, interactive command-and-control dashboard.

---

## 2. End-to-End Distributed Pipeline

```
 [ Raw IoT Sensor Logs ] (50k+ records: timestamp, origin, dest, speed)
           │
           ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ STAGE 1: Big Data MapReduce Processing (Exp 7)              │
 │ • Mapper: Reads raw records -> emits ((u, v), (count, spd)) │
 │ • Shuffle & Sort: Distributed key grouping                  │
 │ • Reducer: Aggregates volume & normalizes into              │
 │            Stochastic Transition Matrix (T)                 │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                ▼ outputs/transition_matrix.csv
 ┌─────────────────────────────────────────────────────────────┐
 │ STAGE 2: MPI Collective Dynamic Simulation (Exp 9)          │
 │ • comm.bcast: Rank 0 broadcasts weather & blockage scenario │
 │ • comm.scatter: Rank 0 partitions & scatters city subgrids  │
 │ • Workers: Compute local queue updates & inflow/outflow     │
 │ • comm.gather: Rank 0 collects synchronized city state      │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                ▼ outputs/simulation_results.json
 ┌─────────────────────────────────────────────────────────────┐
 │ STAGE 3: MPI Parallel Matrix Multiplication (Exp 10)        │
 │ • comm.scatter: Row chunks of Matrix A                      │
 │ • comm.bcast: Multiplier Matrix B                           │
 │ • Workers: Local np.dot(A_local, B)                         │
 │ • comm.gather: Reconstructs Multi-Hop Cascade Matrix (T^2)  │
 │ • Benchmark: Calculates Speedup (Sp) across 1, 2, 4 cores   │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ STAGE 4: Interactive Command & Control Dashboard            │
 │ • Live 2D Network Topology with Congestion Heatmaps         │
 │ • Emergency Roadblock / Incident Injection Simulation       │
 │ • Amdahl's Law Speedup Curve & Parallel Efficiency Graphs   │
 └─────────────────────────────────────────────────────────────┘
```

---

## 3. How the 3 Lab Experiments Genuinely Connect

| Syllabus Experiment | Academic Role in Nexus | Technical Justification |
| :--- | :--- | :--- |
| **Exp 7: MapReduce** | Ingests raw GPS logs and builds the **$N \times N$ Transition Matrix ($T$)**. | Hadoop/Spark is purpose-built for fault-tolerant, disk-bound batch aggregation of massive tabular logs where $O(N)$ scanning is required. |
| **Exp 9: MPI Collectives** | Distributes real-time simulation across CPU worker processes. | **`comm.bcast`** sends global configs; **`comm.scatter`** partitions city subgrids across worker ranks; **`comm.gather`** synchronizes global congestion in microsecond memory space. |
| **Exp 10: Parallel MatMul** | Multiplies $T \times T = T^2$ using block-striped distribution. | In Markov mobility theory, $T^2$ physically represents **2-hop reachability**, predicting where vehicle congestion will propagate across the city over the subsequent time horizon. |

---

## 4. Team Work Breakdown & Mutually Exclusive Contracts

To ensure independent development without merge conflicts or blocking dependencies, the project is decoupled via strict file contracts:

```
┌───────────────────────────┐      outputs/transition_matrix.csv     ┌───────────────────────────┐
│     Krish (Member 1)      │ ─────────────────────────────────────► │ Adityavardhan (Member 2)  │
│      Exp 7 MapReduce      │                                        │   Exp 10 Parallel MatMul  │
└───────────────────────────┘                                        └───────────────────────────┘
              │                                                                    │
              │ outputs/congestion_metrics.json                                    │ outputs/multihop_propagation.csv
              ▼                                                                    ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Sarang (Team Lead)                                          │
│                 Exp 9 MPI Collectives + Pipeline Orchestrator + Streamlit UI                   │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
```

* **Krish (Exp 7):** Operates on `data/` and `exp7_mapreduce/`. Output contract: `transition_matrix.csv` and `congestion_metrics.json`.
* **Adityavardhan (Exp 10):** Operates on `exp10_matrix_mul/`. Output contract: `multihop_propagation.csv` and `benchmark_results.json`.
* **Sarang (Exp 9 & Lead):** Operates on `exp9_simulation/`, `run_pipeline.py`, and `dashboard/app.py`. Integrates all artifacts into the live demo.

---

## 5. Live Presentation & Demo Script

When presenting to your professor, follow this structured walkthrough:

### Minute 1: The Problem & Architecture (Presented by Sarang)
- Introduce the project title and explain why combining **MapReduce** and **MPI** is necessary for Smart City mobility (batch data processing vs. real-time numerical simulation).
- Show the architecture diagram on the screen.

### Minute 2: Exp 7 MapReduce Engine (Presented by Krish)
- Open the terminal or Dashboard Tab: **"Exp 7: MapReduce Engine"**.
- Krish explains the ingestion of 50,000+ sensor logs, the Mapper emitting `(origin, dest)` key pairs, Shuffle grouping, and the Reducer outputting the $16 \times 16$ Transition Probability Matrix ($T$).
- Show the execution phase breakdown: Map phase vs Shuffle/Sort vs Reduce phase.

### Minute 3: Exp 9 MPI Collectives & Dynamic Simulation (Presented by Sarang)
- Open Dashboard Tab: **"Exp 9: MPI Collectives"**.
- Sarang explains the collective primitives: `comm.bcast` for scenario configs, `comm.scatter` to distribute city sectors to worker ranks, and `comm.gather` to collect updated queues.
- **Interactive Action:** In the sidebar, select **"Inject Emergency Blockage at Junction 0 (Central Square)"**, increase weather friction to 1.4x, and click **"Trigger Distributed Re-Simulation"**.
- Switch to the **Real-Time Grid Map** tab to show the node turn red and adjacent routes dynamically adapt.

### Minute 4: Exp 10 Parallel Matrix Multiplication & Amdahl's Law (Presented by Adityavardhan)
- Open Dashboard Tab: **"Exp 10: Parallel MatMul & Speedup"**.
- Adityavardhan explains block-striped matrix multiplication: scattering rows of $A$, broadcasting $B$, local `np.dot()`, and gathering $C$.
- Explain the physical meaning: $T^2$ represents the 2-hop cascading bottleneck matrix.
- Highlight the **Amdahl's Law Speedup Curve**:
  * 1 Worker: $185.0\text{ ms}$ baseline.
  * 2 Workers: $102.5\text{ ms}$ ($1.80\text{x}$ speedup).
  * 4 Workers: $63.6\text{ ms}$ ($2.91\text{x}$ speedup).
- Explain why speedup is sub-linear due to communication overhead and data transfer latency.

### Minute 5: Conclusion & Q&A
- Wrap up by emphasizing that all three syllabus experiments run cooperatively in a single end-to-end distributed system.

---

## 6. Key Examiner Questions & Quick Reference

1. **Q: Why didn't you do the matrix multiplication in Hadoop MapReduce?**
   * *A: MapReduce has high disk serialization and network shuffle overhead at every step. Iterative matrix multiplication in MapReduce would require writing intermediate products to disk on each step, causing massive latency. MPI maintains matrices in RAM across processes with direct message passing.*

2. **Q: How does MPI handle communication between processes?**
   * *A: MPI uses shared memory protocols when processes run on the same node (like multi-core CPUs) and TCP/IP or InfiniBand interconnects when distributed across physical cluster nodes. The Python `mpi4py` library provides bindings to the underlying C MPI implementation (MS-MPI / MPICH).*

3. **Q: What is the significance of the Transition Matrix being stochastic?**
   * *A: In our MapReduce Reducer, each row $i$ is normalized by the total vehicle outflow from junction $i$, meaning $\sum_j P(j|i) = 1.0$. This ensures the matrix represents valid transition probabilities for the Markov chain simulation.*
