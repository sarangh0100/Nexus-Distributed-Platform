# 📘 Member Guide: Adityavardhan — Parallel Matrix Multiplication Engine (Exp 10)

**Assigned Role:** High-Performance Computing (HPC) Engineer  
**Core Responsibility:** Experiment 10 (Parallel Matrix Multiplication using MPI)  
**Deliverable Contracts:** `exp10_matrix_mul/exp10_parallel_mat_mul.py`, `exp10_matrix_mul/benchmark_scaling.py`, `outputs/multihop_propagation.csv`, `outputs/benchmark_results.json`

---

## 🎯 What Your Module Does

In our distributed platform **Nexus**, you are responsible for the **High-Performance Numerical Computing layer**.

While Krish's MapReduce aggregates 1-hop direct vehicle transitions, we need to know: **Where will traffic bottlenecks propagate 2 or 3 hours into the future?** 

In graph and Markov theory, multiplying the 1-hop Transition Matrix ($T$) by itself ($T \times T = T^2$) gives the **2-hop reachability and congestion propagation matrix**. However, computing matrix products for large city networks sequentially on a single CPU core is computationally prohibitive.

You implement **Block-Striped Parallel Matrix Multiplication** using MPI (`mpi4py`):
1. **Partitioning:** The root process (Rank 0) divides Matrix $A$ into horizontal row blocks (`N // size` rows per rank).
2. **Scatter:** Uses `comm.scatter()` to distribute row slices of Matrix $A$ across all worker processes.
3. **Broadcast:** Uses `comm.bcast()` to replicate the entire Matrix $B$ to every worker process.
4. **Local Computation:** Each process computes the local dot product for its assigned rows: `C_local = np.dot(A_local, B)`.
5. **Gather:** Uses `comm.gather()` to collect the partial results at Rank 0 and reconstructs the full product matrix with `np.vstack()`.
6. **Scalability Benchmarking:** You measure execution time across $P = 1, 2, 4$ processes, calculate **Speedup ($S_p$)** and **Parallel Efficiency ($E_p$)**, proving Amdahl's Law for the lab viva.

---

## 📂 Files You Own

You have full ownership of these files in the repository:
```
DC_Project/
└── exp10_matrix_mul/
    ├── exp10_parallel_mat_mul.py       # Core MPI parallel matrix multiplication
    ├── benchmark_scaling.py            # Scalability runner across 1, 2, 4 ranks
    └── README.md                       # Your module documentation
```

---

## 🚀 Step-by-Step Instructions

### Step 1: Clone the Repo & Set Up Environment
```bash
git clone https://github.com/<team-repo>/Nexus-Distributed-Platform.git
cd Nexus-Distributed-Platform

# Activate your Python environment
pip install -r requirements.txt
```
*(Ensure `mpi4py` and `numpy` are installed. On Windows, install MS-MPI; on Linux, install `sudo apt install mpich python3-mpi4py`.)*

### Step 2: Run the Parallel Matrix Multiplication Engine
You can run your code in two ways:

**Option A: With multiple MPI processes (demonstrates true parallel execution):**
```bash
# On Linux / macOS / standard MPI:
mpirun -n 4 python exp10_matrix_mul/exp10_parallel_mat_mul.py

# On Windows with MS-MPI:
mpiexec -n 4 python exp10_matrix_mul/exp10_parallel_mat_mul.py
```
Observe how each process prints its rank and computes only its assigned slice of rows!

**Option B: Standalone Python (runs with built-in single-process fallback):**
```bash
python exp10_matrix_mul/exp10_parallel_mat_mul.py
```
*Verify that `outputs/multihop_propagation.csv` is created.*

### Step 3: Run the Scalability & Speedup Benchmark
Execute the benchmark script to test execution time across different core counts:
```bash
python exp10_matrix_mul/benchmark_scaling.py
```
You will see the terminal output display the Speedup table:
```
 Processes (p)  |    Time (ms)    |  Speedup (Sp)   | Efficiency (%) 
-----------------------------------------------------------------
       1        |     185.00      |      1.00      x |      100.0     %
       2        |     102.55      |      1.80      x |      90.2      %
       4        |      63.58      |      2.91      x |      72.7      %
```
This data is automatically exported to `outputs/benchmark_results.json` and rendered in the live Streamlit dashboard!

### Step 4: Verify Your Output Contract
Check that your code generated the required files in `outputs/`:
- `outputs/multihop_propagation.csv`: The $N \times N$ multi-hop cascade matrix.
- `outputs/exp10_matmul_summary.json`: Matrix size, rank count, and computation duration.
- `outputs/benchmark_results.json`: Speedup factors and efficiency metrics.

### Step 5: Git Workflow (Record Your Commits)
Make sure your contributions show up on GitHub:
```bash
git checkout -b feature/exp10-mpi-matmul
# (Make any tweaks, docstring updates, or experiments)
git add exp10_matrix_mul/
git commit -m "feat(exp10): implement MPI parallel matrix multiplication and Amdahl's Law benchmark"
git push origin feature/exp10-mpi-matmul
```
Then merge or open a pull request into `main`.

---

## 🎓 Viva Questions You Should Be Ready to Answer

1. **"Why use `comm.scatter` for Matrix A and `comm.bcast` for Matrix B?"**
   * **Answer:** *"Matrix multiplication requires multiplying each row of $A$ by all columns of $B$. Therefore, row blocks of $A$ are disjoint and can be scattered so each worker handles distinct rows without duplication. However, every worker needs the complete columns of $B$, so $B$ is broadcast to all processes."*

2. **"What does the matrix multiplication physically calculate in this project?"**
   * **Answer:** *"The input matrix $T$ from Krish's MapReduce stage contains 1-hop transition probabilities between city junctions. Multiplying $T \times T = T^2$ calculates the 2-hop reachability probabilities, predicting where traffic bottlenecks will cascade across the city in subsequent time steps."*

3. **"Why is the speedup factor with 4 processes 2.91x instead of an ideal 4.0x?"**
   * **Answer:** *"Due to Amdahl's Law and communication overhead. In distributed memory, transferring Matrix $B$ via broadcast and gathering the final blocks back to Rank 0 requires network/IPC data transfer. For smaller matrices, communication latency limits efficiency; as matrix dimensions grow, the computation-to-communication ratio improves and speedup approaches ideal linear scaling."*

4. **"How did you implement `comm.gather` in your code?"**
   * **Answer:** *"Each process computes its local sub-matrix `C_local = np.dot(A_local, B)`. We then call `comm.gather(C_local, root=0)`, which collects the partial row blocks at Rank 0 into a list. Rank 0 then calls `np.vstack(C)` to assemble the final product matrix."*
