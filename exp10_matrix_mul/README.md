# Module 2: Parallel Matrix Multiplication Engine (Exp 10)

## Responsibilities
- Implements parallel matrix multiplication using MPI collective communication primitives.
- Divides the $N \times N$ matrix into row chunks and scatters them across worker processes (`comm.scatter`).
- Broadcasts the multiplier matrix to all processes (`comm.bcast`).
- Gathers computed partial dot products back at root (`comm.gather`).
- Benchmarks execution time across 1, 2, 4, and 8 processes to analyze Speedup ($S_p$) and Parallel Efficiency ($E_p$).

## Files
1. `exp10_matrix_mul/exp10_parallel_mat_mul.py`: Core MPI parallel matrix multiplication engine.
2. `exp10_matrix_mul/benchmark_scaling.py`: Automated scalability benchmark across process counts.

## How to Run Independently
```bash
# 1. Run with real MPI (using 4 worker processes):
mpirun -n 4 python exp10_matrix_mul/exp10_parallel_mat_mul.py

# Or on Windows MS-MPI:
mpiexec -n 4 python exp10_matrix_mul/exp10_parallel_mat_mul.py

# Or standalone Python test:
python exp10_matrix_mul/exp10_parallel_mat_mul.py

# 2. Run the scalability benchmark:
python exp10_matrix_mul/benchmark_scaling.py
```

## Produced Outputs (Contract)
- `outputs/multihop_propagation.csv`: The calculated multi-hop transition/reachability matrix.
- `outputs/exp10_matmul_summary.json`: Matrix size, rank count, and timing.
- `outputs/benchmark_results.json`: Speedup and efficiency data for the dashboard.
