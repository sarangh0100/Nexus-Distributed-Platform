"""
Nexus Smart City - Distributed Computing Lab Exp 10
Module: Member 2 (Parallel Matrix Multiplication Engine)

Implements Parallel Matrix Multiplication using MPI collective primitives:
1. Divide Matrix A into equal row blocks and SCATTER to worker processes.
2. BROADCAST Matrix B to all processes.
3. Compute local dot product: C_local = np.dot(A_local, B).
4. GATHER partial blocks at the root process and reconstruct C = A x B.

Follows the exact Lab 10 specification for predicting multi-hop traffic bottleneck propagation.
"""

import os
import sys
import time
import json
import numpy as np

def load_or_create_matrices(n=16):
    base_dir = os.path.dirname(os.path.dirname(__file__))
    matrix_csv = os.path.join(base_dir, "outputs", "transition_matrix.csv")
    
    if os.path.exists(matrix_csv):
        A = np.loadtxt(matrix_csv, delimiter=",")
        n = A.shape[0]
        # B is the transition matrix to compute 2-step Markov reachability (T^2)
        B = np.copy(A)
    else:
        # Fallback synthetic matrix if Exp 7 hasn't been run yet
        np.random.seed(42)
        raw = np.random.uniform(0.1, 1.0, (n, n))
        A = raw / raw.sum(axis=1, keepdims=True)
        B = np.copy(A)
    return A, B, n

def run_mpi_parallel_matmul():
    try:
        from mpi4py import MPI
        use_real_mpi = True
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
    except (ImportError, Exception):
        use_real_mpi = False
        comm = None
        rank = 0
        size = 1

    A = None
    B = None
    A_chunks = None
    N = 16

    # ----------------------------------------------------
    # ROOT INITIALIZATION (Rank 0)
    # ----------------------------------------------------
    if rank == 0:
        A, B, N = load_or_create_matrices()
        
        # Ensure matrix dimension is divisible by number of processes
        # If N not divisible by size, pad with zeros or adjust
        if N % size != 0:
            pad_rows = size - (N % size)
            A = np.pad(A, ((0, pad_rows), (0, 0)), mode='constant')
            B = np.pad(B, ((0, 0), (0, pad_rows)), mode='constant')
            N = A.shape[0]
            
        rows_per_process = N // size
        A_chunks = [A[i * rows_per_process:(i + 1) * rows_per_process, :] for i in range(size)]
        
        print("=" * 65)
        print(f" [EXP 10] Parallel Matrix Multiplication via MPI (Ranks: {size})")
        print(f" Matrix Dimensions: {N} x {N}")
        print(f" Workload: {rows_per_process} rows assigned per MPI rank")
        print("=" * 65)
        start_time = time.time()
    else:
        A_chunks = None
        B = None
        start_time = None

    # ----------------------------------------------------
    # MPI COLLECTIVE COMMUNICATION (Lab 10 Workflow)
    # ----------------------------------------------------
    if use_real_mpi:
        # 1. SCATTER rows of A across processes
        A_local = comm.scatter(A_chunks, root=0)
        
        # 2. BROADCAST matrix B to all processes
        B = comm.bcast(B, root=0)
        
        # 3. Local computation on local chunk
        t_local_start = time.time()
        C_local = np.dot(A_local, B)
        t_local_compute = time.time() - t_local_start
        
        print(f" -> [Rank {rank}] Completed local dot product ({A_local.shape[0]} rows) in {t_local_compute*1000:.3f}ms")
        
        # 4. GATHER results back at root
        C_gathered = comm.gather(C_local, root=0)
    else:
        # Standalone single-rank execution fallback
        A_local = A
        C_local = np.dot(A_local, B)
        C_gathered = [C_local]

    # ----------------------------------------------------
    # ROOT AGGREGATION & RESULT EXPORT
    # ----------------------------------------------------
    if rank == 0:
        C_final = np.vstack(C_gathered)
        total_duration = time.time() - start_time
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        output_dir = os.path.join(base_dir, "outputs")
        os.makedirs(output_dir, exist_ok=True)
        
        output_csv = os.path.join(output_dir, "multihop_propagation.csv")
        np.savetxt(output_csv, C_final, delimiter=",", fmt="%.6f")
        
        summary = {
            "matrix_dimension": N,
            "mpi_processes": size,
            "total_execution_sec": round(total_duration, 5),
            "output_file": output_csv,
            "status": "COMPLETED"
        }
        summary_path = os.path.join(output_dir, "exp10_matmul_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print("-" * 65)
        print(f"[+] Multi-Hop Propagation Matrix (T^2) calculated in {total_duration*1000:.3f}ms")
        print(f"[+] Saved output matrix -> {output_csv}")
        print("=" * 65)
        return summary

if __name__ == "__main__":
    run_mpi_parallel_matmul()
