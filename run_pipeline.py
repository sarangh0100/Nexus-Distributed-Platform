"""
Nexus Smart City - Master End-to-End Distributed Pipeline Runner
Module: Member 3 [Lead] (System Orchestration & Integration)

Coordinates the complete 3-stage distributed execution pipeline:
 Stage 1 (Exp 7)  : Sensor Data Stream -> MapReduce Flow Aggregator -> Transition Matrix (T)
 Stage 2 (Exp 9)  : MPI Collective Communication (Bcast, Scatter, Gather) -> Real-Time City State
 Stage 3 (Exp 10) : MPI Parallel Matrix Multiplication -> Multi-Hop Cascade & Scaling Benchmark
"""

import os
import sys
import time
import subprocess

def run_step(step_name, command_args):
    print("\n" + "=" * 70)
    print(f" >>> [RUNNING] {step_name}")
    print(f" Command: {' '.join(command_args)}")
    print("=" * 70)
    t0 = time.time()
    result = subprocess.run([sys.executable] + command_args, capture_output=False)
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"[!] Warning: Step {step_name} exited with code {result.returncode}")
    else:
        print(f"[SUCCESS] {step_name} Completed in {elapsed:.2f}s")
    return elapsed

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root_dir)
    
    print("*" * 70)
    print("      NEXUS: DISTRIBUTED DATA INTELLIGENCE & SIMULATION PLATFORM")
    print("      Master Pipeline Orchestrator (Experiments 7, 9, 10)")
    print("*" * 70)
    
    total_start = time.time()
    
    # 0. Check data
    data_csv = os.path.join(root_dir, "data", "raw_traffic_sensor_logs.csv")
    if not os.path.exists(data_csv):
        run_step("Data Generator (100k Telemetry Logs)", ["data/generate_traffic_logs.py"])
    else:
        print(f"[i] Existing dataset found at: {data_csv}")

    # 1. Stage 1: Exp 7 MapReduce
    run_step("Stage 1 (Exp 7): MapReduce Job (Map -> Shuffle -> Reduce)", ["exp7_mapreduce/exp7_mapreduce.py"])

    # 2. Stage 2: Exp 9 MPI Collectives
    run_step("Stage 2 (Exp 9): MPI Collective Simulation (Bcast, Scatter, Gather)", ["exp9_simulation/exp9_mpi_collective.py"])

    # 3. Stage 3: Exp 10 Parallel Matrix Multiplication
    run_step("Stage 3 (Exp 10): Parallel Matrix Multiplication (MPI Rows Scatter & Col Bcast)", ["exp10_matrix_mul/exp10_parallel_mat_mul.py"])

    # 4. Scalability Benchmark
    run_step("Performance Benchmark: MPI Scalability & Speedup Curve", ["exp10_matrix_mul/benchmark_scaling.py"])

    total_time = time.time() - total_start
    print("\n" + "*" * 70)
    print(f" [SUCCESS] Complete Distributed Pipeline Executed in {total_time:.2f}s")
    print(" Generated Artifacts in /outputs:")
    outputs_dir = os.path.join(root_dir, "outputs")
    for f in sorted(os.listdir(outputs_dir)):
        p = os.path.join(outputs_dir, f)
        print(f"   • {f:30} ({os.path.getsize(p):,} bytes)")
    print("\n Launch the interactive dashboard with:")
    print("   streamlit run dashboard/app.py")
    print("*" * 70)

if __name__ == "__main__":
    main()
