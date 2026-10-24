"""
Nexus Smart City - Distributed Computing Lab Exp 9
Module: Member 3 [Lead] (MPI Collective Communication Simulation Engine)

Implements Collective Communication Operations:
1. comm.bcast: Root broadcasts global simulation configurations (weather, road blockage, signal multiplier)
2. comm.scatter: Root distributes partitions of city junction queues among worker ranks
3. Worker compute: Each worker process updates local vehicle flow dynamics and queues
4. comm.gather: Root collects updated subgrid states from all processes to form the global city state

Follows the exact Lab 9 collective communication model.
"""

import os
import sys
import time
import json
import random

def load_simulation_inputs():
    base_dir = os.path.dirname(os.path.dirname(__file__))
    meta_path = os.path.join(base_dir, "data", "junctions_metadata.json")
    with open(meta_path, "r") as f:
        junctions = json.load(f)["junctions"]
        
    metrics_path = os.path.join(base_dir, "outputs", "congestion_metrics.json")
    if os.path.exists(metrics_path):
        with open(metrics_path, "r") as f:
            metrics = json.load(f)
            # Enrich junctions with initial queue data from Exp 7
            for j in junctions:
                matching = next((m for m in metrics if m["junction_id"] == j["id"]), None)
                if matching:
                    j["initial_queue"] = int(matching["total_inflow"] * 0.05)
                    j["base_speed"] = matching["avg_speed_kmh"]
                else:
                    j["initial_queue"] = 50
                    j["base_speed"] = 45.0
    else:
        for j in junctions:
            j["initial_queue"] = 50
            j["base_speed"] = 45.0
            
    return junctions

def run_mpi_collective_simulation(blocked_junction_id=None, weather_severity=1.0):
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

    sim_config = None
    scatter_chunks = None
    junctions = []

    # ----------------------------------------------------
    # 1. ROOT INITIALIZATION (Rank 0)
    # ----------------------------------------------------
    if rank == 0:
        junctions = load_simulation_inputs()
        total_junctions = len(junctions)
        
        # Package global simulation config
        sim_config = {
            "weather_friction": float(weather_severity),
            "blocked_junction": blocked_junction_id,
            "simulation_tick_sec": 60,
            "signal_timing_factor": 1.2
        }
        
        # Partition junctions across MPI processes
        chunk_size = (total_junctions + size - 1) // size
        scatter_chunks = []
        for i in range(size):
            chunk = junctions[i * chunk_size:(i + 1) * chunk_size]
            scatter_chunks.append(chunk)

        print("=" * 65)
        print(f" [EXP 9] MPI Collective City Simulation (Processes: {size})")
        print(f" Global Config: Weather Friction = {weather_severity}x | Incident Blockage = Junction {blocked_junction_id}")
        print("=" * 65)
        start_time = time.time()
    else:
        start_time = None

    # ----------------------------------------------------
    # 2. BROADCAST (comm.bcast)
    # ----------------------------------------------------
    if use_real_mpi:
        sim_config = comm.bcast(sim_config, root=0)
    
    # ----------------------------------------------------
    # 3. SCATTER (comm.scatter)
    # ----------------------------------------------------
    if use_real_mpi:
        local_junctions = comm.scatter(scatter_chunks, root=0)
    else:
        local_junctions = junctions

    # ----------------------------------------------------
    # 4. WORKER COMPUTATION (Parallel Queue Updates)
    # ----------------------------------------------------
    local_updated = []
    if local_junctions is not None:
        for j in local_junctions:
            j_id = j["id"]
            curr_q = j.get("initial_queue", 40)
            capacity = j.get("capacity", 2000)
            
            # Impact of road blockage or incident
            is_blocked = (sim_config["blocked_junction"] is not None and int(sim_config["blocked_junction"]) == j_id)
            if is_blocked:
                outflow_rate = 5   # severe bottleneck
                inflow_rate = random.randint(35, 70)
            else:
                outflow_rate = int((capacity / 60) * (1.0 / sim_config["weather_friction"]))
                inflow_rate = random.randint(15, 45)

            new_queue = max(0, curr_q + inflow_rate - outflow_rate)
            congestion_ratio = min(1.0, new_queue / (capacity * 0.1))
            
            if congestion_ratio > 0.75:
                status = "CRITICAL (RED)"
            elif congestion_ratio > 0.45:
                status = "WARNING (YELLOW)"
            else:
                status = "FLOWING (GREEN)"

            local_updated.append({
                "id": j_id,
                "name": j["name"],
                "x": j["x"],
                "y": j["y"],
                "zone": j["zone"],
                "previous_queue": curr_q,
                "updated_queue": new_queue,
                "congestion_ratio": round(congestion_ratio, 3),
                "status": status,
                "processed_by_rank": rank
            })

    # ----------------------------------------------------
    # 5. GATHER (comm.gather)
    # ----------------------------------------------------
    if use_real_mpi:
        gathered_states = comm.gather(local_updated, root=0)
    else:
        gathered_states = [local_updated]

    # ----------------------------------------------------
    # 6. ROOT PERSISTENCE & RESULTS
    # ----------------------------------------------------
    if rank == 0:
        flattened_results = []
        for rank_data in gathered_states:
            if rank_data:
                flattened_results.extend(rank_data)

        # Sort by junction ID
        flattened_results.sort(key=lambda x: x["id"])
        total_time = time.time() - start_time
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        out_dir = os.path.join(base_dir, "outputs")
        os.makedirs(out_dir, exist_ok=True)
        
        out_file = os.path.join(out_dir, "simulation_results.json")
        payload = {
            "sim_config": sim_config,
            "mpi_processes": size,
            "execution_time_sec": round(total_time, 4),
            "junction_states": flattened_results
        }
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(f"[+] Broadcast -> Scatter -> Local Compute -> Gather Completed in {total_time*1000:.3f}ms")
        print(f"[+] Synchronized City State written to: {out_file}")
        print("=" * 65)
        return payload

if __name__ == "__main__":
    # Test with optional blockage at Junction 0 (Central Square) and 1.3x weather friction
    run_mpi_collective_simulation(blocked_junction_id=0, weather_severity=1.2)
