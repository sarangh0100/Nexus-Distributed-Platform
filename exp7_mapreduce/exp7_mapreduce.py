"""
Nexus Smart City - Distributed Computing Lab Exp 7
Module: Member 1 (MapReduce Processing Engine)

Implements the MapReduce computational model to ingest raw telemetry sensor logs,
aggregate origin-destination traffic flow volumes, calculate average speeds,
and synthesize the citywide Junction Transition Probability Matrix (T).

Compatible with standard Hadoop Streaming paradigm (Map -> Shuffle -> Reduce).
"""

import os
import csv
import json
import time
from collections import defaultdict

def mapper(row):
    """
    Map Phase:
    Input: Single telemetry record (row from CSV)
    Emits key-value pair:
       Key: (origin_id, dest_id)
       Value: (vehicle_count, speed_kmh, record_counter)
    """
    try:
        origin_id = int(row["origin_id"])
        dest_id = int(row["dest_id"])
        vehicle_count = int(row["vehicle_count"])
        speed = float(row["avg_speed_kmh"])
        return (origin_id, dest_id), (vehicle_count, speed, 1)
    except (ValueError, KeyError):
        return None

def run_mapreduce(input_csv=None, output_dir=None):
    base_dir = os.path.dirname(os.path.dirname(__file__))
    if input_csv is None:
        input_csv = os.path.join(base_dir, "data", "raw_traffic_sensor_logs.csv")
    if output_dir is None:
        output_dir = os.path.join(base_dir, "outputs")
        
    os.makedirs(output_dir, exist_ok=True)
    
    # Load junction metadata
    meta_file = os.path.join(base_dir, "data", "junctions_metadata.json")
    with open(meta_file, "r") as f:
        meta = json.load(f)
    num_junctions = meta["num_junctions"]
    junction_names = {j["id"]: j["name"] for j in meta["junctions"]}
    
    print("=" * 65)
    print(" [EXP 7] Starting MapReduce Job on Smart City Sensor Stream")
    print(f" Input Dataset: {input_csv}")
    print("=" * 65)
    
    start_time = time.time()
    
    # ----------------------------------------------------
    # PHASE 1: MAP PHASE
    # ----------------------------------------------------
    map_start = time.time()
    intermediate_pairs = []
    total_records = 0
    
    with open(input_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_records += 1
            emitted = mapper(row)
            if emitted:
                intermediate_pairs.append(emitted)
                
    map_duration = time.time() - map_start
    print(f"[*] MAP PHASE COMPLETED: Processed {total_records:,} records in {map_duration:.3f}s")
    print(f"    Emitted {len(intermediate_pairs):,} intermediate key-value pairs.")

    # ----------------------------------------------------
    # PHASE 2: SHUFFLE & SORT
    # ----------------------------------------------------
    shuffle_start = time.time()
    grouped_data = defaultdict(list)
    for key, val in intermediate_pairs:
        grouped_data[key].append(val)
        
    shuffle_duration = time.time() - shuffle_start
    print(f"[*] SHUFFLE & SORT COMPLETED: Grouped into {len(grouped_data):,} unique (Origin, Dest) keys in {shuffle_duration:.3f}s")

    # ----------------------------------------------------
    # PHASE 3: REDUCE PHASE
    # ----------------------------------------------------
    reduce_start = time.time()
    
    # Raw volume matrix
    flow_matrix = [[0.0 for _ in range(num_junctions)] for _ in range(num_junctions)]
    junction_stats = {
        i: {
            "name": junction_names[i],
            "total_vehicles_in": 0,
            "total_vehicles_out": 0,
            "speed_sum": 0.0,
            "record_count": 0
        } for i in range(num_junctions)
    }

    for (u, v), values in grouped_data.items():
        total_vehicles = sum(v_count for v_count, _, _ in values)
        total_speed = sum(spd for _, spd, _ in values)
        count = sum(cnt for _, _, cnt in values)
        
        flow_matrix[u][v] = float(total_vehicles)
        
        junction_stats[u]["total_vehicles_out"] += total_vehicles
        junction_stats[v]["total_vehicles_in"] += total_vehicles
        junction_stats[u]["speed_sum"] += total_speed
        junction_stats[u]["record_count"] += count

    # Synthesize Stochastic Transition Matrix T (where row sums = 1.0)
    transition_matrix = [[0.0 for _ in range(num_junctions)] for _ in range(num_junctions)]
    for i in range(num_junctions):
        row_sum = sum(flow_matrix[i])
        if row_sum > 0:
            for j in range(num_junctions):
                transition_matrix[i][j] = round(flow_matrix[i][j] / row_sum, 6)
        else:
            # Self-loop fallback
            transition_matrix[i][i] = 1.0

    reduce_duration = time.time() - reduce_start
    total_time = time.time() - start_time
    print(f"[*] REDUCE PHASE COMPLETED in {reduce_duration:.3f}s")

    # ----------------------------------------------------
    # EXPORT CONTRACT ARTIFACTS
    # ----------------------------------------------------
    # 1. Transition Matrix CSV
    matrix_csv_path = os.path.join(output_dir, "transition_matrix.csv")
    with open(matrix_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in transition_matrix:
            writer.writerow(row)
    print(f"[+] Exported Transition Matrix (T) -> {matrix_csv_path}")

    # 2. Congestion Metrics JSON
    formatted_metrics = []
    for j_id, stats in junction_stats.items():
        avg_spd = (stats["speed_sum"] / stats["record_count"]) if stats["record_count"] > 0 else 50.0
        # Congestion Index: Ratio of incoming volume to speed (0 to 100 scale)
        congestion_index = min(100.0, round((stats["total_vehicles_in"] / max(1, avg_spd)) * 0.15, 2))
        formatted_metrics.append({
            "junction_id": j_id,
            "junction_name": stats["name"],
            "total_outflow": stats["total_vehicles_out"],
            "total_inflow": stats["total_vehicles_in"],
            "avg_speed_kmh": round(avg_spd, 2),
            "congestion_score": congestion_index
        })

    metrics_json_path = os.path.join(output_dir, "congestion_metrics.json")
    with open(metrics_json_path, "w", encoding="utf-8") as f:
        json.dump(formatted_metrics, f, indent=2)
    print(f"[+] Exported Congestion Metrics -> {metrics_json_path}")

    # 3. MapReduce Job Summary
    summary = {
        "job_name": "Nexus_Traffic_Flow_MapReduce",
        "total_records_ingested": total_records,
        "unique_key_groups": len(grouped_data),
        "map_duration_sec": round(map_duration, 4),
        "shuffle_duration_sec": round(shuffle_duration, 4),
        "reduce_duration_sec": round(reduce_duration, 4),
        "total_execution_sec": round(total_time, 4),
        "status": "SUCCESS"
    }
    summary_path = os.path.join(output_dir, "exp7_mapreduce_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("=" * 65)
    print(f" [EXP 7] MapReduce Job Finished Successfully in {total_time:.3f}s")
    print("=" * 65)
    return summary

if __name__ == "__main__":
    run_mapreduce()
