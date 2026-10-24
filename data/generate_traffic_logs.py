"""
Nexus Smart City - Telemetry Log Generator
Module: Member 1 (Data & Telemetry Engine)

Generates synthetic smart city traffic sensor streams simulating
millions of vehicle transitions across junctions with rush-hour patterns.
"""

import os
import json
import random
import csv
from datetime import datetime, timedelta

def load_junctions():
    meta_path = os.path.join(os.path.dirname(__file__), "junctions_metadata.json")
    with open(meta_path, "r") as f:
        return json.load(f)["junctions"]

def generate_traffic_data(num_records=100000, output_path=None):
    junctions = load_junctions()
    n = len(junctions)
    
    if output_path is None:
        output_path = os.path.join(os.path.dirname(__file__), "raw_traffic_sensor_logs.csv")
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    start_time = datetime(2026, 10, 24, 6, 0, 0)
    weather_types = ["Clear", "Light Rain", "Heavy Rain", "Fog"]
    
    print(f"Generating {num_records:,} traffic sensor telemetry records...")
    
    # Adjacency preferences (nodes closer in coordinates are more likely to have trips)
    weights = []
    for i in range(n):
        row_w = []
        for j in range(n):
            if i == j:
                row_w.append(0.0)
            else:
                dx = junctions[i]["x"] - junctions[j]["x"]
                dy = junctions[i]["y"] - junctions[j]["y"]
                dist = max(1.0, (dx*dx + dy*dy)**0.5)
                # inverse distance attraction + hub bias
                hub_bias = 2.0 if junctions[j]["zone"] in ["Downtown", "Commercial"] else 1.0
                row_w.append(hub_bias / (dist ** 0.8))
        total = sum(row_w)
        weights.append([w / total for w in row_w])

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "record_id", "timestamp", "origin_id", "origin_name",
            "dest_id", "dest_name", "vehicle_count", "avg_speed_kmh", "weather"
        ])
        
        for rec_id in range(1, num_records + 1):
            # Advance time in increments
            delta_seconds = random.randint(1, 30)
            current_time = start_time + timedelta(seconds=(rec_id % 43200) + delta_seconds)
            hour = current_time.hour
            
            # Rush hour multiplier (8-10 AM and 17-20 PM)
            is_rush_hour = (8 <= hour <= 10) or (17 <= hour <= 20)
            
            origin = random.randrange(n)
            dest = random.choices(range(n), weights=weights[origin])[0]
            
            # Base vehicles and speed
            if is_rush_hour:
                vehicle_count = random.randint(40, 150)
                base_speed = random.uniform(15.0, 38.0)
            else:
                vehicle_count = random.randint(5, 60)
                base_speed = random.uniform(40.0, 75.0)
                
            # Weather penalty
            weather = random.choices(weather_types, weights=[0.75, 0.15, 0.07, 0.03])[0]
            if "Rain" in weather:
                base_speed *= 0.82
            elif weather == "Fog":
                base_speed *= 0.75
                
            writer.writerow([
                rec_id,
                current_time.strftime("%Y-%m-%d %H:%M:%S"),
                origin,
                junctions[origin]["name"],
                dest,
                junctions[dest]["name"],
                vehicle_count,
                round(base_speed, 2),
                weather
            ])
            
            if rec_id % 25000 == 0:
                print(f" -> {rec_id:,} / {num_records:,} records generated...")

    print(f"Dataset successfully created at: {output_path}")
    return output_path

if __name__ == "__main__":
    generate_traffic_data(num_records=50000)
