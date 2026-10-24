# Module 3: MPI Collective Communication & Simulation Engine (Exp 9)

## Responsibilities
- Implements MPI collective communication primitives (`bcast`, `scatter`, `gather`).
- **`comm.bcast`**: Broadcasts global simulation parameters (weather, emergency roadblocks) from Rank 0 to all processes.
- **`comm.scatter`**: Partitions and scatters city junction subgrids across MPI ranks.
- Workers compute real-time queue states and inflow/outflow balance in parallel.
- **`comm.gather`**: Collects updated subgrid states from all ranks into Rank 0 to reconstruct the global city traffic state.

## Files
1. `exp9_simulation/exp9_mpi_collective.py`: Core MPI collective simulation engine.

## How to Run Independently
```bash
# 1. Run with 4 MPI ranks:
mpirun -n 4 python exp9_simulation/exp9_mpi_collective.py

# Or on Windows MS-MPI:
mpiexec -n 4 python exp9_simulation/exp9_mpi_collective.py

# Or standalone Python test:
python exp9_simulation/exp9_mpi_collective.py
```

## Produced Outputs (Contract)
- `outputs/simulation_results.json`: Synchronized citywide junction states with worker rank ownership and congestion levels.
