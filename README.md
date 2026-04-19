# Empirical Validation of NoSQL Schema Design Patterns

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green?logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Isolated-blue?logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Analysis-red?logo=pandas&logoColor=white)

## Overview
The architectural shift toward microservices has accelerated the adoption of document-oriented (NoSQL) databases like MongoDB. However, designing schemas presents a fundamental dilemma: **Should you *embed* related data for read performance, or *reference* it to maintain normalization?**

While theoretical guidelines exist, there is a lack of rigorous empirical validation regarding how these patterns degrade under extreme hardware constraints, high cardinality, and massive network fan-outs. 

This project provides a **Hardware-in-the-Loop (HIL) containerized testbed** that empirically benchmarks three NoSQL strategies (Embed, Reference, and Hybrid) across 1,150+ diverse microservice workloads.

## Key Architectural Findings

Our dataset replaced theoretical guesswork with mathematical proof, yielding three major architectural insights:

### 1. The $O(N)$ Query Penalty is Real
By simulating a realistic 1ms cloud network delay, we proved that normalized (**Reference**) schemas suffer from severe $O(N)$ linear latency degradation. At a join fan-out of 50, the Reference strategy's latency skyrocketed to **~2,000ms**, while the **Embed** strategy remained perfectly flat at **~16ms** ($O(1)$ scaling).

### 2. The Unbounded Array Anti-Pattern (16MB Limit)
While Embedding dominates in speed, it is a catastrophic anti-pattern for 1:Many relationships capable of indefinite growth. Our testbed dynamically tracked payload sizes. The exact moment the simulated payload crossed MongoDB's hard 16MB BSON limit (e.g., 64KB payloads $\times$ 5,000 cardinality), the **Embed** strategy yielded a **100% failure rate** (recording 288 deterministic system crashes). The Reference and Hybrid strategies survived with a 0% failure rate.

### 3. Write Contention & The Cost of Atomicity
Traditional NoSQL databases lack cross-document ACID transactions. Under write-heavy workloads (Read Ratio = 0.20) with a 5ms simulated consistency penalty (simulating distributed locks), the **Reference** strategy's latency degraded to nearly 1,000ms. Because MongoDB provides atomic updates at the single-document level, the **Embed** strategy bypassed this penalty completely.

---

## The Engineering: A Crash-Resilient Testbed

Benchmarking databases inside containerized environments often introduces virtualization overhead and out-of-memory (OOM) host crashes. To ensure rigorous, peer-review-grade results, this testbed utilizes:

* **Container/Host Isolation:** The stateless Python load generator runs inside a Docker container, while the stateful MongoDB engine runs natively on Windows Subsystem for Linux (WSL) with strictly capped RAM (`--wiredTigerCacheSizeGB 0.5`).
* **Bounded-Memory Chunked Execution:** To prevent the Python garbage collector from accumulating memory during multi-megabyte payload generation, the test runs in chunks of 300 scenarios, flushing state to a persistent CSV and cleanly restarting.
* **Separated Latency Timers:** The simulation uses `time.perf_counter()` isolated specifically to the database I/O execution blocks, explicitly separating `read_latency_ms` and `write_latency_ms` to prevent blended data artifacts.

---

## Visualizing the Results

<p align="center">
  <img src="your-image.png" alt="Empirical Insights" width="600"/>
</p>

## 💻 How to Run the Simulation

### 1. Generate the Scenarios
```bash
python 01_generate_scenarios.py
```

### 2. Start the Isolated MongoDB Engine (Native Linux/WSL)
```bash
mongod --dbpath ~/safe_data --bind_ip 0.0.0.0 --wiredTigerCacheSizeGB 0.5 --oplogSize 50
```

### 3. Run the Containerized Benchmarking Script
```bash
docker build -t hil_runner:v1 .
docker run -it --rm --add-host=host.docker.internal:host-gateway -v "$(pwd)":/app hil_runner:v1
```

*(The script will automatically pause, save progress to `raw_latency_logs.csv`, and resume safely if restarted to prevent memory exhaustion.)*

---


## 🚀 Empirical Architectural Insights

```
[1] THE N+1 QUERY PROBLEM (Average Latency by Join Fan-Out)
join_fan_out     1       10       50
strategy                            
Embed         16.90   17.22    16.59
Hybrid        64.35  363.84  1556.46
Reference     57.86  412.18  1933.03
-> VERDICT: Embed scales at O(1) (flat). Reference scales at O(N) (linear growth).

[2] UNAVAILABILITY & THE 16MB LIMIT
-> The Embed strategy crashed 288 times.
-> Justification: As Payload Size and Cardinality multiply, the BSON document breaches MongoDB's hard 16MB limit.
-> Proof: Reference & Hybrid strategies survived because they normalize the unbounded lists into separate documents.

[3] THE CPU MYTH BUSTED
strategy
Embed         3.527
Hybrid       76.859
Reference    95.850
Name: cpu_ms, dtype: float64
-> VERDICT: Parsing large JSON documents (Embed) uses LESS CPU than managing multiple TCP network cursors (Reference).
```

---
## Conclusion & Best Practices

Default to Embedding for 1:Few relationships or read-heavy workloads where maximum performance is required.  

Never Embed for unbounded 1:Many relationships, as proven by the deterministic 16MB limit crashes.  

Use the Hybrid Pattern for paged data (e.g., caching the top 50 comments and referencing the rest). It avoids the 16MB limit while drastically reducing the `O(N)` network penalty.

