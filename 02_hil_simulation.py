import pandas as pd
import pymongo
import time
import os
import gc
import random
import numpy as np
from pymongo.errors import ExecutionTimeout

# =========================================================
# CONFIG
# =========================================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://host.docker.internal:27017/")
DB_NAME = "hil_benchmark"

NUM_DOCS = 50
OPS_PER_SCENARIO = 30
MAX_TIMEOUT_MS = 5000

BATCH_SIZE = 1000
MAX_PAYLOAD_BYTES = 256 * 1024
MAX_SCENARIOS_PER_RUN = 300

OUT_FILE = "raw_latency_logs.csv"

# 🔥 NEW CONFIG
LARGE_PAYLOAD_THRESHOLD = 64 * 1024  # 64KB

# =========================================================
# REPRODUCIBILITY
# =========================================================
random.seed(42)
np.random.seed(42)

# =========================================================
# CONNECT
# =========================================================
client = pymongo.MongoClient(MONGO_URI, maxPoolSize=5)
db = client[DB_NAME]

# =========================================================
# HELPERS
# =========================================================
def make_payload(size, depth):
    size = min(int(size), MAX_PAYLOAD_BYTES)
    data = os.urandom(size)

    node = {"d": data}
    for _ in range(max(0, depth - 1)):
        node = {"n": node}
    return node

def batch_insert(collection, docs):
    for i in range(0, len(docs), BATCH_SIZE):
        collection.insert_many(docs[i:i + BATCH_SIZE])

# =========================================================
# CORE EVALUATION
# =========================================================
def evaluate_strategy(strategy, row):
    card = int(row["cardinality"])
    payload_sz = int(row.get("payload_size_b", row.get("payload_size")))
    depth = int(row["nested_depth"])
    jfo = int(row["join_fan_out"])

    # Adjust load for large payloads to prevent freezing
    is_large = payload_sz >= LARGE_PAYLOAD_THRESHOLD
    local_num_docs = 10 if is_large else NUM_DOCS
    local_ops = 10 if is_large else OPS_PER_SCENARIO

    embed_count = max(1, min(50, int(card * 0.2))) if strategy == "Hybrid" else 0

    # 🔥 Pre-emptive BSON check (Validates the 16MB limit theoretically before crashing)
    MAX_BSON_SIZE = 15.5 * 1024 * 1024  # 15.5 MB safe limit
    if strategy == "Embed" and (card * payload_sz) > MAX_BSON_SIZE:
        return None, None, None, None, "ERROR: BSON document too large (>16MB)"
    if strategy == "Hybrid" and (embed_count * payload_sz) > MAX_BSON_SIZE:
        return None, None, None, None, "ERROR: BSON document too large (>16MB)"

    col_main = db[f"main_{strategy}"]
    col_child = db[f"child_{strategy}"]

    dynamic_batch_size = max(1, int((10 * 1024 * 1024) / max(1, payload_sz)))

    try:
        col_main.drop()
        col_child.drop()

        payload = make_payload(payload_sz, depth)

        # ================= SETUP =================
        if strategy == "Embed":
            docs =[]
            for i in range(1, local_num_docs + 1):
                items =[{"idx": j, "data": payload} for j in range(card)]
                docs.append({"_id": i, "items": items})
                if len(docs) >= dynamic_batch_size:
                    col_main.insert_many(docs)
                    docs =[]
            if docs:
                col_main.insert_many(docs)

        elif strategy == "Reference":
            col_main.insert_many([{"_id": i} for i in range(1, local_num_docs + 1)])
            
            # Add index to foreign key, otherwise we measure table scans instead of joins!
            col_child.create_index([("p_id", 1), ("idx", 1)])

            children =[]
            for i in range(1, local_num_docs + 1):
                for j in range(card):
                    children.append({"p_id": i, "idx": j, "data": payload})
                    if len(children) >= dynamic_batch_size:
                        col_child.insert_many(children)
                        children =[]
            if children:
                col_child.insert_many(children)

        elif strategy == "Hybrid":
            parents =[]
            for i in range(1, local_num_docs + 1):
                parents.append({
                    "_id": i,
                    "top":[{"idx": j, "data": payload} for j in range(embed_count)]
                })
            for i in range(0, len(parents), dynamic_batch_size):
                col_main.insert_many(parents[i:i + dynamic_batch_size])

            # Add index
            col_child.create_index([("p_id", 1), ("idx", 1)])

            children =[]
            for i in range(1, local_num_docs + 1):
                for j in range(embed_count, card):
                    children.append({"p_id": i, "idx": j, "data": payload})
                    if len(children) >= dynamic_batch_size:
                        col_child.insert_many(children)
                        children =[]
            if children:
                col_child.insert_many(children)

        # ================= WARMUP PHASE =================
        for _ in range(5):
            col_main.find_one({"_id": 1})

        # ================= BENCHMARK =================
        working_set_max = max(1, int(local_num_docs * float(row["working_set_ratio"])))
        select_limit = max(1, int(card * float(row["query_selectivity"])))
        cp_sec = float(row.get("consistency_penalty_ms", row.get("consistency_penalty", 0))) / 1000.0

        cpu_start = time.process_time()
        t_start = time.perf_counter()

        # 🔥 NEW: Track reads and writes separately
        read_time_total = 0.0
        write_time_total = 0.0
        read_count = 0
        write_count = 0

        # 🔥 ACTUAL BENCHMARK LOOP
        for _ in range(local_ops):
            target_id = random.randint(1, working_set_max)
            is_read = random.random() < float(row["read_ratio"])
            
            op_start = time.perf_counter()

            if is_read:
                if strategy == "Embed":
                    col_main.find_one({"_id": target_id})
                
                elif strategy == "Reference":
                    col_main.find_one({"_id": target_id})
                    for _ in range(jfo):
                        list(col_child.find({"p_id": target_id}).limit(select_limit))
                
                elif strategy == "Hybrid":
                    col_main.find_one({"_id": target_id})
                    if select_limit > embed_count:
                        for _ in range(jfo):
                            list(col_child.find({"p_id": target_id}).limit(select_limit - embed_count))
                
                read_time_total += (time.perf_counter() - op_start)
                read_count += 1

            else:
                target_idx = random.randint(0, card - 1)

                if strategy == "Embed":
                    col_main.update_one({"_id": target_id}, {"$set": {"x": 1}})
                
                elif strategy == "Reference":
                    if cp_sec > 0:
                        time.sleep(cp_sec)
                    col_child.update_one({"p_id": target_id, "idx": target_idx}, {"$set": {"x": 1}})
                
                elif strategy == "Hybrid":
                    col_main.update_one({"_id": target_id}, {"$set": {"x": 1}})

                write_time_total += (time.perf_counter() - op_start)
                write_count += 1

        cpu_end = time.process_time()
        t_end = time.perf_counter()

        # 🔥 Calculate separated metrics
        latency = (t_end - t_start) * 1000 / local_ops
        cpu = (cpu_end - cpu_start) * 1000 / local_ops
        
        # Avoid division by zero if a scenario happens to roll 100% reads or 100% writes
        read_latency = (read_time_total * 1000 / read_count) if read_count > 0 else 0.0
        write_latency = (write_time_total * 1000 / write_count) if write_count > 0 else 0.0

        return round(latency, 3), round(read_latency, 3), round(write_latency, 3), round(cpu, 3), "None"

    except ExecutionTimeout:
        return None, None, None, None, "TIMEOUT"

    except Exception as e:
        clean_err = str(e).replace('\n', ' ').replace('\r', '').replace(',', ';')
        return None, None, None, None, f"ERROR: {clean_err[:50]}"

    finally:
        client.drop_database(DB_NAME)
        gc.collect()

# =========================================================
# MAIN
# =========================================================
def main():
    print("Loading simulation_inputs.csv...")
    df = pd.read_csv("simulation_inputs.csv")

    # 🔥 FIX: Table Header updated with separated latencies, and removed trailing comma
    if not os.path.exists(OUT_FILE):
        with open(OUT_FILE, "w") as f:
            f.write("scenario_id,strategy,latency_ms,read_latency_ms,write_latency_ms,cpu_ms,error\n")

    done = set()
    if os.path.exists(OUT_FILE):
        try:
            prev = pd.read_csv(OUT_FILE)
            done = set(zip(prev["scenario_id"].astype(str), prev["strategy"]))
            print(f"🔁 Resuming... {len(done)} old entries found. Skipping duplicates.")
        except Exception as e:
            print(f"⚠️ Resume load failed: {e}")

    strategies = ["Embed", "Reference", "Hybrid"]
    processed_count = 0

    print("Starting simulation...")

    for i, row in df.iterrows():
        if processed_count >= MAX_SCENARIOS_PER_RUN:
            print("🟡 Chunk limit reached. Restart container to resume.")
            break

        sid = str(row["scenario_id"])
        evaluated = False

        for strategy in strategies:
            if (sid, strategy) in done:
                continue

            # 🔥 FIX: Table Unpacking updated
            latency, read_lat, write_lat, cpu, error = evaluate_strategy(strategy, row)

            # 🔥 FIX: Table row writing updated
            with open(OUT_FILE, "a") as f:
                f.write(f"{sid},{strategy},{latency},{read_lat},{write_lat},{cpu},{error}\n")

            done.add((sid, strategy))
            evaluated = True

        if evaluated:
            processed_count += 1
            if processed_count % 20 == 0:
                print(f"{processed_count} scenarios done in this chunk...")

    print("✅ Run completed.")

if __name__ == "__main__":
    main()