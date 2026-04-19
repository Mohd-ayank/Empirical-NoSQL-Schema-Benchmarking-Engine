import pandas as pd
import itertools
import json
import datetime

# =========================================================
# EXPERIMENTAL FACTORS (FULL FACTORIAL)
# =========================================================

read_ratios = [0.2, 0.5, 0.9]
# EXPANDED: Pushed max payload to 64KB to force serialization bottlenecks
payload_sizes =[128, 4096, 65536]       
cardinalities =[10, 100, 1000, 5000]    # 1:N relationship scale
nested_depths = [1, 3]                   # BSON parsing complexity
working_set_ratios = [0.1, 0.9]          # 10% (hot cache) vs 90% (cold cache)
query_selectivities =[0.05, 0.5]        # Fetch 5% (Top-K) vs 50% (Range)
consistency_penalties = [0, 5]           # 0ms (Eventual), 5ms (Strong/Two-Phase)
index_coverages = ["none", "full"]       # Proves NoSQL index dependency
# EXPANDED: Pushed fan_outs to 50 to prove O(N) penalty on Reference schemas
join_fan_outs = [1, 10, 50]              

# =========================================================
# BUILD DATASET
# =========================================================
combinations = list(itertools.product(
    read_ratios, payload_sizes, cardinalities, nested_depths,
    working_set_ratios, query_selectivities, consistency_penalties,
    index_coverages, join_fan_outs
))

print(f"Generating {len(combinations)} Test Scenarios...")

rows =[]
for sid, combo in enumerate(combinations):
    rr, ps, card, nd, wsr, qs, cp, idx, jfo = combo
    rows.append({
        "scenario_id": f"S{sid:05d}",
        "read_ratio": rr,
        "payload_size_b": ps,
        "cardinality": card,
        "nested_depth": nd,
        "working_set_ratio": wsr,
        "query_selectivity": qs,
        "consistency_penalty_ms": cp,
        "index_coverage": idx,
        "join_fan_out": jfo
    })

df = pd.DataFrame(rows)
df.to_csv("simulation_inputs.csv", index=False)

# Metadata
meta = {
    "created_at": datetime.datetime.utcnow().isoformat() + "Z",
    "num_scenarios": len(df),
    "design": "full_factorial_paired_comparison",
    "note": "Each row is an environment. The runner will test ALL strategies per row."
}
with open("simulation_inputs.meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print("="*50)
print(f"Success! {len(df)} scenarios saved to simulation_inputs.csv")
print("="*50)