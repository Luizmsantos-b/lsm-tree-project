import json
import random
import os

# Seed fixo para reprodutibilidade
random.seed(42)

# Configuração dos 3 cenários
scenarios = {
    "write_heavy": {"num_keys": 500000, "memtable_size": 16000},
    "balanced":    {"num_keys": 250000, "memtable_size": 32000},
    "read_heavy":  {"num_keys": 50000,  "memtable_size": 48000},
}

os.makedirs("results/workloads", exist_ok=True)

for scenario, config in scenarios.items():
    num_keys      = config["num_keys"]
    memtable_size = config["memtable_size"]

    operations = []

    # Gera operações PUT
    for i in range(num_keys):
        key   = f"key_{i:08d}"
        value = random.randint(0, 1000000)
        operations.append({"op": "PUT", "key": key, "value": value})

    # Gera operações GET (70% chaves existentes, 30% inexistentes)
    if scenario == "write_heavy":
        num_searches = num_keys // 10
    elif scenario == "read_heavy":
        num_searches = num_keys * 10
    else:
        num_searches = num_keys

    existing_searches = int(num_searches * 0.7)
    missing_searches  = num_searches - existing_searches

    for _ in range(existing_searches):
        index = random.randint(0, num_keys - 1)
        key   = f"key_{index:08d}"
        operations.append({"op": "GET", "key": key})

    for i in range(missing_searches):
        key = f"key_missing_{i:08d}"
        operations.append({"op": "GET", "key": key})

    # Salva workload
    output = {
        "scenario":     scenario,
        "num_keys":     num_keys,
        "memtable_size": memtable_size,
        "operations":   operations
    }

    path = f"results/workloads/{scenario}.json"
    with open(path, "w") as f:
        json.dump(output, f)

    print(f"[generate] {scenario}: {len(operations)} operações → {path}")
