import json
import os
import matplotlib.pyplot as plt
import numpy as np

# Lê os JSONs de métricas gerados pelo Java
scenarios   = ["write_heavy", "balanced", "read_heavy"]
labels      = ["Write Heavy", "Balanced", "Read Heavy"]
metrics_dir = "results/metrics"

# Coleta métricas
insert_throughput = []
search_avg_us     = []
search_p99_us     = []
compaction_count  = []
flush_count       = []

for scenario in scenarios:
    path = os.path.join(metrics_dir, f"{scenario}.json")

    if not os.path.exists(path):
        print(f"[AVISO] Arquivo não encontrado: {path}")
        insert_throughput.append(0)
        search_avg_us.append(0)
        search_p99_us.append(0)
        compaction_count.append(0)
        flush_count.append(0)
        continue

    with open(path) as f:
        data = json.load(f)

    insert_throughput.append(float(data.get("insert_throughput_ops_sec", 0)))
    search_avg_us.append(float(data.get("search_avg_us", 0)))
    search_p99_us.append(float(data.get("search_p99_us", 0)))
    compaction_count.append(int(data.get("compaction_count", 0)))
    flush_count.append(int(data.get("flush_count", 0)))

# ── Configuração visual ───────────────────────────────────
os.makedirs("results/graphs", exist_ok=True)

x      = np.arange(len(labels))
colors = ["#e74c3c", "#3498db", "#2ecc71"]

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("LSM-Tree — Resultados por Cenário", fontsize=16, fontweight="bold")

# ── Gráfico 1: Throughput de inserção ────────────────────
axes[0, 0].bar(x, insert_throughput, color=colors)
axes[0, 0].set_title("Throughput de Inserção (ops/seg)")
axes[0, 0].set_xticks(x)
axes[0, 0].set_xticklabels(labels)
axes[0, 0].set_ylabel("Operações por segundo")
for i, v in enumerate(insert_throughput):
    axes[0, 0].text(i, v + 100, f"{v:,.0f}", ha="center", fontsize=9)

# ── Gráfico 2: Latência média de busca ───────────────────
axes[0, 1].bar(x, search_avg_us, color=colors)
axes[0, 1].set_title("Latência Média de Busca (μs)")
axes[0, 1].set_xticks(x)
axes[0, 1].set_xticklabels(labels)
axes[0, 1].set_ylabel("Microssegundos (μs)")
for i, v in enumerate(search_avg_us):
    axes[0, 1].text(i, v + 0.5, f"{v:.2f}", ha="center", fontsize=9)

# ── Gráfico 3: P99 de busca ───────────────────────────────
axes[1, 0].bar(x, search_p99_us, color=colors)
axes[1, 0].set_title("Latência P99 de Busca (μs)")
axes[1, 0].set_xticks(x)
axes[1, 0].set_xticklabels(labels)
axes[1, 0].set_ylabel("Microssegundos (μs)")
for i, v in enumerate(search_p99_us):
    axes[1, 0].text(i, v + 0.5, f"{v:.2f}", ha="center", fontsize=9)

# ── Gráfico 4: Flushes e Compactions ─────────────────────
width = 0.35
axes[1, 1].bar(x - width/2, flush_count,      width, label="Flushes",      color="#9b59b6")
axes[1, 1].bar(x + width/2, compaction_count, width, label="Compactions",  color="#f39c12")
axes[1, 1].set_title("Flushes e Compactions")
axes[1, 1].set_xticks(x)
axes[1, 1].set_xticklabels(labels)
axes[1, 1].set_ylabel("Quantidade")
axes[1, 1].legend()

plt.tight_layout()
output_path = "results/graphs/benchmark_results.png"
plt.savefig(output_path, dpi=150)
plt.show()
print(f"[plot] Gráfico salvo em: {output_path}")