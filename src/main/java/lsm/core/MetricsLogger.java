package main.java.lsm.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MetricsLogger {

    private final String scenario;
    private final String outputPath;

    // Contadores
    private int flushCount = 0;
    private int compactionCount = 0;

    // Inserção
    private long insertStartMs = 0;
    private long insertEndMs = 0;
    private long keysInserted = 0;

    // Busca
    private long searchStartMs = 0;
    private long searchEndMs = 0;
    private long searchHits = 0;
    private long searchTotal = 0;

    // Latências individuais de busca
    private final List<Long> latenciesUs = new ArrayList<>();

    public MetricsLogger(String scenario, String outputPath) {
        this.scenario = scenario;
        this.outputPath = outputPath;
    }

    // ── Registro de eventos

    public void recordFlush() {
        flushCount++;
    }

    public void recordCompaction() {
        compactionCount++;
    }

    public void startInsert() {
        insertStartMs = System.currentTimeMillis();
    }

    public void endInsert(long keys) {
        insertEndMs = System.currentTimeMillis();
        keysInserted = keys;
    }

    public void startSearch() {
        searchStartMs = System.currentTimeMillis();
    }

    public void endSearch() {
        searchEndMs = System.currentTimeMillis();
    }

    public void recordSearch(boolean found, long latencyUs) {
        searchTotal++;
        if (found)
            searchHits++;
        latenciesUs.add(latencyUs);
    }

    public void save() throws IOException {
        long insertTotalMs = insertEndMs - insertStartMs;
        double insertThroughput = insertTotalMs > 0
                ? (keysInserted * 1000.0 / insertTotalMs)
                : 0;

        long searchTotalMs = searchEndMs - searchStartMs;
        double searchAvgUs = latenciesUs.isEmpty() ? 0
                : latenciesUs.stream().mapToLong(Long::longValue).average().orElse(0);
        double searchP99Us = calcPercentile(99);

        Files.createDirectories(Paths.get(outputPath).getParent());

        String json = "{\n" +
                "  \"scenario\": \"" + scenario + "\",\n" +
                "  \"timestamp\": \"" + Instant.now() + "\",\n" +
                "  \"insert_total_ms\": " + insertTotalMs + ",\n" +
                "  \"insert_throughput_ops_sec\": " + String.format("%.2f", insertThroughput) + ",\n" +
                "  \"keys_inserted\": " + keysInserted + ",\n" +
                "  \"search_total_ms\": " + searchTotalMs + ",\n" +
                "  \"search_avg_us\": " + String.format("%.4f", searchAvgUs) + ",\n" +
                "  \"search_p99_us\": " + String.format("%.4f", searchP99Us) + ",\n" +
                "  \"search_hits\": " + searchHits + ",\n" +
                "  \"search_total\": " + searchTotal + ",\n" +
                "  \"search_hit_rate\": " + String.format("%.4f", searchTotal > 0
                        ? (double) searchHits / searchTotal
                        : 0)
                + ",\n" +
                "  \"compaction_count\": " + compactionCount + ",\n" +
                "  \"flush_count\": " + flushCount + "\n" +
                "}\n";

        Files.writeString(Paths.get(outputPath), json);
        System.out.println("[MetricsLogger] Salvo em: " + outputPath);
    }

    private double calcPercentile(int percentile) {
        if (latenciesUs.isEmpty())
            return 0;
        List<Long> sorted = new ArrayList<>(latenciesUs);
        sorted.sort(Long::compareTo);
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, index));
    }

    public int getFlushCount() {
        return flushCount;
    }

    public int getCompactionCount() {
        return compactionCount;
    }

}
