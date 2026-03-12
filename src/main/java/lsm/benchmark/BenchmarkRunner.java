package lsm.benchmark;

import lsm.core.LSMTree;
import lsm.util.MetricsLogger;
import java.util.Random;    

public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {

        // ── Lê variáveis de ambiente ─────────────────────────
        String scenario = getEnv("SCENARIO", "balanced");
        int numKeys = getEnvInt("NUM_KEYS", 10000);
        int memTableSize = getEnvInt("MEMTABLE_SIZE", 1000);

        System.out.println("=== BenchmarkRunner ===");
        System.out.println("Cenário      : " + scenario);
        System.out.println("Chaves       : " + numKeys);
        System.out.println("MemTable max : " + memTableSize);

        // ── Configura MetricsLogger ──────────────────────────
        String metricsPath = "results/metrics/" + scenario + ".json";
        MetricsLogger logger = new MetricsLogger(scenario, metricsPath);

        // ── Cria LSMTree ─────────────────────────────────────
        LSMTree lsm = new LSMTree(memTableSize, "results/data", logger);

        // ── Define buscas por cenário ────────────────────────
        int numSearches;
        switch (scenario) {
            case "write_heavy":
                numSearches = numKeys / 10;
                break;
            case "read_heavy":
                numSearches = numKeys * 10;
                break;
            default:
                numSearches = numKeys;
                break;
        }

        // ── Fase 1: Inserção ─────────────────────────────────
        System.out.println("\n[Fase 1] Inserindo " + numKeys + " chaves...");
        Random random = new Random(42);

        logger.startInsert();
        for (int i = 0; i < numKeys; i++) {
            String key = String.format("key_%08d", i);
            int value = random.nextInt(1000000);
            lsm.insert(key, value);
        }
        lsm.close();
        logger.endInsert(numKeys);

        System.out.println("[Fase 1] Concluída!");
        System.out.println("  Flushes     : " + logger.getFlushCount());
        System.out.println("  Compactions : " + logger.getCompactionCount());

        // ── Fase 2: Busca ────────────────────────────────────
        System.out.println("\n[Fase 2] Realizando " + numSearches + " buscas...");

        int existingSearches = (int) (numSearches * 0.7);
        int missingSearches = numSearches - existingSearches;

        logger.startSearch();

        // Busca chaves existentes
        for (int i = 0; i < existingSearches; i++) {
            int index = random.nextInt(numKeys);
            String key = String.format("key_%08d", index);

            long t0 = System.nanoTime();
            Integer value = lsm.get(key);
            long latencyUs = (System.nanoTime() - t0) / 1000;

            logger.recordSearch(value != null, latencyUs);
        }

        // Busca chaves inexistentes
        for (int i = 0; i < missingSearches; i++) {
            String key = String.format("key_missing_%08d", i);

            long t0 = System.nanoTime();
            Integer value = lsm.get(key);
            long latencyUs = (System.nanoTime() - t0) / 1000;

            logger.recordSearch(value != null, latencyUs);
        }

        logger.endSearch();
        System.out.println("[Fase 2] Concluída!");

        // ── Salva métricas ───────────────────────────────────
        logger.save();

        System.out.println("\n=== Resultado Final ===");
        System.out.println("Flushes         : " + logger.getFlushCount());
        System.out.println("Compactions     : " + logger.getCompactionCount());
        System.out.println("Search hits     : " + logger.getSearchHits());
        System.out.println("Search total    : " + logger.getSearchTotal());
        System.out.println("Métricas salvas : " + metricsPath);
    }

    // ── Helpers ──────────────────────────────────────────────

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    private static int getEnvInt(String name, int defaultValue) {
        String value = System.getenv(name);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

}
