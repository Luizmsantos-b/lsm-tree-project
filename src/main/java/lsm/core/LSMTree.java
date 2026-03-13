package lsm.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lsm.util.MetricsLogger;

public class LSMTree {

    // Configurações
    private static final int MAX_TABLES_PER_LEVEL = 4;
    private static final int MAX_LEVELS = 5;

    // Estado
    private final MemTable memTable;
    private final List<List<SSTable>> levels;
    private final String dataDir;

    private long flushCounter = 0;
    private int compactionCount = 0;

    private final MetricsLogger metricsLogger;

    // Construtor
    public LSMTree(int memTableMaxSize, String dataDir, MetricsLogger metricsLogger) throws IOException {
        this.memTable = new MemTable(memTableMaxSize);
        this.dataDir = dataDir;
        this.metricsLogger = metricsLogger;
        this.levels = new ArrayList<>();

        for (int i = 0; i < MAX_LEVELS; i++) {
            levels.add(new ArrayList<>());
        }
    }

    // Insert
    public void insert(String key, int value) throws IOException {
        memTable.insert(key, value);

        if (memTable.isFull()) {
            flush();
        }
    }

    // Get
    public Integer get(String key) throws IOException {

        // 1. Verifica MemTable primeiro (mais recente)
        Integer value = memTable.get(key);
        if (value != null)
            return value;

        // 2. Percorre cada nível do mais recente ao mais antigo
        for (List<SSTable> level : levels) {
            for (int i = level.size() - 1; i >= 0; i--) {
                value = level.get(i).get(key);
                if (value != null)
                    return value;
            }
        }

        return null; // não encontrado
    }

    // Flush
    public void flush() throws IOException {
        
        if (memTable.isEmpty())
            return;

        // Cria nova SSTable em L0
        long timestamp = System.nanoTime();
        String fileName = String.format("sst_%06d.sst", flushCounter++);
        SSTable sst = new SSTable(
                Paths.get(dataDir, "L0", fileName),
                0,
                timestamp);

        // Salva dados ordenados da MemTable no disco
        sst.write(memTable.getSortedEntries());
        levels.get(0).add(sst);
        memTable.clear();

        metricsLogger.recordFlush();

        System.out.println("[flush] SSTable criada: " + sst.getFilePath());

        // Verifica se L0 precisa de compaction
        if (levels.get(0).size() >= MAX_TABLES_PER_LEVEL) {
            compact(0);
        }
    }

    // Compact
    public void compact(int level) throws IOException {
        if (level >= MAX_LEVELS - 1)
            return;

        List<SSTable> currentLevel = levels.get(level);
        if (currentLevel.isEmpty())
            return;

        System.out.println("[compact] Compactando L" + level + " → L" + (level + 1));

        // Lê todos os dados do nível atual E do próximo usando HashMap para deduplicação O(1)
        // Lê nível N+1 primeiro (dados mais antigos), depois nível N (mais recentes sobrescrevem)
        Map<String, String> mergedMap = new HashMap<>();
        int nextLevel = level + 1;
        for (SSTable sst : levels.get(nextLevel)) {
            for (String[] entry : sst.readAll()) {
                mergedMap.put(entry[0], entry[1]);
            }
        }
        for (SSTable sst : currentLevel) {
            for (String[] entry : sst.readAll()) {
                // Dados do nível N (mais recentes) sobrescrevem os do N+1 (mais antigos)
                mergedMap.put(entry[0], entry[1]);
            }
        }

        // Converte Map para List e ordena pelo chave
        List<String[]> merged = new ArrayList<>();
        for (Map.Entry<String, String> entry : mergedMap.entrySet()) {
            merged.add(new String[] { entry.getKey(), entry.getValue() });
        }
        merged.sort((a, b) -> a[0].compareTo(b[0]));

        // Cria nova SSTable no nível superior
        long timestamp = System.nanoTime();
        String fileName = String.format("sst_compact_%06d.sst", compactionCount);
        SSTable compacted = new SSTable(
                Paths.get(dataDir, "L" + nextLevel, fileName),
                nextLevel,
                timestamp);
        compacted.write(merged);

        // Remove SSTables antigas do nível atual e do próximo
        for (SSTable sst : currentLevel)
            sst.delete();
        for (SSTable sst : levels.get(nextLevel))
            sst.delete();

        currentLevel.clear();
        levels.get(nextLevel).clear();
        levels.get(nextLevel).add(compacted);

        compactionCount++;
        metricsLogger.recordCompaction();

        System.out.println("[compact] Concluída! SSTables em L" + nextLevel + ": "
                + levels.get(nextLevel).size());

        // Compaction em cascata se o próximo nível também encheu
        if (levels.get(nextLevel).size() >= MAX_TABLES_PER_LEVEL) {
            compact(nextLevel);
        }
    }

    // Fechar
    // Garante que dados ainda na MemTable sejam salvos
    public void close() throws IOException {
        if (!memTable.isEmpty()) {
            flush();
        }
    }

    // Getters de métricas
    public int getCompactionCount() {
        return compactionCount;
    }

    public int getSSTCountAtLevel(int level) {
        return levels.get(level).size();
    }

    public int getMemTableSize() {
        return memTable.size();
    }
}