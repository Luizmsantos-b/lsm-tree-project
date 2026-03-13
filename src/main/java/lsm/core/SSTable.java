package lsm.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SSTable {

    private final Path filePath;
    private final Path indexPath;
    private final int level;
    private final long timestamp;

    // Índice carregado uma única vez em memória — reutilizado em todas as buscas
    private List<String> indexKeys    = null;
    private List<Long>   indexOffsets = null;

    public SSTable(Path filePath, int level, long timestamp) {
        this.filePath  = filePath;
        this.indexPath = Path.of(filePath.toString().replace(".sst", ".index"));
        this.level     = level;
        this.timestamp = timestamp;
    }

    // Salva os pares chave=valor no disco em ordem crescente de chave.
    // Gera também um arquivo .index com os offsets de byte de cada linha
    // e carrega esse índice em memória imediatamente.

    public void write(List<String[]> entries) throws IOException {
        Files.createDirectories(filePath.getParent());

        List<String> keys    = new ArrayList<>(entries.size());
        List<Long>   offsets = new ArrayList<>(entries.size());

        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            long offset = 0;
            for (String[] entry : entries) {
                keys.add(entry[0]);
                offsets.add(offset);
                String line = entry[0] + "=" + entry[1] + "\n";
                writer.write(line);
                offset += line.getBytes().length;
            }
        }

        // Persiste o índice no disco
        try (BufferedWriter idxWriter = Files.newBufferedWriter(indexPath)) {
            for (int i = 0; i < keys.size(); i++) {
                idxWriter.write(keys.get(i) + "=" + offsets.get(i));
                idxWriter.newLine();
            }
        }

        // Mantém o índice em memória para buscas imediatas
        this.indexKeys    = keys;
        this.indexOffsets = offsets;
    }

    // Carrega o índice do disco para memória (chamado uma única vez por SSTable).
    // Nas buscas subsequentes o índice já está em memória — sem I/O de índice.

    private void loadIndex() throws IOException {
        indexKeys    = new ArrayList<>();
        indexOffsets = new ArrayList<>();

        if (!Files.exists(indexPath))
            return;

        try (BufferedReader reader = Files.newBufferedReader(indexPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                int sep = line.lastIndexOf('=');
                if (sep > 0) {
                    indexKeys.add(line.substring(0, sep));
                    indexOffsets.add(Long.parseLong(line.substring(sep + 1)));
                }
            }
        }
    }

    // Busca um valor pela chave usando busca binária sobre o índice em memória.
    // O índice é carregado do disco apenas na primeira chamada — O(n) uma vez.
    // Buscas seguintes: O(log n) puro, sem nenhum I/O de índice.

    public Integer get(String key) throws IOException {
        if (!Files.exists(filePath))
            return null;

        // Sem índice no disco: cai no fallback linear
        if (!Files.exists(indexPath))
            return getLinear(key);

        // Carrega o índice na primeira busca desta SSTable
        if (indexKeys == null)
            loadIndex();

        if (indexKeys.isEmpty())
            return null;

        // Busca binária sobre as chaves do índice (já em memória)
        int lo = 0, hi = indexKeys.size() - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = indexKeys.get(mid).compareTo(key);
            if (cmp == 0) { found = mid; break; }
            else if (cmp < 0) lo = mid + 1;
            else              hi = mid - 1;
        }

        if (found == -1)
            return null;

        // Seek direto para o offset no .sst — lê apenas 1 linha
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            raf.seek(indexOffsets.get(found));
            String line = raf.readLine();
            if (line != null) {
                int sep = line.indexOf('=');
                if (sep > 0)
                    return Integer.parseInt(line.substring(sep + 1).trim());
            }
        }

        return null;
    }

    // Fallback linear para arquivos sem índice (compatibilidade)
    private Integer getLinear(String key) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2 && parts[0].equals(key))
                    return Integer.parseInt(parts[1]);
            }
        }
        return null;
    }

    // Lê todos os pares do arquivo (usado na compaction)

    public List<String[]> readAll() throws IOException {
        List<String[]> result = new ArrayList<>();
        if (!Files.exists(filePath))
            return result;

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2)
                    result.add(new String[] { parts[0], parts[1] });
            }
        }
        return result;
    }

    // Deleta o .sst e o .index do disco (usado após compaction)

    public void delete() throws IOException {
        Files.deleteIfExists(filePath);
        Files.deleteIfExists(indexPath);
        indexKeys    = null;
        indexOffsets = null;
    }

    public Path getFilePath() { return filePath;  }
    public int  getLevel()    { return level;      }
    public long getTimestamp(){ return timestamp;  }
}