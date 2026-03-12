package lsm.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SSTable {

    private final Path filePath;
    private final int level;
    private final long timestamp;

    public SSTable(Path filePath, int level, long timestamp) {
        this.filePath = filePath;
        this.level = level;
        this.timestamp = timestamp;
    }

    // Salva os pares chave=valor no disco em ordem crescente de chave

    public void write(List<String[]> entries) throws IOException {
        Files.createDirectories(filePath.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            for (String[] entry : entries) {
                writer.write(entry[0] + "=" + entry[1]);
                writer.newLine();
            }
        }
    }

    // Busca um valor pela chave no arquivo

    public Integer get(String key) throws IOException {
        if (!Files.exists(filePath))
            return null;

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2 && parts[0].equals(key)) {
                    return Integer.parseInt(parts[1]);
                }
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
                if (parts.length == 2) {
                    result.add(new String[] { parts[0], parts[1] });
                }
            }
        }
        return result;
    }

    // Deleta o arquivo do disco (usado após compaction)
    
    public void delete() throws IOException {
        Files.deleteIfExists(filePath);
    }

    public Path getFilePath() {
        return filePath;
    }

    public int getLevel() {
        return level;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

