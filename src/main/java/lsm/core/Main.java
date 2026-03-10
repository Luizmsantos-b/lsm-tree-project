package main.java.lsm.core;

//import main.java.lsm.core.MemTable;
//import main.java.lsm.core.SSTable;
import java.nio.file.Paths;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {

        MemTable tree = new MemTable(10);

        System.out.println("=== Inserindo dados ===");
        tree.insert("user_003", 30);
        tree.insert("user_001", 10);
        tree.insert("user_005", 50);
        tree.insert("user_002", 20);
        tree.insert("user_004", 40);

        System.out.println("\n=== InOrder (ordenado por chave) ===");
        tree.inOrder(tree.getRoot());

        System.out.println("\n=== Buscas ===");
        System.out.println("get(user_001) → esperado: 10   | resultado: " + tree.get("user_001"));
        System.out.println("get(user_005) → esperado: 50   | resultado: " + tree.get("user_005"));
        System.out.println("get(user_999) → esperado: null | resultado: " + tree.get("user_999"));

        System.out.println("\n=== Update ===");
        System.out.println("antes → get(user_001): " + tree.get("user_001"));
        tree.insert("user_001", 99);
        System.out.println("depois → get(user_001): " + tree.get("user_001"));

        System.out.println("\n=== Raiz ===");
        System.out.println("Chave da raiz  : " + tree.getRoot().key);
        System.out.println("Raiz é preta?  : " + !tree.getRoot().isRed);

        System.out.println("\n=== Capacidade ===");
        System.out.println("Tamanho atual  : " + tree.size());
        System.out.println("Está cheia?    : " + tree.isFull());

        System.out.println("\n=== Teste SSTable ===");
        List<String[]> entries = tree.getSortedEntries();

        SSTable sst = new SSTable(
                Paths.get("results/L0/sst_001.sst"),
                0,
                System.nanoTime());

        sst.write(entries);
        System.out.println("Arquivo criado em: " + sst.getFilePath());

        System.out.println("\n=== Buscas na SSTable ===");
        System.out.println("get(user_001) → esperado: 99   | resultado: " + sst.get("user_001"));
        System.out.println("get(user_003) → esperado: 30   | resultado: " + sst.get("user_003"));
        System.out.println("get(user_999) → esperado: null | resultado: " + sst.get("user_999"));

        System.out.println("\n=== ReadAll (simula compaction) ===");
        List<String[]> all = sst.readAll();
        for (String[] entry : all) {
            System.out.println(entry[0] + " → " + entry[1]);
        }

        System.out.println("\n=== Teste LSMTree ===");

        // MemTable com limite de 3 entradas para forçar flushes rápido
        LSMTree lsm = new LSMTree(3, "results");

        // Insere 14 entradas (vai forçar vários flushes e uma compaction)
        lsm.insert("user_001", 10);
        lsm.insert("user_002", 20);
        lsm.insert("user_003", 30);
        lsm.insert("user_004", 40);
        lsm.insert("user_005", 50);
        lsm.insert("user_006", 60);
        lsm.insert("user_007", 70);
        lsm.insert("user_008", 80);
        lsm.insert("user_009", 90);
        lsm.insert("user_010", 100);
        lsm.insert("user_011", 110);
        lsm.insert("user_012", 120);
        lsm.insert("user_013", 130);
        lsm.insert("user_014", 140);
        lsm.close();

        // Buscas
        System.out.println("\n=== Buscas na LSMTree ===");
        System.out.println("get(user_001) → esperado: 10  | resultado: " + lsm.get("user_001"));
        System.out.println("get(user_007) → esperado: 70  | resultado: " + lsm.get("user_007"));
        System.out.println("get(user_014) → esperado: 140 | resultado: " + lsm.get("user_014"));
        System.out.println("get(user_999) → esperado: null| resultado: " + lsm.get("user_999"));

        // Métricas
        System.out.println("\n=== Métricas ===");
        System.out.println("Compactions realizadas : " + lsm.getCompactionCount());
        System.out.println("SSTables em L0         : " + lsm.getSSTCountAtLevel(0));
        System.out.println("SSTables em L1         : " + lsm.getSSTCountAtLevel(1));
        System.out.println("Entradas na MemTable   : " + lsm.getMemTableSize());

    }

}