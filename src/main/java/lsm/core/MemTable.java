package lsm.core;

import java.util.ArrayList;
import java.util.List;

public class MemTable {

    private Node root;
    private int size;
    private final int maxSize;

    public MemTable(int maxSize) {
        this.root = null;
        this.size = 0;
        this.maxSize = maxSize;
    }

    public void insert(String key, int value) {
        Node newNode = new Node(key, value);

        if (root == null) {
            root = newNode;
            root.isRed = false;
            size++;
            return;
        }

        if (insertBST(root, newNode)) {
            size++;
            fixInsert(newNode);
        }
    }

    // Navega pela árvore até achar o lugar certo
    // Retorna true se inseriu, false se era duplicata (atualiza valor)
    private boolean insertBST(Node current, Node newNode) {
        int cmp = newNode.key.compareTo(current.key);

        if (cmp == 0) {
            current.value = newNode.value; // atualiza valor existente
            return false;
        }

        if (cmp < 0) {
            if (current.left == null) {
                current.left = newNode;
                newNode.parent = current;
                return true;
            }
            return insertBST(current.left, newNode);
        } else {
            if (current.right == null) {
                current.right = newNode;
                newNode.parent = current;
                return true;
            }
            return insertBST(current.right, newNode);
        }
    }

    private void rotateLeft(Node x) {
        Node y = x.right;
        x.right = y.left;

        if (y.left != null)
            y.left.parent = x;

        y.parent = x.parent;

        if (x.parent == null)
            root = y;
        else if (x == x.parent.left)
            x.parent.left = y;
        else
            x.parent.right = y;

        y.left = x;
        x.parent = y;
    }

    private void rotateRight(Node y) {
        Node x = y.left;
        y.left = x.right;

        if (x.right != null)
            x.right.parent = y;

        x.parent = y.parent;

        if (y.parent == null)
            root = x;
        else if (y == y.parent.left)
            y.parent.left = x;
        else
            y.parent.right = x;

        x.right = y;
        y.parent = x;
    }

    private void fixInsert(Node node) {
        while (node.parent != null && node.parent.isRed) {
            Node parent = node.parent;
            Node grandParent = parent.parent;

            if (parent == grandParent.left) {
                Node uncle = grandParent.right;

                if (uncle != null && uncle.isRed) {
                    parent.isRed = false;
                    uncle.isRed = false;
                    grandParent.isRed = true;
                    node = grandParent;
                } else {
                    if (node == parent.right) {
                        rotateLeft(parent);
                        node = parent;
                        parent = node.parent;
                    }
                    parent.isRed = false;
                    grandParent.isRed = true;
                    rotateRight(grandParent);
                }
            } else {
                Node uncle = grandParent.left;

                if (uncle != null && uncle.isRed) {
                    parent.isRed = false;
                    uncle.isRed = false;
                    grandParent.isRed = true;
                    node = grandParent;
                } else {
                    if (node == parent.left) {
                        rotateRight(parent);
                        node = parent;
                        parent = node.parent;
                    }
                    parent.isRed = false;
                    grandParent.isRed = true;
                    rotateLeft(grandParent);
                }
            }
        }
        root.isRed = false;
    }

    public Integer get(String key) {
        return search(root, key);
    }

    private Integer search(Node current, String key) {
        if (current == null)
            return null;

        int cmp = key.compareTo(current.key);

        if (cmp == 0)
            return current.value;
        if (cmp < 0)
            return search(current.left, key);
        return search(current.right, key);
    }

    public void inOrder(Node node) {
        if (node == null)
            return;
        inOrder(node.left);
        System.out.println(node.key + " → " + node.value);
        inOrder(node.right);
    }

    // Retorna todos os dados ordenados por chave (usado no flush)
    public List<String[]> getSortedEntries() {
        List<String[]> entries = new ArrayList<>();
        collectInOrder(root, entries);
        return entries;
    }

    private void collectInOrder(Node node, List<String[]> entries) {
        if (node == null)
            return;
        collectInOrder(node.left, entries);
        entries.add(new String[] { node.key, String.valueOf(node.value) });
        collectInOrder(node.right, entries);
    }

    public boolean isFull() {
        return size >= maxSize;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public Node getRoot() {
        return root;
    }

    public void clear() {
        root = null;
        size = 0;
    }
}
