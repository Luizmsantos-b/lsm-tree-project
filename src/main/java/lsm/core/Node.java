package main.java.lsm.core;

public class Node {

    String key;
    int value;

    Node left;
    Node right;
    Node parent;

    boolean isRed;

    Node(String key, int value) {
        this.key   = key;
        this.value = value;
        this.isRed = true;
    }

    public boolean hasOnlyLeftChild() {
        return (this.left != null && this.right == null);
    }

    public boolean hasOnlyRightChild() {
        return (this.left == null && this.right != null);
    }

    public boolean isLeaf() {
        return this.left == null && this.right == null;
    }
}
