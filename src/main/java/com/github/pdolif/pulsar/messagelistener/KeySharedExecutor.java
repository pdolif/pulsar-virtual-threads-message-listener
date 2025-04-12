package com.github.pdolif.pulsar.messagelistener;

public class KeySharedExecutor {

    private final String name;

    public KeySharedExecutor(String name) {
        if (name == null) throw new IllegalArgumentException("Name cannot be null");
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
