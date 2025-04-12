package com.github.pdolif.pulsar.messagelistener;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeySharedExecutorShould {

    private final String name = "executor1";
    private KeySharedExecutor keySharedExecutor;

    @Test
    public void haveName() {
        keySharedExecutor = new KeySharedExecutor(name);

        assertThat(keySharedExecutor.getName()).isEqualTo(name);
    }

    @Test
    public void requireNonNullName() {
        assertThrows(IllegalArgumentException.class, () -> new KeySharedExecutor((String) null));
    }

}
