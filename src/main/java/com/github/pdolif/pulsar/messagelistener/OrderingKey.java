package com.github.pdolif.pulsar.messagelistener;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Wrapper for the byte array ordering key of a message.
 * @param key Message ordering key
 */
public record OrderingKey(byte[] key) {

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderingKey that = (OrderingKey) o;
        return Objects.deepEquals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }

    @Override
    public String toString() {
        String keyUtf8 = (key == null) ? null : new String(key, StandardCharsets.UTF_8);
        return "OrderingKey{" +
                "key=" + keyUtf8 +
                '}';
    }
}
