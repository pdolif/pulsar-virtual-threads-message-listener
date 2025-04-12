package com.github.pdolif.pulsar.messagelistener;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OrderingKeyShould {

    @Test
    public void beEqualToOtherOrderingKeyWithSameKeyBytes() {
        var orderingKey1 = new OrderingKey("key1".getBytes());
        var orderingKey2 = new OrderingKey("key1".getBytes());
        assertThat(orderingKey1).isEqualTo(orderingKey2);
    }

    @Test
    public void beEqualIfBothKeyBytesAreNull() {
        var orderingKey1 = new OrderingKey(null);
        var orderingKey2 = new OrderingKey(null);
        assertThat(orderingKey1).isEqualTo(orderingKey2);
    }

    @Test
    public void notBeEqualIfKeyBytesDiffer() {
        var orderingKey1 = new OrderingKey("key1".getBytes());
        var orderingKey2 = new OrderingKey("key2".getBytes());
        assertThat(orderingKey1).isNotEqualTo(orderingKey2);
    }

    @Test
    public void notBeEqualIfOnlyOneKeyBytesAreNull() {
        var orderingKey1 = new OrderingKey("key1".getBytes());
        var orderingKey2 = new OrderingKey(null);
        assertThat(orderingKey1).isNotEqualTo(orderingKey2);
    }

    @Test
    public void notBeEqualToNull() {
        var orderingKey = new OrderingKey("key1".getBytes());
        assertThat(orderingKey).isNotEqualTo(null);
    }

    private static List<Arguments> orderingKeyToString() {
        return List.of(
                Arguments.of(new OrderingKey(null), "OrderingKey{key=null}"),
                Arguments.of(new OrderingKey("key1".getBytes()), "OrderingKey{key=key1}")
        );
    }

    @ParameterizedTest
    @MethodSource("orderingKeyToString")
    public void haveStringRepresentation(OrderingKey orderingKey, String expectedToString) {
        assertThat(orderingKey.toString()).isEqualTo(expectedToString);
    }

}
