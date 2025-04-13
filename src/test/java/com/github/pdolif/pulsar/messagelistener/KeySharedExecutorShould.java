package com.github.pdolif.pulsar.messagelistener;

import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class KeySharedExecutorShould {

    private final String name = "executor1";
    private final OrderingKey orderingKey1 = new OrderingKey("key1".getBytes());
    private final Runnable messageListenerRunnable1 = () -> {};
    private KeySharedExecutor keySharedExecutor;
    private ExecutorServiceProvider executorServiceProviderMock;
    private ExecutorService virtualThreadExecutorService1;

    @BeforeEach
    public void setup() {
        executorServiceProviderMock = mock(ExecutorServiceProvider.class);
        virtualThreadExecutorService1 = spy(createVirtualThreadExecutorService());
        when(executorServiceProviderMock.createSingleThreadedExecutorService())
                .thenReturn(virtualThreadExecutorService1);
    }

    @Test
    public void haveName() {
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        assertThat(keySharedExecutor.getName()).isEqualTo(name);
    }

    @Test
    public void requireNonNullName() {
        assertThrows(IllegalArgumentException.class, () -> new KeySharedExecutor((String) null, executorServiceProviderMock));
    }

    @Test
    public void requireNonNullExecutorServiceProvider() {
        assertThrows(IllegalArgumentException.class, () -> new KeySharedExecutor(name, (ExecutorServiceProvider) null));
    }

    @Test
    public void useExecutorServiceProviderToCreateExecutorService() {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        // when executing a message listener runnable
        keySharedExecutor.execute(messageWith(orderingKey1), messageListenerRunnable1);

        // then the executor service provider is used to create an executor service
        verify(executorServiceProviderMock).createSingleThreadedExecutorService();
    }

    private static List<Arguments> orderingKeys() {
        return List.of(
                Arguments.of(new OrderingKey(null)),
                Arguments.of(new OrderingKey("key".getBytes()))
        );
    }

    @ParameterizedTest
    @MethodSource("orderingKeys")
    public void useProvidedExecutorServiceToExecuteRunnable(OrderingKey orderingKey) {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        // when executing a message listener runnable
        keySharedExecutor.execute(messageWith(orderingKey), messageListenerRunnable1);

        // then the executor service provided by the executor service provider is used to execute the runnable
        verify(virtualThreadExecutorService1).submit(messageListenerRunnable1);
    }

    private ExecutorService createVirtualThreadExecutorService() {
        return Executors.newSingleThreadExecutor(r -> Thread.ofVirtual().factory().newThread(r));
    }

    private Message<?> messageWith(OrderingKey orderingKey) {
        var message = mock(Message.class);
        when(message.getOrderingKey()).thenReturn(orderingKey.key());
        return message;
    }

}
