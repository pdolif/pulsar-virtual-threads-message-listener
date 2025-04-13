package com.github.pdolif.pulsar.messagelistener;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

public class KeySharedExecutor implements MessageListenerExecutor {

    private final String name;
    private final ExecutorServiceProvider executorServiceProvider;

    private final HashMap<OrderingKey, ExecutorService> executorPerKey = new HashMap<>();

    public KeySharedExecutor(String name, ExecutorServiceProvider executorServiceProvider) {
        if (name == null) throw new IllegalArgumentException("Name cannot be null");
        if (executorServiceProvider == null) throw new IllegalArgumentException("ExecutorServiceProvider cannot be null");
        this.name = name;
        this.executorServiceProvider = executorServiceProvider;
    }

    @Override
    public void execute(Message<?> message, Runnable runnable) {
        var orderingKey = new OrderingKey(message.getOrderingKey());
        executeOrdered(orderingKey, runnable);
    }

    private void executeOrdered(OrderingKey orderingKey, Runnable messageListenerRunnable) {
        // get executor for this ordering key
        createExecutorIfNotExists(orderingKey);
        var executor = executorPerKey.get(orderingKey);

        // submit runnable to executor for this ordering key
        executor.submit(messageListenerRunnable);
    }

    private void createExecutorIfNotExists(OrderingKey orderingKey) {
        executorPerKey.computeIfAbsent(orderingKey, key ->
                executorServiceProvider.createSingleThreadedExecutorService());
    }

    public String getName() {
        return name;
    }
}
