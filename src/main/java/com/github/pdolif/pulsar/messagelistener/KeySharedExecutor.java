package com.github.pdolif.pulsar.messagelistener;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class KeySharedExecutor implements MessageListenerExecutor {

    private static final Logger log = LoggerFactory.getLogger(KeySharedExecutor.class);

    private final String name;
    private final ExecutorServiceProvider executorServiceProvider;

    private final HashMap<OrderingKey, ExecutorService> executorPerKey = new HashMap<>();
    private final ConcurrentHashMap<OrderingKey, Integer> queuedMessagesCountPerKey = new ConcurrentHashMap<>();

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
        queuedMessagesCountPerKey.compute(orderingKey, (k, queuedMessagesCount) -> {
            // get executor for this ordering key
            createExecutorIfNotExists(orderingKey);
            var executor = executorPerKey.get(orderingKey);

            // increment queued messages count for ordering key
            if (queuedMessagesCount == null) {
                queuedMessagesCount = 0;
            }
            queuedMessagesCount++;

            // submit runnable to executor for this ordering key
            executor.submit(messageListenerRunnable);

            // submit another runnable to decrement the queued messages count after the first runnable is processed
            executor.submit(() -> processedMessage(orderingKey));

            return queuedMessagesCount;
        });
    }

    private void processedMessage(OrderingKey orderingKey) {
        queuedMessagesCountPerKey.compute(orderingKey, (k, queuedMessagesCount) -> {
            if (queuedMessagesCount == null) {
                log.warn("[{}] No queued messages count found for ordering key {}", name, orderingKey);
                return null;
            }
            queuedMessagesCount--;

            if (queuedMessagesCount == 0) {
                // the last queued message for this ordering key has been processed
                // shutdown executor and remove it from cache
                executorPerKey.get(orderingKey).shutdown();
                executorPerKey.remove(orderingKey);
                // remove queued messages count for this ordering key
                return null;
            }

            return queuedMessagesCount;
        });
    }

    private void createExecutorIfNotExists(OrderingKey orderingKey) {
        executorPerKey.computeIfAbsent(orderingKey, key ->
                executorServiceProvider.createSingleThreadedExecutorService());
    }

    public String getName() {
        return name;
    }
}
