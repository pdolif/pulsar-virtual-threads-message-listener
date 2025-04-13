package com.github.pdolif.pulsar.messagelistener;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class KeySharedExecutor implements MessageListenerExecutor, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KeySharedExecutor.class);

    private final String name;
    private final ExecutorServiceProvider executorServiceProvider;

    private final ConcurrentHashMap<OrderingKey, ExecutorService> executorPerKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<OrderingKey, Integer> queuedMessagesCountPerKey = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KeySharedExecutor(String name, ExecutorServiceProvider executorServiceProvider, Metrics metrics) {
        if (name == null) throw new IllegalArgumentException("Name cannot be null");
        if (executorServiceProvider == null) throw new IllegalArgumentException("ExecutorServiceProvider cannot be null");
        if (metrics == null) throw new IllegalArgumentException("Metrics cannot be null");
        this.name = name;
        this.executorServiceProvider = executorServiceProvider;

        metrics.registerGaugeForMap("key.shared.executor.queued.messages.count", queuedMessagesCountPerKey,
                map -> map.values().stream().mapToInt(Integer::intValue).sum(),
                "Number of queued messages for the key shared executor", "executorName", name);

        metrics.registerGaugeForMap("key.shared.executor.executor.service.count", executorPerKey,
                "Number of executor services used by the key shared executor", "executorName", name);
    }

    public KeySharedExecutor(String name, ExecutorServiceProvider executorServiceProvider) {
        this(name, executorServiceProvider, Metrics.disabled());
    }

    @Override
    public void execute(Message<?> message, Runnable runnable) {
        if (closed.get()) {
            throw new IllegalStateException("Cannot execute message listener task after executor is closed.");
        }
        var orderingKey = new OrderingKey(message.getOrderingKey());
        executeOrdered(orderingKey, runnable);
    }

    private void executeOrdered(OrderingKey orderingKey, Runnable messageListenerRunnable) {
        queuedMessagesCountPerKey.compute(orderingKey, (k, queuedMessagesCount) -> {
            // get executor for this ordering key
            createExecutorIfNotExists(orderingKey);
            var executor = executorPerKey.get(orderingKey);
            if (executor == null) {
                log.error("[{}] Null ExecutorService provided for ordering key {}. Cannot execute message listener task.",
                        name, orderingKey);
                return queuedMessagesCount;
            }

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

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            executorPerKey.values().forEach(ExecutorService::shutdown);
        }
    }

    public String getName() {
        return name;
    }
}
