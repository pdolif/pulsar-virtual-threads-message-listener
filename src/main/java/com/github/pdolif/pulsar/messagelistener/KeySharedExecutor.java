package com.github.pdolif.pulsar.messagelistener;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;

public class KeySharedExecutor implements MessageListenerExecutor {

    private final String name;
    private final ExecutorServiceProvider executorServiceProvider;

    public KeySharedExecutor(String name, ExecutorServiceProvider executorServiceProvider) {
        if (name == null) throw new IllegalArgumentException("Name cannot be null");
        if (executorServiceProvider == null) throw new IllegalArgumentException("ExecutorServiceProvider cannot be null");
        this.name = name;
        this.executorServiceProvider = executorServiceProvider;
    }

    @Override
    public void execute(Message<?> message, Runnable runnable) {
        var executor = executorServiceProvider.createSingleThreadedExecutorService();
        executor.submit(runnable);
    }

    public String getName() {
        return name;
    }
}
