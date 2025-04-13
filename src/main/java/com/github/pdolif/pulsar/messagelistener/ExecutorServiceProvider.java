package com.github.pdolif.pulsar.messagelistener;

import java.util.concurrent.ExecutorService;

/**
 * Interface to provide {@link ExecutorService} instances for {@link KeySharedExecutor}s.
 * The provided executor services are used to execute {@link org.apache.pulsar.client.api.MessageListener} tasks.
 * This allows to customize how the message listener tasks are executed, e.g. on virtual threads.
 */
@FunctionalInterface
public interface ExecutorServiceProvider {

    /**
     * Creates a new single-threaded {@link ExecutorService} to be used by a {@link KeySharedExecutor} for running
     * {@link org.apache.pulsar.client.api.MessageListener} tasks.
     * @return Single-threaded executor service
     */
    ExecutorService createSingleThreadedExecutorService();
}