package com.github.pdolif.pulsar.messagelistener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link ExecutorServiceProvider} that creates single-threaded {@link ExecutorService}s that utilize virtual threads.
 */
public class VirtualThreadExecutorServiceProvider implements ExecutorServiceProvider {
    @Override
    public ExecutorService createSingleThreadedExecutorService() {
        return Executors.newSingleThreadExecutor(r -> Thread.ofVirtual().factory().newThread(r));
    }
}
