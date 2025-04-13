package com.github.pdolif.pulsar.messagelistener;

/**
 * {@link KeySharedExecutor} implementation that ensures that {@link org.apache.pulsar.client.api.MessageListener} tasks
 * for messages with the same ordering key are executed in order.
 * This implementation utilizes virtual threads to run the message listener tasks.
 * <p>
 * See {@link KeySharedExecutor} for more details.
 */
public class VirtualThreadKeySharedExecutor extends KeySharedExecutor {

    /**
     * Creates a new executor with the given name and {@link Metrics}.
     * @param name Name of the KeySharedExecutor used to identify it in metrics and logs
     * @param metrics Metrics instance to monitor the number of queued messages and the number of executor services
     */
    public VirtualThreadKeySharedExecutor(String name, Metrics metrics) {
        super(name, new VirtualThreadExecutorServiceProvider(), metrics);
    }

    /**
     * Creates a new executor with the given name.
     * @param name Name of the KeySharedExecutor used to identify it in logs
     */
    public VirtualThreadKeySharedExecutor(String name) {
        super(name, new VirtualThreadExecutorServiceProvider(), Metrics.disabled());
    }

}