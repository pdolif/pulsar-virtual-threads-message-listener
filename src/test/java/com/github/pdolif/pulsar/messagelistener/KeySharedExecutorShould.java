package com.github.pdolif.pulsar.messagelistener;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class KeySharedExecutorShould {

    private final String name = "executor1";
    private final OrderingKey orderingKey1 = new OrderingKey("key1".getBytes());
    private final OrderingKey orderingKey2 = new OrderingKey("key2".getBytes());
    private final Runnable messageListenerRunnable1 = () -> {};
    private final Runnable messageListenerRunnable2 = () -> {};
    private KeySharedExecutor keySharedExecutor;
    private ExecutorServiceProvider executorServiceProviderMock;
    private ExecutorService virtualThreadExecutorService1;
    private ExecutorService virtualThreadExecutorService2;
    private MeterRegistry meterRegistry;

    @BeforeEach
    public void setup() {
        executorServiceProviderMock = mock(ExecutorServiceProvider.class);
        virtualThreadExecutorService1 = spy(createVirtualThreadExecutorService());
        virtualThreadExecutorService2 = spy(createVirtualThreadExecutorService());
        when(executorServiceProviderMock.createSingleThreadedExecutorService())
                .thenReturn(virtualThreadExecutorService1)
                .thenReturn(virtualThreadExecutorService2)
                .thenAnswer(invocation -> createVirtualThreadExecutorService());
        meterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    public void close() {
        if (keySharedExecutor != null) keySharedExecutor.close();
    }

    @Test
    public void haveName() {
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        assertThat(keySharedExecutor.getName()).isEqualTo(name);
    }

    @Test
    public void requireNonNullName() {
        assertThrows(IllegalArgumentException.class, () ->
                new KeySharedExecutor((String) null, executorServiceProviderMock));
        assertThrows(IllegalArgumentException.class, () ->
                new KeySharedExecutor((String) null, executorServiceProviderMock, Metrics.disabled()));
    }

    @Test
    public void requireNonNullExecutorServiceProvider() {
        assertThrows(IllegalArgumentException.class, () ->
                new KeySharedExecutor(name, (ExecutorServiceProvider) null));
        assertThrows(IllegalArgumentException.class, () ->
                new KeySharedExecutor(name, (ExecutorServiceProvider) null, Metrics.disabled()));
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

    @Test
    public void executeRunnablesOfSameOrderingKeySequentially() {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);
        // given message listener runnables that sleep 100ms
        var sleepDuration = 100;
        AtomicInteger finishedRunnablesCounter = new AtomicInteger(0);
        var runnable1 = sleep(sleepDuration, finishedRunnablesCounter);
        var runnable2 = sleep(sleepDuration, finishedRunnablesCounter);

        // when executing two message listener runnables (that sleep 100ms each) with the same ordering key
        long startTime = System.currentTimeMillis();
        keySharedExecutor.execute(messageWith(orderingKey1), runnable1);
        keySharedExecutor.execute(messageWith(orderingKey1), runnable2);

        await().pollInterval(10, MILLISECONDS).until(() -> finishedRunnablesCounter.get() == 2);
        long endTime = System.currentTimeMillis();
        // then the two message listener runnables are executed sequentially
        assertThat(endTime - startTime).isGreaterThanOrEqualTo(sleepDuration * 2);
    }

    @Test
    public void useDifferentExecutorServicesForRunnablesWithDifferentOrderingKeys() {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        // when executing two message listener runnables with different ordering keys
        keySharedExecutor.execute(messageWith(orderingKey1), messageListenerRunnable1);
        keySharedExecutor.execute(messageWith(orderingKey2), messageListenerRunnable2);

        // then the runnables are submitted to different executor services
        verify(virtualThreadExecutorService1, times(1)).submit(messageListenerRunnable1);
        verify(virtualThreadExecutorService2, times(1)).submit(messageListenerRunnable2);
    }

    @Test
    public void executeRunnablesOfDifferentOrderingKeysConcurrently() {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);
        // given message listener runnables that sleep 100ms
        var sleepDuration = 100;
        AtomicInteger finishedRunnablesCounter = new AtomicInteger(0);
        var runnable1 = sleep(sleepDuration, finishedRunnablesCounter);
        var runnable2 = sleep(sleepDuration, finishedRunnablesCounter);

        // when executing two message listener runnables (that sleep 100ms each) with different ordering keys
        long startTime = System.currentTimeMillis();
        keySharedExecutor.execute(messageWith(orderingKey1), runnable1);
        keySharedExecutor.execute(messageWith(orderingKey2), runnable2);

        await().pollInterval(10, MILLISECONDS).until(() -> finishedRunnablesCounter.get() == 2);
        long endTime = System.currentTimeMillis();
        // then the two message listener runnables are executed concurrently
        assertThat(endTime - startTime).isLessThan(sleepDuration * 2);
    }

    @Test
    public void shutdownIdleExecutorServiceAfterTaskCompleted() throws Exception {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        // when executing a message listener runnable
        keySharedExecutor.execute(messageWith(orderingKey1), messageListenerRunnable1);
        // and waiting until it is completed
        Thread.sleep(100);
        // and executing another message listener runnable for the same ordering key
        keySharedExecutor.execute(messageWith(orderingKey1), messageListenerRunnable2);

        // then the executor service of the first runnable is shutdown after the first runnable is completed
        // and the runnables are submitted to different executor services
        verify(virtualThreadExecutorService1, times(1)).submit(messageListenerRunnable1);
        verify(virtualThreadExecutorService2, times(1)).submit(messageListenerRunnable2);
        verify(virtualThreadExecutorService1).shutdown();
    }

    @Test
    public void reuseExecutorServiceForSameOrderingKeyUnderLoad() {
        // given an executor that uses a mocked executor service provider
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        // when executing lots of message listener runnables so that the executor does not become idle in between
        AtomicInteger finishedRunnablesCounter = new AtomicInteger(0);
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                keySharedExecutor.execute(messageWith(orderingKey1), sleep(2, finishedRunnablesCounter));
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // then all message listener runnables are executed
        await().pollInterval(10, MILLISECONDS).until(() -> finishedRunnablesCounter.get() == 100);
        // and they are executed using only few executor services
        verify(executorServiceProviderMock, atMost(5)).createSingleThreadedExecutorService();
    }

    @Test
    public void closeUnused() {
        var executor = new KeySharedExecutor(name, executorServiceProviderMock);
        executor.close();
    }

    @Test
    public void shutdownExecutorServicesOnClose() {
        var keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        keySharedExecutor.execute(messageWith(orderingKey1), sleep(1000));
        keySharedExecutor.close();

        verify(virtualThreadExecutorService1).shutdown();
    }

    @Test
    public void allowMultipleCloseCalls() {
        var executor = new KeySharedExecutor(name, executorServiceProviderMock);
        executor.close();
        executor.close();
    }

    @Test
    public void throwExceptionWhenExecutingAfterClose() {
        var keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        keySharedExecutor.execute(messageWith(orderingKey1), sleep(1000));
        keySharedExecutor.close();
        assertThrows(IllegalStateException.class, () ->
                keySharedExecutor.execute(messageWith(orderingKey1), sleep(1000)));
    }

    @Test
    public void skipExecutionIfNullExecutorServiceProvided() {
        // given an executor that uses a mocked executor service provider that provides a null executor service
        when(executorServiceProviderMock.createSingleThreadedExecutorService())
                .thenReturn(null)
                .thenReturn(virtualThreadExecutorService1);
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock);

        // when executing two message listener runnables
        keySharedExecutor.execute(messageWith(orderingKey1), messageListenerRunnable1);
        keySharedExecutor.execute(messageWith(orderingKey2), messageListenerRunnable2);

        // then the first runnable is skipped and the second one is executed
        verify(virtualThreadExecutorService1).submit(messageListenerRunnable2);
    }

    @Test
    public void requireNonNullMetrics() {
        assertThrows(IllegalArgumentException.class, () ->
                new KeySharedExecutor(name, executorServiceProviderMock, (Metrics) null));
    }

    @Test
    public void provideExecutorServiceCountMetric() throws InterruptedException {
        // given an executor with a meter registry
        keySharedExecutor = new KeySharedExecutor(name, executorServiceProviderMock, Metrics.with(meterRegistry));
        var delay = 200;

        // when executing two message listener runnables with the same ordering key
        keySharedExecutor.execute(messageWith(orderingKey1), sleep(delay));
        keySharedExecutor.execute(messageWith(orderingKey1), sleep(delay));

        // then the number of executor services used by the key shared executor is 1
        assertThatExecutorServiceCountMetricIs(meterRegistry, name, 1);

        // when executing message listener runnables with two additional ordering keys
        keySharedExecutor.execute(messageWith(orderingKey2), sleep(delay));
        keySharedExecutor.execute(messageWith(new OrderingKey("key3".getBytes())), sleep(delay));

        // then the number of executor services used by the key shared executor is 3
        assertThatExecutorServiceCountMetricIs(meterRegistry, name, 3);

        // when waiting for the runnables to complete
        Thread.sleep(delay * 2 + 50);

        // then the number of executor services used by the key shared executor is 0 again
        assertThatExecutorServiceCountMetricIs(meterRegistry, name, 0);
    }

    @Test
    public void provideExecutorServiceCountMetricForMultipleExecutors() {
        // given two executors that use the same meter registry
        var metrics = Metrics.with(meterRegistry);
        var executorName1 = "executor1";
        var executorName2 = "executor2";
        var executor1 = new KeySharedExecutor(executorName1, executorServiceProviderMock, metrics);
        var executor2 = new KeySharedExecutor(executorName2, executorServiceProviderMock, metrics);

        // when executing message listener runnables with both executors
        executor1.execute(messageWith(orderingKey1), sleep(100));
        executor2.execute(messageWith(orderingKey1), sleep(100));

        // then the number of executor services used by each executor is 1
        assertThatExecutorServiceCountMetricIs(meterRegistry, executorName1, 1);
        assertThatExecutorServiceCountMetricIs(meterRegistry, executorName2, 1);

        executor1.close();
        executor2.close();
    }

    @Test
    public void provideQueuedMessagesCountMetric() throws InterruptedException {
        // given two executors that use the same meter registry
        var metrics = Metrics.with(meterRegistry);
        var executorName1 = "executor1";
        var executorName2 = "executor2";
        var executor1 = new KeySharedExecutor(executorName1, executorServiceProviderMock, metrics);
        var executor2 = new KeySharedExecutor(executorName2, executorServiceProviderMock, metrics);
        var delay = 200;

        // when executing 3 message listener runnables with the same ordering key using the first executor
        executor1.execute(messageWith(orderingKey1), sleep(delay));
        executor1.execute(messageWith(orderingKey1), sleep(delay));
        executor1.execute(messageWith(orderingKey1), sleep(delay));
        // and executing 2 message listener runnables with different ordering keys using the second executor
        executor2.execute(messageWith(orderingKey1), sleep(delay));
        executor2.execute(messageWith(orderingKey2), sleep(delay));

        // then the metric reflects the number of queued messages for each executor
        assertThatQueuedMessagesMetricIs(meterRegistry, executorName1, 3);
        assertThatQueuedMessagesMetricIs(meterRegistry, executorName2, 2);

        // when waiting for the runnables to complete
        Thread.sleep(delay * 3 + 50);

        // then the metric is 0 for each executor
        assertThatQueuedMessagesMetricIs(meterRegistry, executorName1, 0);
        assertThatQueuedMessagesMetricIs(meterRegistry, executorName2, 0);

        executor1.close();
        executor2.close();
    }

    private ExecutorService createVirtualThreadExecutorService() {
        return Executors.newSingleThreadExecutor(r -> Thread.ofVirtual().factory().newThread(r));
    }

    private Message<?> messageWith(OrderingKey orderingKey) {
        var message = mock(Message.class);
        when(message.getOrderingKey()).thenReturn(orderingKey.key());
        return message;
    }

    private Runnable sleep(int delay, AtomicInteger finishedRunnablesCounter) {
        return () -> {
            try {
                Thread.sleep(delay);
                finishedRunnablesCounter.incrementAndGet();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable sleep(int delay) {
        return () -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void assertThatExecutorServiceCountMetricIs(MeterRegistry registry, String executorName, double expectedValue) {
        var value = registry.get("key.shared.executor.executor.service.count")
                .tags("executorName", executorName)
                .gauge()
                .value();
        assertThat(value).isEqualTo(expectedValue);
    }

    private void assertThatQueuedMessagesMetricIs(MeterRegistry registry, String executorName, double expectedValue) {
        var value = registry.get("key.shared.executor.queued.messages.count")
                .tags("executorName", executorName)
                .gauge()
                .value();
        assertThat(value).isEqualTo(expectedValue);
    }

}
