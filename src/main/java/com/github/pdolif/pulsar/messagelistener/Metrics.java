package com.github.pdolif.pulsar.messagelistener;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.function.ToDoubleFunction;

/**
 * Use {@code Metrics.with(meterRegistry)} to collect metrics with Micrometer.
 * Use {@code Metrics.disabled()} to disable metrics collection.
 */
public interface Metrics {

    /**
     * Register a gauge that measures the size of the given map.
     * @param name Name of the gauge
     * @param map Map to measure
     * @param description Description of the gauge
     * @param tags Tags to attach to the gauge
     */
    default void registerGaugeForMap(String name, Map<?, ?> map, String description, String... tags) {}

    /**
     * Register a gauge that measures the given object using the provided function.
     * @param name Name of the gauge
     * @param object Object to measure
     * @param function Function to measure the object
     * @param description Description of the gauge
     * @param tags Tags to attach to the gauge
     * @param <T> Type of the object to measure
     */
    default <T> void registerGaugeForMap(String name, T object, ToDoubleFunction<T> function, String description, String... tags) {}

    /**
     * Use {@code Metrics.disabled()} to disable metrics collection.
     */
    static Disabled disabled() {
        return new Disabled();
    }

    /**
     * Use {@code Metrics.with(meterRegistry)} to enable metrics collection with Micrometer.
     * @param meterRegistry Micrometer MeterRegistry to use for metrics collection
     * @return {@link MicrometerMetrics} instance that uses the provided MeterRegistry for metrics collection
     */
    static MicrometerMetrics with(MeterRegistry meterRegistry) {
        return new MicrometerMetrics(meterRegistry);
    }

    class Disabled implements Metrics {}

    class MicrometerMetrics implements Metrics {

        private final MeterRegistry meterRegistry;

        private MicrometerMetrics(MeterRegistry meterRegistry) {
            if (meterRegistry == null) throw new IllegalArgumentException("MeterRegistry cannot be null");
            this.meterRegistry = meterRegistry;
        }

        @Override
        public void registerGaugeForMap(String name, Map<?, ?> map, String description, String... tags) {
            Gauge.builder(name, map, Map::size)
                    .description(description)
                    .tags(tags)
                    .register(meterRegistry);
        }

        @Override
        public <T> void registerGaugeForMap(String name, T object, ToDoubleFunction<T> function, String description,
                                            String... tags) {
            Gauge.builder(name, object, function)
                    .description(description)
                    .tags(tags)
                    .register(meterRegistry);
        }
    }
}