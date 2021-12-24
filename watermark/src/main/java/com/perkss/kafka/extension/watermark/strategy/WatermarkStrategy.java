package com.perkss.kafka.extension.watermark.strategy;

import com.perkss.kafka.extension.watermark.DefaultTimestampAssigner;
import com.perkss.kafka.extension.watermark.TimestampAssigner;
import com.perkss.kafka.extension.watermark.WatermarkOutputMultiplexer;
import com.perkss.kafka.extension.watermark.WatermarkTransformer;
import com.perkss.kafka.extension.watermark.generator.BoundedOutOfOrdernessWatermarks;
import com.perkss.kafka.extension.watermark.generator.WatermarkGenerator;
import com.perkss.kafka.extension.watermark.generator.WatermarksWithIdleness;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WatermarkStrategy<V> implements ValueTransformerSupplier<V, V> {

    private final Map<String, WatermarkOutputMultiplexer> multiplexers = new ConcurrentHashMap<>();
    private WatermarkGenerator<V> watermarkGenerator;
    private TimestampAssigner<V> timestampAssigner;
    private Duration periodicEmit = Duration.ofMillis(100);
    private String name;

    private WatermarkStrategy(WatermarkGenerator<V> strategy) {
        this.watermarkGenerator = strategy;
    }

    public static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return new WatermarkStrategy<T>(new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness));
    }

    public WatermarkStrategy<V> withWatermarkInterval(Duration interval) {
        this.periodicEmit = interval;
        return this;
    }

    public WatermarkStrategy<V> withTimestampAssigner(TimestampAssigner<V> assigner) {
        this.timestampAssigner = assigner;
        return this;
    }

    public WatermarkStrategy<V> withIdleness(Duration idleTimeout) {
        this.watermarkGenerator = new WatermarksWithIdleness<>(this.watermarkGenerator, idleTimeout);
        return this;
    }

    public WatermarkStrategy<V> named(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ValueTransformer<V, V> get() {
        if (name == null) {
            throw new IllegalStateException("Name has not been set");
        }

        if (timestampAssigner == null) {
            timestampAssigner = new DefaultTimestampAssigner<V>();
        }

        WatermarkTransformer<V> transformer;
        if (multiplexers.containsKey(name)) {
            WatermarkOutputMultiplexer existing = multiplexers.get(name);
            transformer = new WatermarkTransformer<V>(existing,
                    watermarkGenerator,
                    timestampAssigner,
                    periodicEmit,
                    name);
        } else {
            WatermarkOutputMultiplexer watermarkOutputMultiplexer = new WatermarkOutputMultiplexer();
            transformer = new WatermarkTransformer<V>(watermarkOutputMultiplexer,
                    watermarkGenerator,
                    timestampAssigner,
                    periodicEmit,
                    name);

            multiplexers.put(name, watermarkOutputMultiplexer);
        }
        return transformer;
    }
}
