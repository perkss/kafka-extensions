package com.perkss.kafka.extension.watermark;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WatermarkStrategy<V> implements ValueTransformerSupplier<V, V> {

    private final Map<String, WatermarkOutputMultiplexer> multiplexers = new ConcurrentHashMap<>();
    private final WatermarkGenerator<V> watermarkGenerator;
    private String name;

    private WatermarkStrategy(WatermarkGenerator<V> strategy) {
        this.watermarkGenerator = strategy;
    }


//    static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
//        return (ctx) -> new AscendingTimestampsWatermarks<>();
//    }

    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return new WatermarkStrategy<T>(new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness));
    }

//    static <T> WatermarkStrategy<T> forGenerator(WatermarkGeneratorSupplier<T> generatorSupplier) {
//        return generatorSupplier::createWatermarkGenerator;
//    }


    public WatermarkStrategy<V> named(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ValueTransformer<V, V> get() {
        if (name == null) {
            throw new IllegalStateException("Name has not been set");
        }

        WatermarkTransformer<V> transformer;
        if (multiplexers.containsKey(name)) {
            WatermarkOutputMultiplexer existing = multiplexers.get(name);
            transformer = new WatermarkTransformer<V>(existing,
                    watermarkGenerator,
                    name);
        } else {
            WatermarkOutputMultiplexer watermarkOutputMultiplexer = new WatermarkOutputMultiplexer();
            transformer = new WatermarkTransformer<V>(watermarkOutputMultiplexer,
                    watermarkGenerator,
                    name);

            multiplexers.put(name, watermarkOutputMultiplexer);
        }
        return transformer;
    }
}
