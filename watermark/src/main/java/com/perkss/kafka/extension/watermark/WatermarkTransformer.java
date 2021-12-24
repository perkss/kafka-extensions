package com.perkss.kafka.extension.watermark;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

// TODO metrics
public class WatermarkTransformer<V> implements ValueTransformer<V, V> {

    public static final String WATERMARK_HEADER = "watermark";
    private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;
    private final WatermarkGenerator<V> watermarkGenerator;
    private final String name;
    private final AtomicLong currentWatermarkAllPartitions = new AtomicLong(Long.MIN_VALUE);
    private ProcessorContext context;
    private String id;

    public WatermarkTransformer(WatermarkOutputMultiplexer watermarkOutputMultiplexer, WatermarkGenerator<V> watermarkGenerator, String name) {
        this.watermarkOutputMultiplexer = watermarkOutputMultiplexer;
        this.watermarkGenerator = watermarkGenerator;
        this.name = name;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.watermarkOutputMultiplexer.init(context);

        id = String.format("%s-%s", name, context.partition());
        this.watermarkOutputMultiplexer.registerNewOutput(id);

        // Watermark interval configurable
        this.context.schedule(Duration.ofSeconds(5),
                PunctuationType.STREAM_TIME, (time) -> onPeriodicEmit());
    }

    @Override
    public V transform(V value) {
        watermarkGenerator.onEvent(value, context.timestamp(), watermarkOutputMultiplexer.getImmediateOutput(id));

        if (context.headers() != null) {
            context.headers().add(WATERMARK_HEADER, ByteUtils.longToBytes(currentWatermarkAllPartitions.get()));
        }

        return value;
    }

    // called by punctuator

    /**
     * This will get the watermark for all partitions that are used in this task.
     * TODO wouldnt work in a actual multi instant Kafka Streams app? As not shared state
     * Store it in a state store?
     */
    void onPeriodicEmit() {
        watermarkGenerator.onPeriodicEmit(watermarkOutputMultiplexer.getDeferredOutput(id));
        Watermark allPartitionsWatermark = watermarkOutputMultiplexer.onPeriodicEmit();
        currentWatermarkAllPartitions.set(allPartitionsWatermark.getTimestamp());
    }

    @Override
    public void close() {
        watermarkOutputMultiplexer.unregisterOutput(id);
    }


}
