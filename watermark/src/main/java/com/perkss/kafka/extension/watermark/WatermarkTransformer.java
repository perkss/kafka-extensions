package com.perkss.kafka.extension.watermark;

import com.perkss.kafka.extension.watermark.generator.WatermarkGenerator;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A watermark transformer that will apply watermarks for each task associated and then combine them in the
 * related {@link WatermarkOutputMultiplexer}
 * <p>
 * Each value transformed will have the latest combined watermark added to it along with updating the multiplexer with
 * its own event time. The punctuator will call the {@link WatermarkTransformer#onPeriodicCalculate} to calculate the latest
 * combined watermark at the interval and update for this task.
 */
public class WatermarkTransformer<V> implements ValueTransformer<V, V> {

    private static final Logger logger = LoggerFactory.getLogger(WatermarkTransformer.class);
    public static final String WATERMARK_HEADER = "watermark";
    private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;
    private final WatermarkGenerator<V> watermarkGenerator;
    private final TimestampAssigner<V> timestampAssigner;
    private final String name;
    private final Duration periodicWatermarkCalculation;
    private final AtomicLong currentWatermarkAllPartitions = new AtomicLong(Long.MIN_VALUE);
    private final Set<String> registeredTaskNames = new HashSet<>();

    private ProcessorContext context;
    private WatermarkOutput deferredOutput;
    private Sensor watermarkLag;
    private Sensor currentEmitEventTimeLag;

    public WatermarkTransformer(WatermarkOutputMultiplexer watermarkOutputMultiplexer,
                                WatermarkGenerator<V> watermarkGenerator,
                                TimestampAssigner<V> timestampAssigner,
                                Duration periodicEmit,
                                String name) {
        this.watermarkOutputMultiplexer = watermarkOutputMultiplexer;
        this.watermarkGenerator = watermarkGenerator;
        this.timestampAssigner = timestampAssigner;
        this.periodicWatermarkCalculation = periodicEmit;
        this.name = name;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.watermarkLag = context.metrics().addSensor("watermark-lag", Sensor.RecordingLevel.INFO);
        this.currentEmitEventTimeLag = context.metrics().addSensor("current-emit-event-time-lag", Sensor.RecordingLevel.INFO);

        this.context.schedule(periodicWatermarkCalculation,
                PunctuationType.STREAM_TIME, (time) -> onPeriodicCalculate());
    }

    @Override
    public V transform(V value) {
        String partitionId = String.format("%s-%s", context.topic(), context.partition());
        if (!this.registeredTaskNames.contains(partitionId)) {
            this.watermarkOutputMultiplexer.registerNewOutput(partitionId);

            this.deferredOutput =
                    watermarkOutputMultiplexer.getDeferredOutput(partitionId);

            registeredTaskNames.add(partitionId);
        }


        watermarkGenerator.onEvent(value,
                timestampAssigner.extractTimestamp(value, context.timestamp()),
                watermarkOutputMultiplexer.getImmediateOutput(partitionId));

        if (context.headers() != null) {
            context.headers().add(WATERMARK_HEADER, ByteUtils.longToBytes(currentWatermarkAllPartitions.get()));
        }

        return value;
    }

    /**
     * This will get the watermark for all partitions that are used in this task. It then sets this tasks watermark.
     * <p>
     * TODO wouldnt work in a actual multi instant Kafka Streams app? As not shared state
     * Store it in a state store?
     */
    void onPeriodicCalculate() {
        watermarkGenerator.onPeriodicEmit(deferredOutput);
        Watermark allPartitionsWatermark = watermarkOutputMultiplexer.onPeriodicEmit();
        watermarkLag.record(System.currentTimeMillis() - allPartitionsWatermark.getTimestamp());
        currentEmitEventTimeLag.record(System.currentTimeMillis() - context.timestamp());
        currentWatermarkAllPartitions.set(allPartitionsWatermark.getTimestamp());
    }

    @Override
    public void close() {
        String partitionId = String.format("%s-%s", context.topic(), context.partition());
        logger.info("Closed {} Transformer", name);
        watermarkOutputMultiplexer.unregisterOutput(partitionId);
        currentWatermarkAllPartitions.set(Long.MIN_VALUE);
        context = null;
        watermarkLag = null;
        currentEmitEventTimeLag = null;
        deferredOutput = null;
    }

}
