package com.perkss.kafka.extension.watermark;

import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link WatermarkOutputMultiplexer} combines the watermark (and idleness) updates of multiple
 * partitions/shards/splits into one combined watermark update and forwards it to an underlying
 * {@link WatermarkOutput}.
 *
 * <p>A multiplexed output can either be immediate or deferred. Watermark updates on an immediate
 * output will potentially directly affect the combined watermark state, which will be forwarded to
 * the underlying output immediately. Watermark updates on a deferred output will only update an
 * internal state but not directly update the combined watermark state. Only when {@link
 * #onPeriodicEmit()} is called will the deferred updates be combined and forwarded to the
 * underlying output.
 *
 * <p>For registering a new multiplexed output, you must first call {@link #registerNewOutput(String)}
 * and then call {@link #getImmediateOutput(String)} or {@link #getDeferredOutput(String)} with the output
 * ID you get from that. You can get both an immediate and deferred output for a given output ID,
 * you can also call the getters multiple times.
 *
 * <p><b>WARNING:</b>This class is not thread safe.
 */
public class WatermarkOutputMultiplexer {

    // TODO make thread safe or the watermark output
    private WatermarkOutput underlyingOutput;
    /**
     * Map view, to allow finding them when requesting the {@link WatermarkOutput} for a given id.
     */
    private final Map<String, OutputState> watermarkPerOutputId;
    /**
     * List of all watermark outputs, for efficient access.
     */
    private final List<OutputState> watermarkOutputs;
    private long combinedWatermark = Long.MIN_VALUE;

    /**
     * Creates a new {@link WatermarkOutputMultiplexer} that emits combined updates to the given
     * {@link WatermarkOutput}.
     */
    public WatermarkOutputMultiplexer() {
        this.watermarkPerOutputId = new HashMap<>();
        this.watermarkOutputs = new ArrayList<>();
    }

    // TODO add init method and context is passed
    public void init(ProcessorContext context) {
        // TODO this represents the entire feed so consumer that has gone idle?
        this.underlyingOutput = new SourceContextWatermarkOutputAdapter<String>();
    }

    /**
     * Registers a new multiplexed output, which creates internal states for that output and returns
     * an output ID that can be used to get a deferred or immediate {@link WatermarkOutput} for that
     * output.
     */
    // TODO here we register the task and partition
    public void registerNewOutput(String id) {
        final OutputState outputState = new OutputState();

        final OutputState previouslyRegistered = watermarkPerOutputId.putIfAbsent(id, outputState);
        if (previouslyRegistered != null) {
            throw new IllegalStateException(String.format("Already contains an output for ID %s", id));
        }

        watermarkOutputs.add(outputState);
    }

    public boolean unregisterOutput(String id) {
        final OutputState output = watermarkPerOutputId.remove(id);
        if (output != null) {
            watermarkOutputs.remove(output);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns an immediate {@link WatermarkOutput} for the given output ID.
     *
     * <p>>See {@link WatermarkOutputMultiplexer} for a description of immediate and deferred
     * outputs.
     */
    public WatermarkOutput getImmediateOutput(String outputId) {
        final OutputState outputState = watermarkPerOutputId.get(outputId);
        if (outputState == null) {
            throw new IllegalStateException(String.format("no output registered under id %s", outputId));
        }
        return new ImmediateOutput(outputState);
    }

    /**
     * Returns a deferred {@link WatermarkOutput} for the given output ID.
     *
     * <p>>See {@link WatermarkOutputMultiplexer} for a description of immediate and deferred
     * outputs.
     */
    public WatermarkOutput getDeferredOutput(String outputId) {
        final OutputState outputState = watermarkPerOutputId.get(outputId);

        if (outputState == null) {
            throw new IllegalStateException(String.format("no output registered under id %s", outputId));
        }

        return new DeferredOutput(outputState);
    }

    /**
     * Tells the {@link WatermarkOutputMultiplexer} to combine all outstanding deferred watermark
     * updates and possibly emit a new update to the underlying {@link WatermarkOutput}.
     *
     * @return
     */
    public Watermark onPeriodicEmit() {
        return updateCombinedWatermark();
    }

    /**
     * Checks whether we need to update the combined watermark. Should be called when a newly
     * emitted per-output watermark is higher than the max so far or if we need to combined the
     * deferred per-output updates.
     *
     * @return
     */
    private Watermark updateCombinedWatermark() {
        long minimumOverAllOutputs = Long.MAX_VALUE;

        boolean hasOutputs = false;
        boolean allIdle = true;
        for (OutputState outputState : watermarkOutputs) {
            if (!outputState.isIdle()) {
                minimumOverAllOutputs = Math.min(minimumOverAllOutputs, outputState.getWatermark());
                allIdle = false;
            }
            hasOutputs = true;
        }

        // if we don't have any outputs minimumOverAllOutputs is not valid, it's still
        // at its initial Long.MAX_VALUE state and we must not emit that
        if (!hasOutputs) {
            return new Watermark(combinedWatermark);
        }

        if (allIdle) {
            // seems like this is not partition level and should be or the outputs are
            underlyingOutput.markIdle();
            return new Watermark(combinedWatermark);
        } else if (minimumOverAllOutputs > combinedWatermark) {
            combinedWatermark = minimumOverAllOutputs;
            // need each transformer for eacb partition to have this tine set
            underlyingOutput.emitWatermark(new Watermark(minimumOverAllOutputs));
            return new Watermark(minimumOverAllOutputs);
        }
        return new Watermark(combinedWatermark);
    }

    /**
     * Per-output watermark state.
     */
    private static class OutputState {
        private long watermark = Long.MIN_VALUE;
        private boolean idle = false;

        /**
         * Returns the current watermark timestamp. This will throw {@link IllegalStateException} if
         * the output is currently idle.
         */
        public long getWatermark() {
            if (idle) {
                throw new IllegalStateException(String.format("Output is idle."));
            }
            return watermark;
        }

        /**
         * Returns true if the watermark was advanced, that is if the new watermark is larger than
         * the previous one.
         *
         * <p>Setting a watermark will clear the idleness flag.
         */
        public boolean setWatermark(long watermark) {
            this.idle = false;
            final boolean updated = watermark > this.watermark;
            this.watermark = Math.max(watermark, this.watermark);
            return updated;
        }

        public boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
        }
    }

    /**
     * Updating the state of a deferred output will never lead to a combined watermark update. Only
     * when {@link WatermarkOutputMultiplexer#onPeriodicEmit()} is called will the deferred updates
     * be combined into a potential combined update of the underlying {@link WatermarkOutput}.
     */
    private static class DeferredOutput implements WatermarkOutput {

        private final OutputState state;

        public DeferredOutput(OutputState state) {
            this.state = state;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            state.setWatermark(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {
            state.setIdle(true);
        }
    }

    /**
     * Updating the state of an immediate output can possible lead to a combined watermark update to
     * the underlying {@link WatermarkOutput}.
     */
    private class ImmediateOutput implements WatermarkOutput {

        private final OutputState state;

        public ImmediateOutput(OutputState state) {
            this.state = state;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            long timestamp = watermark.getTimestamp();
            boolean wasUpdated = state.setWatermark(timestamp);

            // if it's higher than the max watermark so far we might have to update the
            // combined watermark
            if (wasUpdated && timestamp > combinedWatermark) {
                updateCombinedWatermark();
            }
        }

        @Override
        public void markIdle() {
            state.setIdle(true);

            // this can always lead to an advancing watermark. We don't know if this output
            // was holding back the watermark or not.
            updateCombinedWatermark();
        }
    }

}
