package com.perkss.kafka.extension.watermark;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Watermark {

    private static final ThreadLocal<SimpleDateFormat> TS_FORMATTER = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    // ------------------------------------------------------------------------
    /**
     * The timestamp of the watermark in milliseconds.
     */
    private final long timestamp;

    /**
     * Creates a new watermark with the given timestamp in milliseconds.
     */
    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }


    /**
     * Returns the timestamp associated with this Watermark.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Formats the timestamp of this watermark, assuming it is a millisecond timestamp.
     * The returned format is "yyyy-MM-dd HH:mm:ss.SSS".
     */
    public String getFormattedTimestamp() {
        return TS_FORMATTER.get().format(new Date(timestamp));
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null &&
                        o.getClass() == Watermark.class &&
                        ((Watermark) o).timestamp == this.timestamp;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(timestamp);
    }

    @Override
    public String toString() {
        return "Watermark @ " + timestamp + " (" + getFormattedTimestamp() + ')';
    }

}
