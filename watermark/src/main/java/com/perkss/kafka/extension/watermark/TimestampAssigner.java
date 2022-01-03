package com.perkss.kafka.extension.watermark;

@FunctionalInterface
public interface TimestampAssigner<T> {


    /**
     * Assigns a timestamp to an element, in milliseconds since the Epoch. This is independent of
     * any particular time zone or calendar.
     *
     * <p>The method is passed the previously assigned timestamp of the element.
     * That previous timestamp may have been assigned from a previous assigner.
     *
     * @param element         The element that the timestamp will be assigned to.
     * @param recordTimestamp The current internal timestamp of the element,
     *                        or a negative value, if no timestamp has been assigned yet.
     * @return The new timestamp.
     */
    long extractTimestamp(T element, long recordTimestamp);
}
