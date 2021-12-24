package com.perkss.kafka.extension.watermark;

public class DefaultTimestampAssigner<T> implements TimestampAssigner<T> {

    @Override
    public long extractTimestamp(T element, long recordTimestamp) {
        return recordTimestamp;
    }
}
