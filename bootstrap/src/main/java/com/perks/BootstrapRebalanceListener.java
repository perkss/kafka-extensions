package com.perks;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class BootstrapRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
