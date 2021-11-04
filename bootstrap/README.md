# Kafka Bootstrap

A bootstrapping consumer that will reload a topic from the beginning
and bootstrap it too its local state. This is shared consumer so will 
select partitions as given to the consumer group along with removing 
and adding partition data on joins and leaves of the consumer group