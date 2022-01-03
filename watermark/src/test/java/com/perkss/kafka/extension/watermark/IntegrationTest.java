package com.perkss.kafka.extension.watermark;

import com.perkss.kafka.extension.watermark.strategy.WatermarkStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class IntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);
    private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.0.0");
    private static final Serde<String> stringSerde = Serdes.String();

    private static AdminClient adminClient;
    private static KafkaProducer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;
    private static String inputTopic;
    private static String outputTopic;
    private static String bootstrapServers;

    @BeforeAll
    static void beforeAll() throws ExecutionException, InterruptedException, TimeoutException {
        KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE);
        kafka.start();
        inputTopic = "input-topic";
        outputTopic = "output-topic";
        bootstrapServers = kafka.getBootstrapServers();
        setup(bootstrapServers, 3, 1, Arrays.asList(inputTopic, outputTopic));
    }

    @Test
    void messagesFlowWithTimestampAssigned() throws ExecutionException, InterruptedException {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((key, value) -> logger.info("Received {}, {}", key, value))
                .transformValues(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withIdleness(Duration.ofMillis(500))
                        .named("name"))
                .peek((key, value) -> logger.info("Sending {}, {}", key, value))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();

        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        StreamsConfig config = new StreamsConfig(properties);

        KafkaStreams stream = new KafkaStreams(topology, config);

        stream.start();

        consumer.subscribe(Collections.singletonList(outputTopic));

        producer.send(new ProducerRecord<>(inputTopic, "1", "FirstMessage")).get();

        waitAtMost(120, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    assertThat(records.count(), equalTo(1));
                });

        consumer.unsubscribe();
    }

    private static void setup(String bootstrapServers,
                              int partitions,
                              int replicationFactor,
                              List<String> topicNames) throws ExecutionException, InterruptedException, TimeoutException {
        adminClient = AdminClient.create(ImmutableMap.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));

        producer = new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );

        consumer = new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        Collection<NewTopic> topics = new ArrayList<>();

        topicNames.forEach(topicName ->
                topics.add(new NewTopic(topicName, partitions, (short) replicationFactor))
        );

        adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

    }


}
