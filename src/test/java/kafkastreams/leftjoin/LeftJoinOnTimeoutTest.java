package kafkastreams.leftjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LeftJoinOnTimeoutTest {

    public static final String SOURCE_LHS_TOPIC = "source_lhs_topic";
    public static final String SOURCE_RHS_TOPIC = "source_rhs_topic";
    public static final String TARGET_TOPIC = "target_topic";
    public static final String SCHEDULED_CHANGE_LOG_TOPIC = "left_join_app_id-LJ_TIME_OUT-target_topic-STATE-STORE-0000000002-changelog";

    public static final List<String> TOPICS = asList(SOURCE_LHS_TOPIC, SOURCE_RHS_TOPIC, TARGET_TOPIC,
            SCHEDULED_CHANGE_LOG_TOPIC);

    public static final int NUM_PARTITIONS = 2;
    public static final long KEY_3 = 3L;
    public static final int SLIDING_WINDOW_DURATION_IN_MS = 100;

    @Rule
    public KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, false);

    private Producer<Long, String> producer;

    private Collection<ConsumerRecord<Long, String>> joinedMessages;

    private static final long KEY_1 = 1L;
    private static final long KEY_2 = 2L;

    private AdminClient adminClient;

    @Before
    public void before() throws ExecutionException, InterruptedException {
        adminClient = AdminClient.create(adminConfig());

        adminClient.createTopics(TOPICS.stream().map(name -> new NewTopic(name, NUM_PARTITIONS, (short)1))
                .collect(Collectors.toList())).all().get();

        producer = producer(LongSerializer.class, StringSerializer.class);

        joinedMessages = subscribe(TARGET_TOPIC);
    }

    @After
    public void after() {
        joinedMessages.clear();
    }

    @Test
    public void shouldJoinLeftWithRight(){

        KafkaStreams kafkaStreams = buildLeftJoinTopologyLongWindow();

        try {
            send(SOURCE_LHS_TOPIC, 1L, KEY_1, "left_1");
            send(SOURCE_LHS_TOPIC, 20L, KEY_1, "left_2");
            send(SOURCE_RHS_TOPIC, 1L, KEY_1, "right");

            await(joinedMessages, new String[]{"key", "value"},
                    new Tuple(KEY_1, "left_1+right"),
                    new Tuple(KEY_1, "left_2+right"));
        } finally {
            closeAndCleanUp(kafkaStreams);
        }
    }

    @Test
    public void shouldJoinLeftWithRightWoStateLog(){

        KafkaStreams kafkaStreams = buildLeftJoinTopology(
                TimeUnit.SECONDS.toMillis(100),
                TimeUnit.SECONDS.toMillis(300), false);

        try {
            send(SOURCE_LHS_TOPIC, 1L, KEY_1, "left_1");
            send(SOURCE_LHS_TOPIC, 20L, KEY_1, "left_2");
            send(SOURCE_RHS_TOPIC, 1L, KEY_1, "right");

            await(joinedMessages, new String[]{"key", "value"},
                    new Tuple(KEY_1, "left_1+right"),
                    new Tuple(KEY_1, "left_2+right"));
        } finally {
            closeAndCleanUp(kafkaStreams);
        }
    }

    @Test
    public void shouldLeftJoinOnTimeout(){

        KafkaStreams kafkaStreams = buildLeftJoinTopologyShortWindow();

        try {
            send(SOURCE_LHS_TOPIC, 1L, KEY_1, "left");

            await(joinedMessages, new String[]{"key", "value"}, new Tuple(KEY_1, "left+"));
        } finally {
            closeAndCleanUp(kafkaStreams);
        }
    }

    @Test
    public void shouldLeftJoinOnTimeoutAfterRestoration() {

        KafkaStreams kafkaStreams = buildLeftJoinTopologyLongWindow();

        try {
            send(SOURCE_LHS_TOPIC, 1L, KEY_1, "left");
            send(SOURCE_LHS_TOPIC, 1L, KEY_2, "left");

            await(subscribe(SCHEDULED_CHANGE_LOG_TOPIC), 2);

        } finally {
            closeAndCleanUp(kafkaStreams);
        }

        KafkaStreams kafkaStreamsNew = buildLeftJoinTopologyShortWindow();

        try {
            await(joinedMessages, new String[]{"key", "value"}, new Tuple(KEY_1, "left+"), new Tuple(KEY_2, "left+"));
        } finally {
            closeAndCleanUp(kafkaStreamsNew);
        }
    }

    @Test
    public void shouldLeftJoinOnTimeoutAfterRebalance() {

        KafkaStreams kafkaStreams = buildLeftJoinTopologyLongWindow();

        try {
            send(SOURCE_LHS_TOPIC, 1L, KEY_1, "left");
            send(SOURCE_LHS_TOPIC, 1L, KEY_3, "left");

            await(subscribe(SCHEDULED_CHANGE_LOG_TOPIC), 2);

            KafkaStreams kafkaStreamsNew = buildLeftJoinTopologyShortWindow();

            try {
                await(joinedMessages, 1);
            } finally {
                closeAndCleanUp(kafkaStreamsNew);
            }

        } finally {
            closeAndCleanUp(kafkaStreams);
        }
    }

    private void closeAndCleanUp(KafkaStreams kafkaStreams) {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    private KafkaStreams buildLeftJoinTopologyLongWindow() {
        return buildLeftJoinTopology(
                TimeUnit.SECONDS.toMillis(100),
                TimeUnit.SECONDS.toMillis(300), true);
    }

    private KafkaStreams buildLeftJoinTopologyShortWindow() {
        return buildLeftJoinTopology(SLIDING_WINDOW_DURATION_IN_MS,
                SLIDING_WINDOW_DURATION_IN_MS * 3,
                true);
    }


    private KafkaStreams buildLeftJoinTopology(
            long joinerSlidingWindowDurationInMs, long joinerSlidingWindowRetentionInMs, boolean enableStateLog){
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<Long, String> sourceLhs = kStreamBuilder.stream(Serdes.Long(), Serdes.String(), SOURCE_LHS_TOPIC);
        KStream<Long, String> sourceRhs = kStreamBuilder.stream(Serdes.Long(), Serdes.String(), SOURCE_RHS_TOPIC);

        LeftJoinOnTimeoutBuilder<Long, String, String, String> builder = new LeftJoinOnTimeoutBuilder<>(kStreamBuilder, sourceLhs, sourceRhs,
                (lhs, rhs) -> StringUtils.isNotEmpty(rhs) ? lhs + "+" + rhs : lhs + "+",
                joinerSlidingWindowDurationInMs, joinerSlidingWindowRetentionInMs)
                .sinkTo(TARGET_TOPIC, producer(ByteArraySerializer.class, ByteArraySerializer.class))
                .serdes(Serdes.Long(), Serdes.String(), Serdes.String(), Serdes.String());
        if(enableStateLog) {
            builder = builder.enableStateLog(Long.class, String.class);
        }
        builder.buildTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig());
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        return kafkaStreams;
    }

    private void await(Collection<ConsumerRecord<Long, String>> records, int size){
        Awaitility.await()
                .atMost(30, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> Assertions.assertThat(records).size().isEqualTo(size));
    }

    private void await(Collection<ConsumerRecord<Long, String>> records, String[] properties, Tuple... values){
        Awaitility.await()
                .atMost(30, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> Assertions.assertThat(records)
                        .extracting(properties)
                        .containsExactly(values));
    }

    private RecordMetadata send(String topic, long timestamp, Long key, String value){
        try {
            return producer.send(new ProducerRecord<>(topic, null, timestamp, key, value)).get();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private <K, V> Producer<K, V> producer(Class keySerializer, Class valueSerializer){
        return new KafkaProducer<>(producerConfigs(keySerializer, valueSerializer));
    }


    private Collection<ConsumerRecord<Long, String>> subscribe(String topic){
        ConcurrentLinkedQueue<ConsumerRecord<Long, String>> records = new ConcurrentLinkedQueue<>();
        final Consumer<Long, String> consumer = new KafkaConsumer<>(consumerConfigs());

        consumer.subscribe(singletonList(topic));

        new Thread(() -> {
            while (true) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10);
                consumerRecords.records(topic).forEach(records::add);
            }
        }).start();

        return records;
    }

    private Properties adminConfig() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokersAsString());
        return props;
    }

    private Properties streamsConfig(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "left_join_app_id");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "left_join_client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokersAsString());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        try {
            props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams-")
                    .toAbsolutePath().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        return props;
    }

    private Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokersAsString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private Map<String, Object> producerConfigs(Class keySerializer, Class valueSerializer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return props;
    }
}
