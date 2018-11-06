package kafkastreams.leftjoin;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;

public class LeftJoinOnTimeoutBuilder<K, LV, RV, JV> {

    private static Logger log = LoggerFactory.getLogger(LeftJoinOnTimeoutBuilder.class);

    public static final String SCHEDULED_STATE_STORE_PREFIX = "LJ_TIME_OUT";
    public static final long DEFAULT_TIMEOUT_GAP_IN_MS = 100;
    public static final int DEFAULT_SCHEDULED_CAPACITY = 1000;

    private KStreamBuilder kStreamBuilder;
    private KStream<K, LV> lhsStream;
    private KStream<K, RV> rhsStream;
    private ValueJoiner<? super LV, ? super RV, ? extends JV> joiner;
    private Producer<byte[], byte[]> producer;
    private String joinTopicName;
    private long joinWindowDurationInMs;
    private long joinWindowRetentionInMs;
    private long leftJoinTimeoutInMs;
    private int maxScheduled;
    private Serde<K> keySerde;
    private Serde<LV> lhsSerde;
    private Serde<RV> rhsSerde;
    private Serde<JV> joinedSerde;
    private Class<K> keyClass;
    private Class<LV> lhsClass;


    public LeftJoinOnTimeoutBuilder(
            KStreamBuilder kStreamBuilder,
            KStream<K, LV> lhsStream,
            KStream<K, RV> rhsStream,
            ValueJoiner<? super LV, ? super RV, ? extends JV> joiner,
            long joinWindowDurationInMs,
            long joinWindowRetentionInMs) {

        this.kStreamBuilder = kStreamBuilder;
        this.lhsStream = lhsStream;
        this.rhsStream = rhsStream;
        this.joiner = joiner;
        this.joinWindowDurationInMs = joinWindowDurationInMs;
        this.joinWindowRetentionInMs = joinWindowRetentionInMs;
        this.leftJoinTimeoutInMs = joinWindowDurationInMs + DEFAULT_TIMEOUT_GAP_IN_MS;
        this.maxScheduled = DEFAULT_SCHEDULED_CAPACITY;
    }

    public LeftJoinOnTimeoutBuilder<K, LV, RV, JV> sinkTo(
            String joinTopicName, Producer<byte[], byte[]> producer){
        this.joinTopicName = joinTopicName;
        this.producer = producer;
        return this;
    }

    public LeftJoinOnTimeoutBuilder<K, LV, RV, JV> serdes(
            Serde<K> keySerde, Serde<LV> lhsSerde, Serde<RV> rhsSerde, Serde<JV> joinedSerde){
        this.keySerde = keySerde;
        this.lhsSerde = lhsSerde;
        this.rhsSerde = rhsSerde;
        this.joinedSerde = joinedSerde;
        return this;
    }

    public LeftJoinOnTimeoutBuilder<K, LV, RV, JV> timeout(long leftJoinTimeoutInMs){
        this.leftJoinTimeoutInMs = leftJoinTimeoutInMs;
        return this;
    }

    public LeftJoinOnTimeoutBuilder<K, LV, RV, JV> scheduledCapacity(int maxScheduled){
        this.maxScheduled = maxScheduled;
        return this;
    }

    public LeftJoinOnTimeoutBuilder<K, LV, RV, JV> enableStateLog(Class<K> keyClass, Class<LV> lhsClass){
        this.keyClass = keyClass;
        this.lhsClass = lhsClass;
        return this;
    }

    public String buildTopology(){
        validateArguments();

        String scheduledStoreName = kStreamBuilder.newStoreName(SCHEDULED_STATE_STORE_PREFIX + "-" + joinTopicName+"-");

        ScheduledStateStoreSupplier<K, LV> scheduledStoreSupplier = getScheduledStoreSupplier(
                scheduledStoreName,
                joiner, joinTopicName, keySerde.serializer(), joinedSerde.serializer(),
                producer, leftJoinTimeoutInMs, maxScheduled);
        if(keyClass != null){
            scheduledStoreSupplier = scheduledStoreSupplier.enableStateLog(keySerde, keyClass, lhsClass);
        }

        kStreamBuilder.addStateStore(scheduledStoreSupplier);

        lhsStream.process(() -> new ScheduleProcessor<>(scheduledStoreName), scheduledStoreName);

        KStream<K, JV> joinedStream = lhsStream.join(rhsStream,
                joiner,
                JoinWindows.of(joinWindowDurationInMs).until(joinWindowRetentionInMs),
                keySerde, lhsSerde, rhsSerde);

        joinedStream.process(() -> new CancelProcessor<>(scheduledStoreName), scheduledStoreName);

        joinedStream.to(keySerde, joinedSerde, joinTopicName);

        return scheduledStoreName;
    }

    private void validateArguments(){
        notNull(kStreamBuilder, "kStreamBuilder is mandatory argument");
        notNull(lhsStream, "lhsStream is mandatory argument");
        notNull(rhsStream, "rhsStream is mandatory argument");
        notNull(joiner, "joiner is mandatory argument");
        isTrue(joinWindowDurationInMs > 0, "joinWindowDurationInMs should be positive");
        isTrue(leftJoinTimeoutInMs > joinWindowDurationInMs, "leftJoinTimeoutInMs should be out of joinWindowDurationInMs");
        notNull(joinTopicName, "joinTopicName is mandatory argument");
        notNull(producer, "producerTemplate is mandatory argument");
        notNull(keySerde, "keySerde is mandatory argument");
        notNull(lhsSerde, "lhsSerde is mandatory argument");
        notNull(rhsSerde, "rhsSerde is mandatory argument");
        notNull(joinedSerde, "joinedSerde is mandatory argument");
        isTrue(maxScheduled > 0, "maxScheduled should be positive");

        if(keyClass != null){
            notNull(lhsClass, "lhsClass is mandatory argument if log enabled");
        }

    }

    private static <K, LV, RV, JV> ScheduledStateStoreSupplier<K, LV> getScheduledStoreSupplier(
            String name,
            ValueJoiner<? super LV, ? super RV, ? extends JV> joiner,
            String joinTopicName,
            Serializer<K> keySerializer, Serializer<JV> joinedSerializer,
            Producer<byte[], byte[]> producer,
            long windowDurationInMs, int maxScheduled) {
        return new ScheduledStateStoreSupplier<>(
                name,
                sendLeftJoinedMessage(joinTopicName, keySerializer, joinedSerializer, producer, joiner),
                windowDurationInMs, TimeUnit.MILLISECONDS, maxScheduled);
    }

    private static <K, LV, RV, JV> ScheduledStateStore.ScheduledTaskTransformer<K, LV> sendLeftJoinedMessage(
            String joinTopicName,
            Serializer<K> keySerializer, Serializer<JV> joinedSerializer,
            Producer<byte[], byte[]> producer,
            ValueJoiner<? super LV, ? super RV, ? extends JV> joiner) {
        return scheduled -> () -> {
            JV leftJoinedValue = joiner.apply(scheduled.value, null);
            ProducerRecord<byte[], byte[]> leftJoinedRecord = new ProducerRecord<>(
                    joinTopicName, null, scheduled.timestamp,
                    keySerializer.serialize(joinTopicName, scheduled.key),
                    joinedSerializer.serialize(joinTopicName, leftJoinedValue));
            log.warn("Left joined message send on window end {}", leftJoinedRecord);
            producer.send(leftJoinedRecord);
        };
    }

}
