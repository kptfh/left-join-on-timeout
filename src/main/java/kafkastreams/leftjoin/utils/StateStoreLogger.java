package kafkastreams.leftjoin.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordCollector;

public class StateStoreLogger<K, V> {
    private final String storeName;
    private final ProcessorContext context;
    private final RecordCollector recordCollector;
    private final String topic;
    private final int partition;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public StateStoreLogger(String storeName, ProcessorContext context, Serde<K> keySerde, Serde<V> valueSerde) {
        this.storeName = storeName;
        this.context = context;
        this.recordCollector = ((RecordCollector.Supplier) context).recordCollector();
        this.topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        this.partition = context.taskId().partition;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public void logAdded(K key, V value){
        recordCollector.send(this.topic, key, value, this.partition, context.timestamp(),
                keySerde.serializer(), valueSerde.serializer());
    }

    public void logRemoved(K key){
        recordCollector.send(this.topic, key, null, this.partition, context.timestamp(),
                keySerde.serializer(), valueSerde.serializer());
    }

    public KeyValue<K, V> readChange(byte[] key, byte[] value){
        return new KeyValue<>(
                keySerde.deserializer().deserialize(topic, key),
                valueSerde.deserializer().deserialize(topic, value));
    }

    public String getStoreName() {
        return storeName;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }
}