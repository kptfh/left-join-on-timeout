package kafkastreams.leftjoin;

import com.fasterxml.jackson.core.type.TypeReference;
import kafkastreams.leftjoin.utils.JsonGenericDeserializer;
import kafkastreams.leftjoin.utils.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ScheduledStateStoreSupplier<K, V> implements StateStoreSupplier<ScheduledStateStore<K, V>> {
    private final String name;
    private final ScheduledStateStore.ScheduledTaskTransformer<K, V> scheduledTaskTransformer;
    private final long delayInMs;
    private final int capacity;
    private boolean stateLogEnabled = false;
    private Serde<K> keySerde;
    private Serde<Scheduled<K, V>> scheduledSerde;

    public ScheduledStateStoreSupplier(final String name, ScheduledStateStore.ScheduledTaskTransformer<K, V> scheduledTaskTransformer,
                                       long delay, TimeUnit unit, int capacity) {
        this.name = name;
        this.scheduledTaskTransformer = scheduledTaskTransformer;
        this.delayInMs = unit.toMillis(delay);
        this.capacity = capacity;
    }

    public ScheduledStateStoreSupplier<K, V> enableStateLog(
            Serde<K> keySerde, Class<K> keyClass, Class<V> valueClass){
        this.keySerde = keySerde;
        this.scheduledSerde = scheduledSerde(keyClass, valueClass);

        this.stateLogEnabled = true;
        return this;
    }

    static <K, V> Serde<Scheduled<K, V>> scheduledSerde(Class<K> keyClass, Class<V> valueClass){
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                scheduledJsonDeserializer(keyClass, valueClass));
    }

    private static <K, V> JsonGenericDeserializer<Scheduled<K, V>> scheduledJsonDeserializer(Class<K> keyClass, Class<V> valueClass) {
        return new JsonGenericDeserializer<>(
                new TypeReference<Object>() {
                    @Override
                    public Type getType(){
                        return new ParameterizedType() {
                            @Override
                            public Type[] getActualTypeArguments() {
                                return new Type[]{keyClass, valueClass};
                            }

                            @Override
                            public Type getRawType() {
                                return Scheduled.class;
                            }

                            @Override
                            public Type getOwnerType() {
                                return null;
                            }
                        };
                    }
                }
        );
    }

    @Override
    public ScheduledStateStore<K, V> get() {
        ScheduledStateStore<K, V> scheduledStateStore = new ScheduledStateStore<>(
                name, scheduledTaskTransformer, delayInMs, capacity);
        return stateLogEnabled ? scheduledStateStore.enableStateLog(keySerde, scheduledSerde) : scheduledStateStore;
    }

    @Override
    public boolean loggingEnabled() {
        return stateLogEnabled;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Map<String, String> logConfig() {
        return Collections.emptyMap();
    }
}
