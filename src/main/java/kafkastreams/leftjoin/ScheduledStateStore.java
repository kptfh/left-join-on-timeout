package kafkastreams.leftjoin;

import kafkastreams.leftjoin.utils.BlockingScheduledExecutor;
import kafkastreams.leftjoin.utils.StateStoreLogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ScheduledStateStore<K, V> implements StateStore {

    private final String name;
    private final long delayInMs;
    private final ScheduledTaskTransformer<K, V> scheduledTaskTransformer;
    private final BlockingScheduledExecutor executor;
    private final Map<K, Scheduled<K, V>> keyToScheduledMap;
    private boolean open;

    private boolean stateLogEnabled;
    private Serde<K> keySerde;
    private Serde<Scheduled<K, V>> scheduledSerde;
    private StateStoreLogger<K, Scheduled<K, V>> stateLogger;

    public ScheduledStateStore(String name,
                        ScheduledTaskTransformer<K, V> scheduledTaskTransformer,
                        long delayInMs, int capacity) {
        this.name = name;
        this.scheduledTaskTransformer = scheduledTaskTransformer;
        this.delayInMs = delayInMs;

        this.executor = new BlockingScheduledExecutor(new ScheduledThreadPoolExecutor(1), capacity);
        keyToScheduledMap = new ConcurrentHashMap<>(capacity);
    }

    public ScheduledStateStore<K, V> enableStateLog(Serde<K> keySerde, Serde<Scheduled<K, V>> scheduledSerde){
        this.keySerde = keySerde;
        this.scheduledSerde = scheduledSerde;
        this.stateLogEnabled = true;
        return this;
    }

    public void schedule(K key, V value, ProcessorContext context){
        Scheduled<K, V> scheduled = new Scheduled<>(key, value, context.timestamp());
        scheduleImpl(scheduled);

        if(stateLogEnabled) {
            stateLogger.logAdded(key, scheduled);
        }
        log.debug("Scheduled task {} for key {}", scheduled, key);
    }

    private void scheduleImpl(Scheduled<K, V> scheduled){
        Runnable command = scheduledTaskTransformer.buildTask(scheduled);
        scheduled.setScheduledFuture(executor.schedule(
                () -> {
                    try {
                        command.run();
                    } finally {
                        keyToScheduledMap.remove(scheduled.key);
                    }
                }, delayInMs, TimeUnit.MILLISECONDS));
        keyToScheduledMap.put(scheduled.key, scheduled);
    }

    public void cancel(K key){
        Scheduled<K, V> scheduled = keyToScheduledMap.get(key);
        if(scheduled != null){
            executor.cancel(scheduled.getScheduledFuture());
            keyToScheduledMap.remove(key);
            if(stateLogEnabled) {
                stateLogger.logRemoved(key);
            }
        } else {
            log.warn("No scheduled task for key: {}", key);
        }
        log.debug("Cancelled scheduled task {} for key {}", scheduled, key);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        if(stateLogEnabled) {
            this.stateLogger = new StateStoreLogger<>(name, context, keySerde, scheduledSerde);

            context.register(this, true, (k, v) -> {
                KeyValue<K, Scheduled<K, V>> keyValue = stateLogger.readChange(k, v);
                scheduleImpl(keyValue.value);
                log.debug("Scheduled task {} restored for key {}", keyValue.value, keyValue.key);
            });
        }

        this.open = true;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        this.keyToScheduledMap.clear();
        this.executor.shutdownNow();
        this.open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    public int size(){
        return keyToScheduledMap.size();
    }

    public interface ScheduledTaskTransformer<K, V>{
        Runnable buildTask(Scheduled<K, V> scheduled);
    }
}
