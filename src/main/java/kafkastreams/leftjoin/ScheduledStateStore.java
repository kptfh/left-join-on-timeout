package kafkastreams.leftjoin;

import kafkastreams.leftjoin.utils.BlockingScheduledExecutor;
import kafkastreams.leftjoin.utils.StateStoreLogger;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static kafkastreams.leftjoin.utils.MultiMapUtils.addToMultiMap;
import static kafkastreams.leftjoin.utils.MultiMapUtils.removeFromMultiMap;

public class ScheduledStateStore<K, V> implements StateStore {

    private static Logger log = LoggerFactory.getLogger(ScheduledStateStore.class);

    private final String name;
    private final long delayInMs;
    private final ScheduledTaskTransformer<K, V> scheduledTaskTransformer;
    private final BlockingScheduledExecutor executor;
    private final ConcurrentHashMap<K, List<Scheduled<K, V>>> keyToScheduled;
    private boolean open;

    private boolean stateLogEnabled = false;
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
        this.keyToScheduled = new ConcurrentHashMap<>(capacity);
    }

    public ScheduledStateStore<K, V> enableStateLog(Serde<K> keySerde, Serde<Scheduled<K, V>> scheduledSerde) {
        this.keySerde = keySerde;
        this.scheduledSerde = scheduledSerde;
        this.stateLogEnabled = true;
        return this;
    }

    public void schedule(K key, V value, ProcessorContext context) {
        Scheduled<K, V> scheduled = new Scheduled<>(key, value, context.timestamp());
        scheduleImpl(key, scheduled);

        if (stateLogEnabled) {
            stateLogger.logAdded(key, scheduled);
        }
        log.debug("Scheduled task {} for key {}", scheduled, key);
    }

    private void scheduleImpl(K key, Scheduled<K, V> scheduled) {
        addToMultiMap(keyToScheduled, key, key_ -> {
            Runnable command = scheduledTaskTransformer.buildTask(scheduled);
            scheduled.setScheduledFuture(executor.schedule(() -> {
                try {
                    command.run();
                }
                catch (Throwable t){
                    log.error("Error while running scheduled task", t);
                }
                finally {
                    removeFromMultiMap(keyToScheduled, key_, scheduled);
                }
            }, delayInMs, TimeUnit.MILLISECONDS));
            return scheduled;
        });
    }

    public void cancel(K key) {
        keyToScheduled.compute(key, (key_, scheduleds) -> {

            if (scheduleds == null) {
                log.warn("No scheduled tasks for key: {}", key);
                return null;
            }

            scheduleds.forEach(scheduled -> {
                ScheduledFuture scheduledFuture = scheduled.getScheduledFuture();
                boolean cancelled = scheduledFuture.cancel(false);

                if (!cancelled) {
                    if (scheduledFuture.isCancelled()) {
                        log.warn("Task already cancelled for keyOffset: {}", key);
                    } else {
                        log.warn("Task already complete for keyOffset: {}", key);
                    }
                }
            });

            if (stateLogEnabled) {
                stateLogger.logRemoved(key);
            }

            log.debug("Cancelled scheduled task for key {}", key);
            return null;
        });
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        if (stateLogEnabled) {
            this.stateLogger = new StateStoreLogger<>(name, context, keySerde, scheduledSerde);

            context.register(this, true, (k, v) -> {
                KeyValue<K, Scheduled<K, V>> keyValue = stateLogger.readChange(k, v);
                scheduleImpl(keyValue.key, keyValue.value);
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

    public int size() {
        return executor.size();
    }

    public interface ScheduledTaskTransformer<K, V> {
        Runnable buildTask(Scheduled<K, V> scheduled);
    }
}
