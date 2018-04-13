package kafkastreams.leftjoin;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CancelProcessor<K, V> extends AbstractProcessor<K, V>{

    private final String scheduledStateStoreName;
    private ScheduledStateStore<K, ?> scheduledStateStore;

    public CancelProcessor(String scheduledStateStoreName) {
        this.scheduledStateStoreName = scheduledStateStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        scheduledStateStore = (ScheduledStateStore<K, ?>)context.getStateStore(scheduledStateStoreName);
    }

    @Override
    public void process(K key, V value) {
        scheduledStateStore.cancel(key);
    }
}
