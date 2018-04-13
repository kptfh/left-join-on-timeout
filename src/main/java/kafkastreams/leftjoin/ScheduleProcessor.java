package kafkastreams.leftjoin;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ScheduleProcessor<K, V> extends AbstractProcessor<K, V>{

    private final String scheduledStateStoreName;
    private ScheduledStateStore<K, V> scheduledStateStore;

    public ScheduleProcessor(String scheduledStateStoreName) {
        this.scheduledStateStoreName = scheduledStateStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        scheduledStateStore = (ScheduledStateStore<K, V>)context.getStateStore(scheduledStateStoreName);
    }

    @Override
    public void process(K key, V value) {
        scheduledStateStore.schedule(key, value, context());
    }
}
