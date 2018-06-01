package kafkastreams.leftjoin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.ScheduledFuture;

public class Scheduled<K, V> {
    public final K key;
    public final V value;
    public final long timestamp;

    @JsonIgnore
    private transient ScheduledFuture scheduledFuture;

    @JsonCreator
    public Scheduled(@JsonProperty("key") K key,
                     @JsonProperty("value") V value,
                     @JsonProperty("timestamp") long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public void setScheduledFuture(ScheduledFuture scheduledFuture) {
        if(this.scheduledFuture != null){
            throw new IllegalStateException("Already initialized");
        }
        this.scheduledFuture = scheduledFuture;
    }

    public ScheduledFuture getScheduledFuture() {
        return scheduledFuture;
    }
}