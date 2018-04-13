package kafkastreams.leftjoin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduledSerdeTest {

    private static final String SCHEDULED_FUTURE_FIELD_NAME = "scheduledFuture";
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldCorrectlySerde() throws IOException {
        Serde<Scheduled<Long, String>> scheduledSerde = ScheduledStateStoreSupplier.scheduledSerde(Long.class, String.class);

        Scheduled<Long, String> scheduled = new Scheduled<>(1L, "test value", 100);
        scheduled.setScheduledFuture(new TestScheduledFuture());

        byte[] data = scheduledSerde.serializer().serialize(null, scheduled);

        JsonNode jsonNode = objectMapper.readTree(data);
        assertThat(jsonNode.get(SCHEDULED_FUTURE_FIELD_NAME)).isNull();

        Scheduled<Long, String> scheduledDeserialized = scheduledSerde.deserializer().deserialize(null, data);
        assertThat(scheduled).isEqualToIgnoringGivenFields(scheduledDeserialized,
                SCHEDULED_FUTURE_FIELD_NAME);
        assertThat((Object)scheduledDeserialized.getScheduledFuture()).isNull();
    }

    private static class TestScheduledFuture<V> implements ScheduledFuture<V> {

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }

}
