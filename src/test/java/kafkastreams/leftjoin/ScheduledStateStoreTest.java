package kafkastreams.leftjoin;

import com.fasterxml.jackson.core.type.TypeReference;
import kafkastreams.leftjoin.utils.JsonGenericDeserializer;
import kafkastreams.leftjoin.utils.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.State.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ScheduledStateStoreTest {

    private ConcurrentLinkedQueue<Long> completedTasks = new ConcurrentLinkedQueue<>();

    private ScheduledStateStore<Long, Object> scheduledStateStore = new ScheduledStateStore<Long, Object>("name",
            scheduled -> () -> completedTasks.add(scheduled.key),
            200, 2)
            .enableStateLog(Serdes.Long(),
                    Serdes.serdeFrom(new JsonSerializer<>(),
                            new JsonGenericDeserializer<>(
                                    new TypeReference<Scheduled<Long, Object>>() {
                                    }
                            ))
                    );

    @Mock
    private RecordCollector recordCollector;

    @Mock
    private ProcessorContextImpl context;

    @Before
    public void setupMocks(){
        Mockito.when(context.timestamp()).thenReturn(-1L);
        Mockito.when(context.taskId()).thenReturn(new TaskId(1, 2));
        Mockito.when(context.recordCollector()).thenReturn(recordCollector);
        scheduledStateStore.init(context, null);
    }

    @After
    public void after(){
        scheduledStateStore.close();
    }

    @Test
    public void shouldCancel(){
        schedule(1L);
        schedule(2L);
        cancel(1L);

        assertThat(scheduledStateStore.size()).isEqualTo(1);

        await(2L);
    }

    @Test
    public void shouldNotOverflow(){
        schedule(1L);
        schedule(2L);
        assertThat(scheduledStateStore.size()).isEqualTo(2);

        Thread t = new Thread(() -> schedule(3L));
        t.start();

        Awaitility.await()
                .atMost(2, SECONDS)
                .pollInterval(new Duration(10, MILLISECONDS))
                .untilAsserted(() -> {
                            assertThat(t.getState()).isIn(BLOCKED, WAITING);
                            assertThat(scheduledStateStore.size()).isEqualTo(2);
                        }
                );

        Awaitility.await()
                .atMost(2, SECONDS)
                .pollInterval(new Duration(20, MILLISECONDS))
                .untilAsserted(() -> {
                            assertThat(t.getState()).isIn(RUNNABLE, TERMINATED);
                            assertThat(scheduledStateStore.size()).isLessThanOrEqualTo(2);
                        }
                );

        await(1L, 2L, 3L);
    }

    private void schedule(Long key) {
        scheduledStateStore.schedule(key, null, context);
    }

    private void cancel(Long key) {
        scheduledStateStore.cancel(key);
    }

    private void await(Long... keys){
        Awaitility.await()
                .atMost(2, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(completedTasks).containsExactly(keys));
    }
}
