package kafkastreams.leftjoin.utils;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.State.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class BlockingScheduledExecutorTest {

    private static final int MAX_TASKS_IN_QUEUE = 2;
    private BlockingScheduledExecutor blockingScheduledExecutor = new BlockingScheduledExecutor(
            new ScheduledThreadPoolExecutor(1), MAX_TASKS_IN_QUEUE
    );

    @Test
    public void shouldSchedule(){
        LongAdder sum = new LongAdder();

        IntStream.range(0, MAX_TASKS_IN_QUEUE * 5)
                .mapToObj(value -> scheduleWithShortDelay(value, sum))
                .collect(Collectors.toList());

        Awaitility.await()
                .atMost(10, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(sum.longValue()).isEqualTo(MAX_TASKS_IN_QUEUE * 5));
    }

    @Test
    public void shouldNotCancelComplete(){
        LongAdder sum = new LongAdder();

        IntStream.range(0, MAX_TASKS_IN_QUEUE * 5)
                .mapToObj(value -> scheduleWithShortDelay(value, sum))
                .collect(Collectors.toList());

        Awaitility.await()
                .atMost(10, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(sum.longValue()).isEqualTo(MAX_TASKS_IN_QUEUE * 5));

        assertThat(blockingScheduledExecutor.cancel(0)).isFalse();
    }

    @Test
    public void shouldCancel(){
        LongAdder sum = new LongAdder();

        IntStream.range(0, MAX_TASKS_IN_QUEUE)
                .mapToObj(value -> scheduleWithLongDelay(value, sum))
                .collect(Collectors.toList());

        blockingScheduledExecutor.cancel(0);

        scheduleWithLongDelay(MAX_TASKS_IN_QUEUE + 1, sum);

        assertThat(sum.longValue()).isEqualTo(0);
    }

    @Test
    public void shouldNotCancelCompleted(){
        LongAdder sum = new LongAdder();

        ScheduledFuture scheduledFutures = IntStream.range(0, 1)
                .mapToObj(value -> scheduleWithShortDelay(value, sum))
                .collect(Collectors.toList()).get(0);

        Awaitility.await()
                .atMost(10, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(sum.longValue()).isEqualTo(1));

        assertThat(blockingScheduledExecutor.cancel(scheduledFutures)).isFalse();
    }

    @Test
    public void shouldBlockOnCapacityAndUnblockByCancel(){
        LongAdder sum = new LongAdder();
        IntStream.range(0, MAX_TASKS_IN_QUEUE)
                .mapToObj(value -> scheduleWithLongDelay(value, sum))
                .collect(Collectors.toList());

        Thread t = new Thread(() -> scheduleWithLongDelay(MAX_TASKS_IN_QUEUE + 1, sum));
        t.start();

        Awaitility.await()
                .atMost(10, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(t.getState()).isIn(BLOCKED, WAITING));

        assertThat(sum.longValue()).isEqualTo(0);


        blockingScheduledExecutor.cancel(0);

        Awaitility.await()
                .atMost(10, SECONDS)
                .pollInterval(Duration.ONE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(t.getState()).isIn(RUNNABLE, TERMINATED));

        assertThat(sum.longValue()).isEqualTo(0);
    }

    @Test
    public void shouldScheduleAndCancel(){
        LongAdder sum = new LongAdder();

        int maxTasksInQueue = 100;
        BlockingScheduledExecutor blockingScheduledExecutor = new BlockingScheduledExecutor(
                new ScheduledThreadPoolExecutor(1), maxTasksInQueue
        );

        Random random = new Random();
        IntStream.range(0, maxTasksInQueue * 2)
                .mapToObj(value -> blockingScheduledExecutor.schedule(value, sum::increment, random.nextInt(100), TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());

        IntStream.range(0, maxTasksInQueue * 2)
                .mapToObj(value -> blockingScheduledExecutor.cancel(value))
                .collect(Collectors.toList());

        assertThat(blockingScheduledExecutor.size()).isEqualTo(0);
        assertThat(blockingScheduledExecutor.semaphoreQueueLength()).isEqualTo(0);
    }

    private ScheduledFuture scheduleWithShortDelay(int key, LongAdder sum) {
        return blockingScheduledExecutor.schedule(key, sum::increment, 1, TimeUnit.MILLISECONDS);
    }

    private ScheduledFuture scheduleWithLongDelay(int key, LongAdder sum) {
        return blockingScheduledExecutor.schedule(key, sum::increment, 100, TimeUnit.SECONDS);
    }
}
