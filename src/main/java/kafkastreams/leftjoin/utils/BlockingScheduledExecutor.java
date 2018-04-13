package kafkastreams.leftjoin.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BlockingScheduledExecutor {

    private final ScheduledThreadPoolExecutor executor;
    private final Semaphore semaphore;

    public BlockingScheduledExecutor(ScheduledThreadPoolExecutor executor, int maxTasksInQueue) {
        this.executor = executor;
        this.executor.setRemoveOnCancelPolicy(false);
        semaphore = new Semaphore(maxTasksInQueue);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return executor.schedule(() -> {
            try {
                command.run();
            } finally {
                if(!Thread.currentThread().isInterrupted()) {
                    try {
                        semaphore.release();
                    } catch (Error e) {
                        log.warn("Excessive semaphore release", e);
                    }
                }
            }

        }, delay, unit);
    }

    public boolean cancel(ScheduledFuture<?> scheduledFuture) {
        if(scheduledFuture.cancel(true)){
            try {
                semaphore.release();
            } catch (Error e) {
                log.warn("Excessive semaphore release", e);
            }
            return true;
        } else {
            return false;
        }
    }

    public void shutdownNow(){
        executor.shutdownNow();
    }
}
