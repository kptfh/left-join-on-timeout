package kafkastreams.leftjoin.utils;

import java.util.Map;
import java.util.concurrent.*;

public class BlockingScheduledExecutor<K> {

    private final ScheduledThreadPoolExecutor executor;
    private final Semaphore semaphore;
    private final Map<K, ScheduledFuture<?>> keyToScheduledMap;

    public BlockingScheduledExecutor(ScheduledThreadPoolExecutor executor, int maxTasksInQueue) {
        this.executor = executor;
        this.executor.setRemoveOnCancelPolicy(false);
        this.semaphore = new Semaphore(maxTasksInQueue);
        this.keyToScheduledMap = new ConcurrentHashMap<>(maxTasksInQueue);
    }

    public ScheduledFuture<?> schedule(K key, Runnable command, long delay, TimeUnit unit) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        CountDownLatch addedToMap = new CountDownLatch(1);
        ScheduledFuture<?> scheduled = executor.schedule(() -> {
            try {
                addedToMap.await();
                command.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                if(keyToScheduledMap.remove(key) != null){
                    semaphore.release();
                }
            }
        }, delay, unit);

        keyToScheduledMap.put(key, scheduled);
        addedToMap.countDown();

        return scheduled;
    }

    public boolean cancel(K key) {
        ScheduledFuture<?> scheduled = keyToScheduledMap.remove(key);
        if(scheduled == null) {
            return false;
        }
        if(scheduled.cancel(false)){
            semaphore.release();
            return true;
        } else {
            return false;
        }
    }

    public void shutdownNow(){
        executor.shutdownNow();
        keyToScheduledMap.clear();
    }

    public int size(){
        return keyToScheduledMap.size();
    }

    int semaphoreQueueLength(){
        return semaphore.getQueueLength();
    }
}
