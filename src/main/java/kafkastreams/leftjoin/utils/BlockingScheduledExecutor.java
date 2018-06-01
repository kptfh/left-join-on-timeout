package kafkastreams.leftjoin.utils;


import java.util.concurrent.*;

public class BlockingScheduledExecutor {

    private final ScheduledThreadPoolExecutor executor;
    private final Semaphore semaphore;
    private final ConcurrentHashMap<ScheduledIdentity, ScheduledFuture<?>> keyToScheduledMap;

    public BlockingScheduledExecutor(ScheduledThreadPoolExecutor executor, int maxTasksInQueue) {
        this.executor = executor;
        this.executor.setRemoveOnCancelPolicy(false);
        this.semaphore = new Semaphore(maxTasksInQueue);
        this.keyToScheduledMap = new ConcurrentHashMap<>(maxTasksInQueue);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        ScheduledIdentity scheduledIdentity = new ScheduledIdentity(command, delay, unit);
        ScheduledFuture<?> scheduled = keyToScheduledMap.computeIfAbsent(scheduledIdentity,
                identity -> executor.schedule(executeAndRelease(identity), delay, unit));
        return new BlockingScheduledFuture<>(scheduledIdentity, scheduled);
    }

    private Runnable executeAndRelease(ScheduledIdentity scheduledIdentity) {
        return () -> {
            try {
                scheduledIdentity.command.run();
            } finally {
                if(keyToScheduledMap.remove(scheduledIdentity) != null){
                    semaphore.release();
                }
            }
        };
    }

    private boolean cancel(ScheduledIdentity scheduledIdentity, boolean mayInterruptIfRunning) {
        ScheduledFuture<?> scheduled = keyToScheduledMap.remove(scheduledIdentity);
        if(scheduled == null) {
            return false;
        }
        if(scheduled.cancel(mayInterruptIfRunning)){
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

    private static class ScheduledIdentity {
        private final Runnable command;
        private final long delay;
        private final TimeUnit unit;

        private ScheduledIdentity(Runnable command, long delay, TimeUnit unit) {
            this.command = command;
            this.delay = delay;
            this.unit = unit;
        }

    }

    private class BlockingScheduledFuture<V> implements ScheduledFuture<V> {

        private final ScheduledIdentity scheduledIdentity;
        private final ScheduledFuture<V> scheduledFuture;

        private BlockingScheduledFuture(ScheduledIdentity scheduledIdentity, ScheduledFuture<V> scheduledFuture){
            this.scheduledIdentity = scheduledIdentity;
            this.scheduledFuture = scheduledFuture;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return scheduledFuture.getDelay(unit);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return BlockingScheduledExecutor.this.cancel(scheduledIdentity, mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return scheduledFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return scheduledFuture.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return scheduledFuture.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return scheduledFuture.get(timeout, unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return scheduledFuture.compareTo(o);
        }
    }
}
