package org.sushil;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of TaskExecutor service with the following features:
 * - Thread-safe concurrent task submission
 * - Asynchronous task execution with configurable concurrency
 * - Order preservation: tasks are executed in submission order
 * - Task group isolation: tasks in the same group don't run concurrently
 * - Uses ExecutorService for thread pool management
 * - Uses CompletableFuture for async result handling
 * - Uses ConcurrentHashMap for thread-safe group tracking
 * - Uses BlockingQueue for ordered task queuing
 * - Uses ReentrantLock for fine-grained concurrency control
 */
public class TaskExecutorImpl implements Main.TaskExecutor {

    private final ExecutorService executorService;

    private final BlockingQueue<TaskWrapper<?>> taskQueue;

    private final ConcurrentHashMap<UUID, Boolean> activeGroups;

    private final ReentrantLock dispatchLock;

    private final Thread dispatcherThread;

    private volatile boolean isShuttingDown;

    private final Set<CompletableFuture<?>> activeFutures;
    private final ReentrantLock futuresLock;

    /**
     * Creates a TaskExecutor with specified maximum concurrency.
     *
     * @param maxConcurrency Maximum number of tasks that can run concurrently
     */
    public TaskExecutorImpl(int maxConcurrency) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("Max concurrency must be positive");
        }

        this.executorService = Executors.newFixedThreadPool(maxConcurrency);

        this.taskQueue = new LinkedBlockingQueue<>();

        this.activeGroups = new ConcurrentHashMap<>();

        this.dispatchLock = new ReentrantLock();

        this.isShuttingDown = false;

        this.activeFutures = ConcurrentHashMap.newKeySet();
        this.futuresLock = new ReentrantLock();

        this.dispatcherThread = new Thread(this::dispatchTasks, "TaskDispatcher");
        this.dispatcherThread.setDaemon(true);
        this.dispatcherThread.start();
    }

    /**
     * Submit a task for execution. This method is non-blocking and thread-safe.
     *
     * @param task Task to be executed
     * @return Future representing the pending result
     */
    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        if (task == null) {
            throw new IllegalArgumentException("Task must not be null");
        }

        if (isShuttingDown) {
            throw new RejectedExecutionException("TaskExecutor is shutting down");
        }

        CompletableFuture<T> future = new CompletableFuture<>();

        TaskWrapper<T> wrapper = new TaskWrapper<>(task, future);

        futuresLock.lock();
        try {
            activeFutures.add(future);
        } finally {
            futuresLock.unlock();
        }

        try {
            taskQueue.put(wrapper);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(new RejectedExecutionException("Task submission interrupted", e));
        }

        return future;
    }

    /**
     * Dispatcher thread that continuously processes tasks from the queue.
     * Ensures task order and group isolation constraints are maintained.
     */
    private void dispatchTasks() {
        while (!isShuttingDown || !taskQueue.isEmpty()) {
            try {
                TaskWrapper<?> wrapper = taskQueue.poll(100, TimeUnit.MILLISECONDS);

                if (wrapper == null) {
                    continue;
                }

                UUID groupId = wrapper.task.taskGroup().groupUUID();

                // Busy-wait with small sleep until group is available
                // This ensures order preservation while respecting group constraints
                while (activeGroups.putIfAbsent(groupId, Boolean.TRUE) != null) {
                    Thread.sleep(10);
                }

                submitToExecutor(wrapper);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Submit task wrapper to the executor service for actual execution.
     */
    private <T> void submitToExecutor(TaskWrapper<T> wrapper) {

        CompletableFuture.supplyAsync(() -> {
                    try {
                        return wrapper.task.taskAction().call();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, executorService)
                .whenComplete((result, exception) -> {
                    UUID groupId = wrapper.task.taskGroup().groupUUID();
                    activeGroups.remove(groupId);

                    if (exception != null) {
                        wrapper.future.completeExceptionally(exception);
                    } else {
                        wrapper.future.complete(result);
                    }

                    futuresLock.lock();
                    try {
                        activeFutures.remove(wrapper.future);
                    } finally {
                        futuresLock.unlock();
                    }
                });
    }

    /**
     * Initiates an orderly shutdown.
     */
    @Override
    public void shutdown() {
        dispatchLock.lock();
        try {
            if (isShuttingDown) {
                return;
            }
            isShuttingDown = true;
        } finally {
            dispatchLock.unlock();
        }

        try {
            dispatcherThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Attempts to stop all actively executing tasks.
     */
    @Override
    public void shutdownNow() {
        dispatchLock.lock();
        try {
            isShuttingDown = true;
        } finally {
            dispatchLock.unlock();
        }

        dispatcherThread.interrupt();

        futuresLock.lock();
        try {
            for (CompletableFuture<?> future : activeFutures) {
                future.cancel(true);
            }
        } finally {
            futuresLock.unlock();
        }

        taskQueue.clear();
        executorService.shutdownNow();
    }

    /**
     * Internal wrapper class to pair a task with its future.
     */
    private static class TaskWrapper<T> {
        final Main.Task<T> task;
        final CompletableFuture<T> future;

        TaskWrapper(Main.Task<T> task, CompletableFuture<T> future) {
            this.task = task;
            this.future = future;
        }
    }
}
