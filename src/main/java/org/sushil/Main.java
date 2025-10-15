package org.sushil;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Main {


    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {

        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result
         *
         */
        <T> Future<T> submitTask(Task<T> task);


        /**
         * Initiates an orderly shutdown in which previously submitted tasks are executed,
         * but no new tasks will be accepted.
         */
        void shutdown();


        /**
         * Attempts to stop all actively executing tasks and halts the processing of waiting tasks.
         */
        void shutdownNow();
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID   Unique task identifier.
     * @param taskGroup  Task group.
     * @param taskType   Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T>        Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //TODO change to LOGGER
        System.out.println("Task Executor Service Demo");
        System.out.println("===========================\n");

        TaskExecutor executor = new TaskExecutorImpl(4);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        System.out.println("Submitting tasks...\n");

        Future<String> future1 = executor.submitTask(new Task<>(
                UUID.randomUUID(),
                group1,
                TaskType.READ,
                () -> {
                    System.out.println("Task 1 (Group 1) started");
                    Thread.sleep(1000);
                    System.out.println("Task 1 (Group 1) completed");
                    return "Result 1";
                }
        ));

        Future<String> future2 = executor.submitTask(new Task<>(
                UUID.randomUUID(),
                group2,
                TaskType.WRITE,
                () -> {
                    System.out.println("Task 2 (Group 2) started");
                    Thread.sleep(500);
                    System.out.println("Task 2 (Group 2) completed");
                    return "Result 2";
                }
        ));

        Future<String> future3 = executor.submitTask(new Task<>(
                UUID.randomUUID(),
                group1,
                TaskType.WRITE,
                () -> {
                    System.out.println("Task 3 (Group 1) started - should wait for Task 1");
                    Thread.sleep(500);
                    System.out.println("Task 3 (Group 1) completed");
                    return "Result 3";
                }
        ));

        Future<Integer> future4 = executor.submitTask(new Task<>(
                UUID.randomUUID(),
                group2,
                TaskType.READ,
                () -> {
                    System.out.println("Task 4 (Group 2) started - should wait for Task 2");
                    Thread.sleep(300);
                    System.out.println("Task 4 (Group 2) completed");
                    return 42;
                }
        ));


        System.out.println("\nRetrieving results...\n");
        System.out.println("Future 1 result: " + future1.get());
        System.out.println("Future 2 result: " + future2.get());
        System.out.println("Future 3 result: " + future3.get());
        System.out.println("Future 4 result: " + future4.get());


        System.out.println("\nShutting down executor...");
        executor.shutdown();
        System.out.println("Done!");
    }
}
