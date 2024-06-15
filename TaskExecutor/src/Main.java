import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        <T> Future<T> submitTask(Task<T> task);
    }

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

    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("Group UUID must not be null");
            }
        }
    }

    public static class TaskExecutorImpl implements TaskExecutor {

        private final ExecutorService executorService;

        private final ConcurrentHashMap<UUID, Object> groupLocks = new ConcurrentHashMap<>();

        public TaskExecutorImpl(int maxPoolSize) {
            executorService = Executors.newFixedThreadPool(maxPoolSize);
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            Object groupLock = groupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new Object());
            synchronized (groupLock) {
                return executorService.submit(task.taskAction());
            }
        }

        public void shutdown() {
            executorService.shutdown();
        }
    }

    public static void main(String[] args) {

        int maxPoolSize = 10;

        TaskExecutor executor = new TaskExecutorImpl(maxPoolSize);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        Task<String> task1 = new Task<>(
                UUID.randomUUID(),
                group1,
                TaskType.READ,
                () -> {
                    return "Task-1";
                }
        );

        Task<String> task2 = new Task<>(
                UUID.randomUUID(),
                group1,
                TaskType.WRITE,
                () -> {
                    return "Task-2";
                }
        );

        Task<String> task3 = new Task<>(
                UUID.randomUUID(),
                group2,
                TaskType.READ,
                () -> {
                    return "Task-3";
                }
        );

        Future<String> future1 = executor.submitTask(task1);
        Future<String> future2 = executor.submitTask(task2);
        Future<String> future3 = executor.submitTask(task3);

        try {
            System.out.println("First : "+ future1.get());
            System.out.println("Second : "+ future2.get());
            System.out.println("Third : "+ future3.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            ((TaskExecutorImpl) executor).shutdown();
        }
    }
}

