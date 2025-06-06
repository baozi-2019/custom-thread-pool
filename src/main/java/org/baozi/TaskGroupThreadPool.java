package org.baozi;

import org.baozi.queue.TaskQueue;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class TaskGroupThreadPool {
    private final Worker[] coreWorkers;
    private final Worker[] additionalWorkers;
    private final int maxThreadSize;
    private final int coreThreadSize;
    private final ThreadFactory threadFactory;
    private final Duration keepAliveTime;
    private final TaskQueue taskQueue;

//    private final ReentrantLock pollLock = new ReentrantLock();
//    private final Condition pollCondition = pollLock.newCondition();

    private final ReentrantLock mainLock = new ReentrantLock();
    private final Condition mainCondition = mainLock.newCondition();

    private volatile int status;

    private final int WAITING = 1;
    private final int RUNNING = 1 << 1;
    private final int STOPED = 1 << 2;

    class Worker implements Runnable {
        private final Thread thread;
        private final TaskQueue.Task task;
        private volatile int status;
        private final boolean isCore;

        public Worker(TaskQueue.Task task, boolean isCore) {
            this.task = task;
            this.thread = getThreadFactory().newThread(this);
            this.status = RUNNING;
            this.isCore = isCore;

            this.thread.start();
        }

        public void runTask(TaskQueue.Task task) {
            task.run();
            taskQueue.finishTask(task);
        }

        @Override
        public void run() {
            if (task != null) {
                runTask(task);
            }
            while (true) {
                status = WAITING;
                TaskQueue.Task task;
                try {
                    task = getTask(isCore);
                } catch (InterruptedException e) {
                    task = null;
                }
                if (task == null) break;
                status = RUNNING;
                runTask(task);
            }
            mainLock.lock();
            try {
                status = STOPED;
                mainCondition.signal();
            } finally {
                mainLock.unlock();
            }
        }

        public int getStatus() {
            return status;
        }
    }

    private TaskQueue.Task getTask(boolean isCore) throws InterruptedException {
        TaskQueue.Task task = taskQueue.poll();
        if (task != null) return task;
        if (isCore) {
            LockSupport.park();
        } else {
            LockSupport.parkNanos(keepAliveTime.toNanos());
        }
        return taskQueue.poll();
    }

    public TaskGroupThreadPool(int coreThreadSize, int maxThreadSize, Duration keepAliveTime, ThreadFactory threadFactory, TaskQueue taskQueue) {
        this.coreWorkers = new Worker[coreThreadSize];
        this.additionalWorkers = new Worker[maxThreadSize - coreThreadSize];
        this.coreThreadSize = coreThreadSize;
        this.maxThreadSize = maxThreadSize;
        this.keepAliveTime = keepAliveTime;
        this.threadFactory = threadFactory;
        this.taskQueue = taskQueue;
        this.status = RUNNING;
    }

    public void exec(TaskQueue.Task task) {
        if (status == WAITING) return;
        // 判断能不能直接计算
        task = taskQueue.tryPoll(task);
        if (task != null) {
            // 核心线程有没有空余地方
            Worker idleWorker = findIdleWorker(task, true);
            if (idleWorker != null) return;

            // 附加线程有没有空余地方
            idleWorker = findIdleWorker(task, false);
            if (idleWorker != null) return;

            System.out.println("没地方了");
            taskQueue.push(task);
        }


    }

    private Worker findIdleWorker(TaskQueue.Task task, boolean inCore) {
        Worker idleWorker = null;
        Worker[] workers = coreWorkers;
        int workerPoolSize = coreThreadSize;
        if (!inCore) {
            workers = additionalWorkers;
            workerPoolSize = maxThreadSize - coreThreadSize;
        }
        for (int i = 0; i < workerPoolSize; i++) {
            Worker worker = workers[i];
            if (worker == null || worker.getStatus() == STOPED) {
                System.out.println("增加工人");
                worker = new Worker(task, inCore);
                workers[i] = worker;
                idleWorker = worker;
                break;
            }
            if (worker.getStatus() == WAITING) {
                System.out.println("通知工人工作");
                taskQueue.push(task);
                LockSupport.unpark(worker.thread);
                idleWorker = worker;
                break;
            }
        }
        return idleWorker;
    }

    private void foreachWorkers(Worker[] workers, Consumer<Worker> consumer) {
        for (Worker worker : workers) {
            if (worker == null) continue;
            consumer.accept(worker);
        }
    }

    public void shutdown() {
        mainLock.lock();
        try {
            status = WAITING;
        } finally {
            mainLock.unlock();
        }
        tryTerminate();
    }

    public boolean awaitTermination(Duration timeout) {
        long timeoutNanos = timeout.toNanos();
        mainLock.lock();
        try {
            for (; ; ) {
                // 循环等待，直到超时
                timeoutNanos = mainCondition.awaitNanos(timeoutNanos);
                // 检查线程是否都关闭
                AtomicBoolean isAllThreadCloseAtomic = new AtomicBoolean(true);
                foreachWorkers(additionalWorkers, worker -> {
                    boolean isAllThreadClose = isAllThreadCloseAtomic.get();
                    isAllThreadClose &= worker.getStatus() == STOPED;
                    isAllThreadCloseAtomic.set(isAllThreadClose);
                });
                foreachWorkers(coreWorkers, worker -> {
                    boolean isAllThreadClose = isAllThreadCloseAtomic.get();
                    isAllThreadClose &= worker.getStatus() == STOPED;
                    isAllThreadCloseAtomic.set(isAllThreadClose);
                });
                if (isAllThreadCloseAtomic.get()) {
                    return true;
                } else if (timeoutNanos <= 0L) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            System.out.println("线程中断");
        } finally {
            mainLock.unlock();
        }
        return false;
    }

    public void shutdownNow() {
        mainLock.lock();
        try {
            status = STOPED;
        } finally {
            mainLock.unlock();
        }
        tryTerminate();
    }

    private void tryTerminate() {
        // 所有等待中的线程，停止运行
        foreachWorkers(coreWorkers, worker -> {
            if (worker.getStatus() == WAITING) worker.thread.interrupt();
        });
        foreachWorkers(additionalWorkers, worker -> {
            if (worker.getStatus() == WAITING) worker.thread.interrupt();
        });
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }
}
