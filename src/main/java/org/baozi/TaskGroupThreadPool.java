package org.baozi;

import org.baozi.queue.BlockThreadTaskQueue;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class TaskGroupThreadPool {
    private final Worker[] coreWorkers;
    private final Worker[] additionalWorkers;
    private final int maxThreadSize;
    private final int coreThreadSize;
    private final ThreadFactory threadFactory;
    private final Duration keepAliveTime;
    private final BlockThreadTaskQueue taskQueue;

    private final ReentrantLock mainLock = new ReentrantLock();
    private final Condition mainCondition = mainLock.newCondition();

    private volatile int status;

    private final int WAITING = 1;
    private final int RUNNING = 1 << 1;
    private final int STOPED = 1 << 2;

    class Worker implements Runnable {
        private final Thread thread;
        private final BlockThreadTaskQueue.Task task;
        private volatile int status;

        public Worker(BlockThreadTaskQueue.Task task) {
            this.task = task;
            this.thread = getThreadFactory().newThread(this);
            this.status = RUNNING;
        }

        public void runTask(BlockThreadTaskQueue.Task task) {
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
                BlockThreadTaskQueue.Task task = taskQueue.poll(true);
                if (task == null) break;
                status = RUNNING;
                runTask(task);
            }
            status = STOPED;
            mainCondition.signal();
        }

        public int getStatus() {
            return status;
        }
    }

    public TaskGroupThreadPool(int coreThreadSize, int maxThreadSize, Duration keepAliveTime, ThreadFactory threadFactory, BlockThreadTaskQueue taskQueue) {
        this.coreWorkers = new Worker[coreThreadSize];
        this.additionalWorkers = new Worker[maxThreadSize - coreThreadSize];
        this.coreThreadSize = coreThreadSize;
        this.maxThreadSize = maxThreadSize;
        this.keepAliveTime = keepAliveTime;
        this.threadFactory = threadFactory;
        this.taskQueue = taskQueue;
        this.status = RUNNING;
    }

    public void exec(BlockThreadTaskQueue.Task task) {
        if (status == WAITING) return;

        // 任务丢到队列里
        taskQueue.push(task);

        task = taskQueue.poll(false);
        if (task != null) {
            System.out.println("增加工人");
            // 核心线程有没有空余地方
            Worker idleWorker = findIdleWorker(task, true);
            if (workerRun(idleWorker)) return;

            // 附加线程有没有空余地方
            idleWorker = findIdleWorker(task, false);
            if (workerRun(idleWorker)) return;
        }

        System.out.println("没地方了");


    }

    private Worker findIdleWorker(BlockThreadTaskQueue.Task task, boolean inCore) {
        Worker idleWorker = null;
        Worker[] workers = coreWorkers;
        int workerPoolSize = coreThreadSize;
        if (!inCore) {
            workers = additionalWorkers;
            workerPoolSize = maxThreadSize - coreThreadSize;
        }
        for (int i = 0; i < workerPoolSize; i++) {
            Worker worker = workers[i];
            if (worker == null) {
                worker = new Worker(task);
                workers[i] = worker;
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

    private boolean workerRun(Worker worker) {
        if (worker != null) {
            // 新建了一个工作者，启动
            worker.thread.start();
            return true;
        }
        return false;
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
