package org.baozi;

import org.baozi.queue.BlockThreadTaskQueue;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;

public class TaskGroupThreadPool {
    private final Worker[] coreWorkers;
    private final Worker[] additionalWorkers;
    private final int maxThreadSize;
    private final int coreThreadSize;
    private final ThreadFactory threadFactory;
    private final Duration keepAliveTime;
    private final BlockThreadTaskQueue taskQueue;

    class Worker implements Runnable {
        private final Thread thread;
        private final BlockThreadTaskQueue.Task task;

        public Worker(BlockThreadTaskQueue.Task task) {
            this.task = task;
            this.thread = getThreadFactory().newThread(task);
        }

        public boolean isWaiting() {
            return this.thread.getState() == Thread.State.WAITING;
        }

        @Override
        public void run() {

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
    }

    public void exec(BlockThreadTaskQueue.Task task) {
        // 从核心线程找worker
        Worker idleWorker = findIdleWorker(task, true);
        if (workerRun(idleWorker)) return;

        // 从附加线程找worker
        idleWorker = findIdleWorker(task, false);
        if (workerRun(idleWorker)) return;

        // 还是找不到，丢队列里
        taskQueue.push(task);
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
            if (worker.isWaiting()) {
                idleWorker = worker;
                break;
            }
        }
        return idleWorker;
    }

    private boolean workerRun(Worker worker) {
        if (worker != null) {
            // 找到没活干的了，让它执行，并返回
            worker.thread.start();
            return true;
        }
        return false;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }
}
