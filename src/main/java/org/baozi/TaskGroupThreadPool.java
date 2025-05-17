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
            this.thread = getThreadFactory().newThread(this);
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
                BlockThreadTaskQueue.Task task = taskQueue.poll(true);
                if (task == null) break;
                runTask(task);
            }
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

    private boolean workerRun(Worker worker) {
        if (worker != null) {
            // 新建了一个工作者，启动
            worker.thread.start();
            return true;
        }
        return false;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }
}
