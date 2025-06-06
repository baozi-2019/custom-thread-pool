package org.baozi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.baozi.queue.TaskQueue;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class TestThreadPool {
    public static void main(String[] args) throws InterruptedException {
//        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 3, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
//        threadPoolExecutor.execute(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("1111111111111111");
//            }
//        });
//
//
//    }
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("TestThreadPool-%d").build();
        TaskGroupThreadPool taskGroupThreadPool = new TaskGroupThreadPool(2, 5, Duration.ofMinutes(5), threadFactory, new TaskQueue());

        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                taskGroupThreadPool.exec(new TaskQueue.Task() {
                    @Override
                    public String calcGroup() {
                        return UUID.randomUUID().toString();
                    }

                    @Override
                    public void run() {
                        try {
                            TimeUnit.MILLISECONDS.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println("Thread name: " + Thread.currentThread().getName() + " " + calcGroup());
                    }
                });
            }
        });
        thread.start();

        Thread.sleep(5000);
        taskGroupThreadPool.shutdown();
        if (!taskGroupThreadPool.awaitTermination(Duration.ofSeconds(5))) {
            taskGroupThreadPool.shutdownNow();
        }
        thread.interrupt();
    }
}
