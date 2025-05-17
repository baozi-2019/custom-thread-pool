package org.baozi;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Main {

    public static class Data {
        private final ReentrantLock takeLock = new ReentrantLock();
        private final Condition notEmpty = takeLock.newCondition();

        public void put(Thread thread) {
//            LockSupport.unpark(thread);
            takeLock.lock();
            notEmpty.signal();
            takeLock.unlock();
        }

        public String take() throws InterruptedException {
            takeLock.lock();
            notEmpty.await();
            takeLock.unlock();
            return "1111111111";
        }
    }

    public static class A implements Runnable {
        private final Data data;

        public A(Data data) {
            this.data = data;
        }

        @Override
        public void run() {
            // 拿数据
            String take = null;
            try {
                take = data.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(take);
        }

        public Data getData() {
            return data;
        }
    }

    public static class M {
        private Thread thread;
        private Data data;

        public void exec(A a) throws InterruptedException {
            data = a.getData();
            thread = new Thread(a);
            thread.start();
            TimeUnit.MILLISECONDS.sleep(500);
            System.out.println(thread.getState());
        }

        public void addData() {
            data.put(thread);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        M m = new M();
        Data data = new Data();
        m.exec(new A(data));

        TimeUnit.SECONDS.sleep(5);

        m.addData();
    }
}
