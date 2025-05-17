package org.baozi.queue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BlockThreadTaskQueue {
    private final LinkedList<Task> taskList = new LinkedList<>();
    private final ReentrantLock takeLock = new ReentrantLock(true);
    private final Condition notEmpty = takeLock.newCondition();
    private final LinkedList<String> calcGroupList = new LinkedList<>();

    public interface Task extends Runnable {
        String calcGroup();
    }

    public void push(Task task) {
        takeLock.lock();
        try {
            taskList.add(task);
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    public void finishTask(Task task) {
        takeLock.lock();
        try {
            calcGroupList.remove(task.calcGroup());
        } finally {
            takeLock.unlock();
        }
    }

    public Task poll(boolean block) {
        takeLock.lock();
        try {
            do {
                Iterator<Task> iterator = taskList.iterator();
                while (iterator.hasNext()) {
                    Task task = iterator.next();
                    String calcGroup = task.calcGroup();
                    if (calcGroupList.contains(calcGroup)) continue;
                    // 找到没有冲突的任务，弹出
                    iterator.remove();
                    calcGroupList.add(calcGroup);
                    return task;
                }
                if (block) notEmpty.await();
            } while (block);
        } catch (InterruptedException e) {
            System.out.println(e);
        } finally {
            takeLock.unlock();
        }
        return null;
    }
}
