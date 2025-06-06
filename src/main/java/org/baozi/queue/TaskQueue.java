package org.baozi.queue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class TaskQueue {
    private final LinkedList<Task> taskList = new LinkedList<>();
    private final ReentrantLock takeLock = new ReentrantLock(true);
    private final LinkedList<String> calcGroupList = new LinkedList<>();

    public interface Task extends Runnable {
        String calcGroup();
    }

    public void push(Task task) {
        takeLock.lock();
        try {
            taskList.add(task);
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

    public Task poll() {
        takeLock.lock();
        try {
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
        } finally {
            takeLock.unlock();
        }
        return null;
    }

    public Task tryPoll(Task task) {
        takeLock.lock();
        try {
            String calcGroup = task.calcGroup();
            if (calcGroupList.contains(calcGroup)) {
                // 不能被拉取，push进去，返回null
                push(task);
                return null;
            }
            return task;
        } finally {
            takeLock.unlock();
        }
    }
}
