package org.apache.cassandra.utils;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;
import java.util.Date;

// fulfils the Condition interface without spurious wakeup problems
// (or lost notify problems either: that is, even if you call await()
// _after_ signal(), it will work as desired.)
public class SimpleCondition implements Condition
{
    volatile boolean set;

    public synchronized void await() throws InterruptedException
    {
        while (!set)
            wait();
    }

    public synchronized void signal()
    {
        set = true;
        notify();
    }

    public synchronized void signalAll()
    {
        set = true;
        notifyAll();
    }

    public void awaitUninterruptibly()
    {
        throw new UnsupportedOperationException();
    }

    public long awaitNanos(long nanosTimeout) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public boolean awaitUntil(Date deadline) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }
}
