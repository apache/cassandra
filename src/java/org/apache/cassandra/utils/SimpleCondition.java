package org.apache.cassandra.utils;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;
import java.util.Date;

// fulfils the Condition interface without spurious wakeup problems
// (or lost notify problems either: that is, even if you call await()
// _after_ signal(), it will work as desired.)
public class SimpleCondition implements Condition
{
    boolean set;

    public synchronized void await() throws InterruptedException
    {
        while (!set)
            wait();
    }

    public synchronized boolean await(long time, TimeUnit unit) throws InterruptedException
    {
        // micro/nanoseconds not supported
        assert unit == TimeUnit.DAYS || unit == TimeUnit.HOURS || unit == TimeUnit.MINUTES || unit == TimeUnit.SECONDS || unit == TimeUnit.MILLISECONDS;

        long end = System.currentTimeMillis() + unit.convert(time, TimeUnit.MILLISECONDS);
        while (!set && end > System.currentTimeMillis())
        {
            TimeUnit.MILLISECONDS.timedWait(this, end - System.currentTimeMillis());
        }
        return set;
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

    public synchronized boolean isSignaled()
    {
        return set;
    }

    public void awaitUninterruptibly()
    {
        throw new UnsupportedOperationException();
    }

    public long awaitNanos(long nanosTimeout) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public boolean awaitUntil(Date deadline) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }
}
