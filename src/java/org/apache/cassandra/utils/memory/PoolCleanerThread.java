package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * A thread that reclaims memor from a Pool on demand.  The actual reclaiming work is delegated to the
 * cleaner Runnable, e.g., FlushLargestColumnFamily
 */
class PoolCleanerThread<P extends Pool> extends Thread
{
    /** The pool we're cleaning */
    final P pool;

    /** should ensure that at least some memory has been marked reclaiming after completion */
    final Runnable cleaner;

    /** signalled whenever needsCleaning() may return true */
    final WaitQueue wait = new WaitQueue();

    PoolCleanerThread(P pool, Runnable cleaner)
    {
        super(pool.getClass().getSimpleName() + "Cleaner");
        this.pool = pool;
        this.cleaner = cleaner;
    }

    boolean needsCleaning()
    {
        return pool.needsCleaning();
    }

    // should ONLY be called when we really think it already needs cleaning
    void trigger()
    {
        wait.signal();
    }

    @Override
    public void run()
    {
        while (true)
        {
            while (!needsCleaning())
            {
                final WaitQueue.Signal signal = wait.register();
                if (!needsCleaning())
                    signal.awaitUninterruptibly();
                else
                    signal.cancel();
            }

            cleaner.run();
        }
    }
}
