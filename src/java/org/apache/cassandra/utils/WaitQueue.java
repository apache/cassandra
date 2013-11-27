package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;

/**
 * <p>A relatively easy to use utility for general purpose thread signalling.</p>
 * <p>Usage on a thread awaiting a state change using a WaitQueue q is:</p>
 * <pre>
 * {@code
 *      while (!conditionMet())
 *          WaitSignal s = q.register();
 *              if (!conditionMet())    // or, perhaps more correctly, !conditionChanged()
 *                  s.await();
 *              else
 *                  s.cancel();
 * }
 * </pre>
 * A signalling thread, AFTER changing the state, then calls q.signal() to wake up one, or q.signalAll()
 * to wake up all, waiting threads.
 *
 * <p>A few notes on utilisation:</p>
 * <p>1. A thread will only exit await() when it has been signalled, but this does
 * not guarantee the condition has not been altered since it was signalled,
 * and depending on your design it is likely the outer condition will need to be
 * checked in a loop, though this is not always the case.</p>
 * <p>2. Each signal is single use, so must be re-registered after each await(). This is true even if it times out.</p>
 * <p>3. If you choose not to wait on the signal (because the condition has been met before you waited on it)
 * you must cancel() the signal if the signalling thread uses signal() to awake waiters; otherwise signals will be
 * lost</p>
 * <p>4. Care must be taken when selecting conditionMet() to ensure we are waiting on the condition that actually
 * indicates progress is possible. In some complex cases it may be tempting to wait on a condition that is only indicative
 * of local progress, not progress on the task we are aiming to complete, and a race may leave us waiting for a condition
 * to be met that we no longer need.
 * <p>5. This scheme is not fair</p>
 * <p>6. Only the thread that calls register() may call await()</p>
 * <p>To understand intuitively how this class works, the idea is simply that a thread, once it considers itself
 * incapable of making progress, registers itself to be awoken once that condition changes. However, that condition
 * could have changed between checking and registering (in which case a thread updating the state would have been unable to signal it),
 * so before going to sleep on the signal, it checks the condition again, sleeping only if it hasn't changed.</p>
 */
// TODO : switch to a Lock Free queue
public final class WaitQueue
{
    public final class Signal
    {
        private final Thread thread = Thread.currentThread();
        volatile int signalled;

        private boolean isSignalled()
        {
            return signalled == 1;
        }

        public boolean isCancelled()
        {
            return signalled == -1;
        }

        private boolean signal()
        {
            if (signalledUpdater.compareAndSet(this, 0, 1))
            {
                LockSupport.unpark(thread);
                return true;
            }
            return false;
        }

        public void awaitUninterruptibly()
        {
            assert !isCancelled();
            if (thread != Thread.currentThread())
                throw new IllegalStateException();
            boolean interrupted = false;
            while (!isSignalled())
            {
                if (Thread.interrupted())
                    interrupted = true;
                LockSupport.park();
            }
            if (interrupted)
                thread.interrupt();
        }

        public void await() throws InterruptedException
        {
            assert !isCancelled();
            while (!isSignalled())
            {
                if (Thread.interrupted())
                {
                    checkAndClear();
                    throw new InterruptedException();
                }
                if (thread != Thread.currentThread())
                    throw new IllegalStateException();
                LockSupport.park();
            }
        }

        public long awaitNanos(long nanosTimeout) throws InterruptedException
        {
            assert signalled != -1;
            long start = System.nanoTime();
            while (!isSignalled())
            {
                if (Thread.interrupted())
                {
                    checkAndClear();
                    throw new InterruptedException();
                }
                LockSupport.parkNanos(nanosTimeout);
            }
            return nanosTimeout - (System.nanoTime() - start);
        }

        public boolean await(long time, TimeUnit unit) throws InterruptedException
        {
            // ignores nanos atm
            long until = System.currentTimeMillis() + unit.toMillis(time);
            if (until < 0)
                until = Long.MAX_VALUE;
            return awaitUntil(until);
        }

        public boolean awaitUntil(long until) throws InterruptedException
        {
            assert !isCancelled();
            while (until < System.currentTimeMillis() && !isSignalled())
            {
                if (Thread.interrupted())
                {
                    checkAndClear();
                    throw new InterruptedException();
                }
                LockSupport.parkUntil(until);
            }
            return checkAndClear();
        }

        private boolean checkAndClear()
        {
            if (isSignalled())
            {
                signalled = -1;
                return true;
            }
            else if (signalledUpdater.compareAndSet(this, 0, -1))
            {
                cleanUpCancelled();
                return false;
            }
            else
            {
                // must now be signalled, as checkAndClear() can only be called by
                // owning thread if used correctly
                signalled = -1;
                return true;
            }
        }

        public void cancel()
        {
            if (signalled < 0)
                return;
            if (!signalledUpdater.compareAndSet(this, 0, -1))
            {
                signalled = -1;
                signal();
                cleanUpCancelled();
            }
        }

    }

    private static final AtomicIntegerFieldUpdater signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(Signal.class, "signalled");

    // the waiting signals
    private final ConcurrentLinkedQueue<Signal> queue = new ConcurrentLinkedQueue<>();

    /**
     * The calling thread MUST be the thread that uses the signal (for now)
     * @return
     */
    public Signal register()
    {
        Signal signal = new Signal();
        queue.add(signal);
        return signal;
    }

    /**
     * Signal one waiting thread
     */
    public void signal()
    {
        if (queue.isEmpty())
            return;
        Iterator<Signal> iter = queue.iterator();
        while (iter.hasNext())
        {
            Signal next = iter.next();
            if (next.signal())
            {
                iter.remove();
                return;
            }
        }
    }

    /**
     * Signal all waiting threads
     */
    public void signalAll()
    {
        if (queue.isEmpty())
            return;
        Iterator<Signal> iter = queue.iterator();
        while (iter.hasNext())
        {
            Signal next = iter.next();
            if (next.signal())
                iter.remove();
        }
    }

    private void cleanUpCancelled()
    {
        Iterator<Signal> iter = queue.iterator();
        while (iter.hasNext())
        {
            Signal next = iter.next();
            if (next.isCancelled())
                iter.remove();
        }
    }

    /**
     * Return how many threads are waiting
     * @return
     */
    public int getWaiting()
    {
        if (queue.isEmpty())
            return 0;
        Iterator<Signal> iter = queue.iterator();
        int count = 0;
        while (iter.hasNext())
        {
            Signal next = iter.next();
            if (next.isCancelled())
                iter.remove();
            else
                count++;
        }
        return count;
    }

}
