package org.apache.cassandra.utils.concurrent;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single managed reference to a RefCounted object
 */
public final class Ref
{
    static final Logger logger = LoggerFactory.getLogger(Ref.class);
    static final boolean DEBUG_ENABLED = System.getProperty("cassandra.debugrefcount", "false").equalsIgnoreCase("true");

    final State state;

    Ref(RefCountedImpl.GlobalState state, boolean isSharedRef)
    {
        this.state = new State(state, this, RefCountedImpl.referenceQueue, isSharedRef);
    }

    /**
     * Must be called exactly once, when the logical operation for which this Ref was created has terminated.
     * Failure to abide by this contract will result in an error (eventually) being reported, assuming a
     * hard reference to the resource it managed is not leaked.
     */
    public void release()
    {
        state.release(false);
    }

    /**
     * A convenience method for reporting:
     * @return the number of currently extant references globally, including the shared reference
     */
    public int globalCount()
    {
        return state.globalState.count();
    }

    // similar to RefCountedState, but tracks only the management of each unique ref created to the managed object
    // ensures it is only released once, and that it is always released
    static final class State extends PhantomReference<Ref>
    {
        final Debug debug = DEBUG_ENABLED ? new Debug() : null;
        final boolean isSharedRef;
        final RefCountedImpl.GlobalState globalState;
        private volatile int released;

        private static final AtomicIntegerFieldUpdater<State> releasedUpdater = AtomicIntegerFieldUpdater.newUpdater(State.class, "released");

        public State(final RefCountedImpl.GlobalState globalState, Ref reference, ReferenceQueue<? super Ref> q, boolean isSharedRef)
        {
            super(reference, q);
            this.globalState = globalState;
            this.isSharedRef = isSharedRef;
            globalState.register(this);
        }

        void release(boolean leak)
        {
            if (!releasedUpdater.compareAndSet(this, 0, 1))
            {
                if (!leak)
                {
                    String id = this.toString();
                    logger.error("BAD RELEASE: attempted to release a{} reference ({}) that has already been released", isSharedRef ? " shared" : "", id);
                    if (DEBUG_ENABLED)
                        debug.log(id);
                    throw new IllegalStateException("Attempted to release a reference that has already been released");
                }
                return;
            }
            globalState.release(this);
            if (leak)
            {
                String id = this.toString();
                if (isSharedRef)
                    logger.error("LEAK DETECTED: the shared reference ({}) to {} was not released before the object was garbage collected", id, globalState);
                else
                    logger.error("LEAK DETECTED: a reference ({}) to {} was not released before the reference was garbage collected", id, globalState);
                if (DEBUG_ENABLED)
                    debug.log(id);
            }
            else if (DEBUG_ENABLED)
            {
                debug.deallocate();
            }
        }
    }

    static final class Debug
    {
        String allocateThread, deallocateThread;
        StackTraceElement[] allocateTrace, deallocateTrace;
        Debug()
        {
            Thread thread = Thread.currentThread();
            allocateThread = thread.toString();
            allocateTrace = thread.getStackTrace();
        }
        synchronized void deallocate()
        {
            Thread thread = Thread.currentThread();
            deallocateThread = thread.toString();
            deallocateTrace = thread.getStackTrace();
        }
        synchronized void log(String id)
        {
            logger.error("Allocate trace {}:\n{}", id, print(allocateThread, allocateTrace));
            if (deallocateThread != null)
                logger.error("Deallocate trace {}:\n{}", id, print(deallocateThread, deallocateTrace));
        }
        String print(String thread, StackTraceElement[] trace)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(thread.toString());
            sb.append("\n");
            for (StackTraceElement element : trace)
            {
                sb.append("\tat ");
                sb.append(element );
                sb.append("\n");
            }
            return sb.toString();
        }
    }

}


