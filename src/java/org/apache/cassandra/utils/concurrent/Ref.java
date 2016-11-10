/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils.concurrent;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * An object that needs ref counting does the two following:
 *   - defines a Tidy object that will cleanup once it's gone,
 *     (this must retain no references to the object we're tracking (only its resources and how to clean up))
 * Then, one of two options:
 * 1) Construct a Ref directly pointing to it, and always use this Ref; or
 * 2)
 *   - implements RefCounted
 *   - encapsulates a Ref, we'll call selfRef, to which it proxies all calls to RefCounted behaviours
 *   - users must ensure no references to the selfRef leak, or are retained outside of a method scope.
 *     (to ensure the selfRef is collected with the object, so that leaks may be detected and corrected)
 *
 * This class' functionality is achieved by what may look at first glance like a complex web of references,
 * but boils down to:
 *
 * Target --> selfRef --> [Ref.State] <--> Ref.GlobalState --> Tidy
 *                                             ^
 *                                             |
 * Ref ----------------------------------------
 *                                             |
 * Global -------------------------------------
 *
 * So that, if Target is collected, Impl is collected and, hence, so is selfRef.
 *
 * Once ref or selfRef are collected, the paired Ref.State's release method is called, which if it had
 * not already been called will update Ref.GlobalState and log an error.
 *
 * Once the Ref.GlobalState has been completely released, the Tidy method is called and it removes the global reference
 * to itself so it may also be collected.
 */
public final class Ref<T> implements RefCounted<T>
{
    static final Logger logger = LoggerFactory.getLogger(Ref.class);
    public static final boolean DEBUG_ENABLED = System.getProperty("cassandra.debugrefcount", "false").equalsIgnoreCase("true");

    final State state;
    final T referent;

    public Ref(T referent, Tidy tidy)
    {
        this.state = new State(new GlobalState(tidy), this, referenceQueue);
        this.referent = referent;
    }

    Ref(T referent, GlobalState state)
    {
        this.state = new State(state, this, referenceQueue);
        this.referent = referent;
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

    public Throwable ensureReleased(Throwable accumulate)
    {
        return state.ensureReleased(accumulate);
    }

    public void ensureReleased()
    {
        maybeFail(state.ensureReleased(null));
    }

    public void close()
    {
        ensureReleased();
    }

    public T get()
    {
        state.assertNotReleased();
        return referent;
    }

    public Ref<T> tryRef()
    {
        return state.globalState.ref() ? new Ref<>(referent, state.globalState) : null;
    }

    public Ref<T> ref()
    {
        Ref<T> ref = tryRef();
        // TODO: print the last release as well as the release here
        if (ref == null)
            state.assertNotReleased();
        return ref;
    }

    public String printDebugInfo()
    {
        if (DEBUG_ENABLED)
        {
            state.debug.log(state.toString());
            return "Memory was freed by " + state.debug.deallocateThread;
        }
        return "Memory was freed";
    }

    /**
     * A convenience method for reporting:
     * @return the number of currently extant references globally, including the shared reference
     */
    public int globalCount()
    {
        return state.globalState.count();
    }

    // similar to Ref.GlobalState, but tracks only the management of each unique ref created to the managed object
    // ensures it is only released once, and that it is always released
    static final class State extends PhantomReference<Ref>
    {
        final Debug debug = DEBUG_ENABLED ? new Debug() : null;
        final GlobalState globalState;
        private volatile int released;

        private static final AtomicIntegerFieldUpdater<State> releasedUpdater = AtomicIntegerFieldUpdater.newUpdater(State.class, "released");

        public State(final GlobalState globalState, Ref reference, ReferenceQueue<? super Ref> q)
        {
            super(reference, q);
            this.globalState = globalState;
            globalState.register(this);
        }

        void assertNotReleased()
        {
            if (DEBUG_ENABLED && released == 1)
                debug.log(toString());
            assert released == 0;
        }

        Throwable ensureReleased(Throwable accumulate)
        {
            if (releasedUpdater.getAndSet(this, 1) == 0)
            {
                accumulate = globalState.release(this, accumulate);
                if (DEBUG_ENABLED)
                    debug.deallocate();
            }
            return accumulate;
        }

        void release(boolean leak)
        {
            if (!releasedUpdater.compareAndSet(this, 0, 1))
            {
                if (!leak)
                {
                    String id = this.toString();
                    logger.error("BAD RELEASE: attempted to release a reference ({}) that has already been released", id);
                    if (DEBUG_ENABLED)
                        debug.log(id);
                    throw new IllegalStateException("Attempted to release a reference that has already been released");
                }
                return;
            }
            Throwable fail = globalState.release(this, null);
            if (leak)
            {
                String id = this.toString();
                logger.error("LEAK DETECTED: a reference ({}) to {} was not released before the reference was garbage collected", id, globalState);
                if (DEBUG_ENABLED)
                    debug.log(id);
            }
            else if (DEBUG_ENABLED)
            {
                debug.deallocate();
            }
            if (fail != null)
                logger.error("Error when closing {}", globalState, fail);
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
            sb.append(thread);
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

    // the object that manages the actual cleaning up; this does not reference the target object
    // so that we can detect when references are lost to the resource itself, and still cleanup afterwards
    // the Tidy object MUST not contain any references to the object we are managing
    static final class GlobalState
    {
        // we need to retain a reference to each of the PhantomReference instances
        // we are using to track individual refs
        private final Collection<State> locallyExtant = new ConcurrentLinkedDeque<>();
        // the number of live refs
        private final AtomicInteger counts = new AtomicInteger();
        // the object to call to cleanup when our refs are all finished with
        private final Tidy tidy;

        GlobalState(Tidy tidy)
        {
            this.tidy = tidy;
            globallyExtant.add(this);
        }

        void register(Ref.State ref)
        {
            locallyExtant.add(ref);
        }

        // increment ref count if not already tidied, and return success/failure
        boolean ref()
        {
            while (true)
            {
                int cur = counts.get();
                if (cur < 0)
                    return false;
                if (counts.compareAndSet(cur, cur + 1))
                    return true;
            }
        }

        // release a single reference, and cleanup if no more are extant
        Throwable release(Ref.State ref, Throwable accumulate)
        {
            locallyExtant.remove(ref);
            if (-1 == counts.decrementAndGet())
            {
                globallyExtant.remove(this);
                try
                {
                    tidy.tidy();
                }
                catch (Throwable t)
                {
                    accumulate = merge(accumulate, t);
                }
            }
            return accumulate;
        }

        int count()
        {
            return 1 + counts.get();
        }

        public String toString()
        {
            return tidy.getClass() + "@" + System.identityHashCode(tidy) + ":" + tidy.name();
        }
    }

    private static final Set<GlobalState> globallyExtant = Collections.newSetFromMap(new ConcurrentHashMap<GlobalState, Boolean>());
    static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
    private static final ExecutorService EXEC = Executors.newFixedThreadPool(1, new NamedThreadFactory("Reference-Reaper"));
    static
    {
        EXEC.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    while (true)
                    {
                        Object obj = referenceQueue.remove();
                        if (obj instanceof Ref.State)
                        {
                            ((Ref.State) obj).release(true);
                        }
                    }
                }
                catch (InterruptedException e)
                {
                }
                finally
                {
                    EXEC.execute(this);
                }
            }
        });
    }
}
