package org.apache.cassandra.utils.concurrent;

import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.concurrent.NamedThreadFactory;

// default implementation; can be hidden and proxied (like we do for SSTableReader)
final class RefCountedImpl implements RefCounted
{
    private final Ref sharedRef;
    private final GlobalState state;

    public RefCountedImpl(Tidy tidy)
    {
        this.state = new GlobalState(tidy);
        sharedRef = new Ref(this.state, true);
        globallyExtant.add(this.state);
    }

    /**
     * see {@link RefCounted#tryRef()}
     */
    public Ref tryRef()
    {
        return state.ref() ? new Ref(state, false) : null;
    }

    /**
     * see {@link RefCounted#sharedRef()}
     */
    public Ref sharedRef()
    {
        return sharedRef;
    }

    // the object that manages the actual cleaning up; this does not reference the RefCounted.Impl
    // so that we can detect when references are lost to the resource itself, and still cleanup afterwards
    // the Tidy object MUST not contain any references to the object we are managing
    static final class GlobalState
    {
        // we need to retain a reference to each of the PhantomReference instances
        // we are using to track individual refs
        private final ConcurrentLinkedQueue<Ref.State> locallyExtant = new ConcurrentLinkedQueue<>();
        // the number of live refs
        private final AtomicInteger counts = new AtomicInteger();
        // the object to call to cleanup when our refs are all finished with
        private final Tidy tidy;

        GlobalState(Tidy tidy)
        {
            this.tidy = tidy;
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
        void release(Ref.State ref)
        {
            locallyExtant.remove(ref);
            if (-1 == counts.decrementAndGet())
            {
                globallyExtant.remove(this);
                tidy.tidy();
            }
        }

        int count()
        {
            return 1 + counts.get();
        }

        public String toString()
        {
            return tidy.name();
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

