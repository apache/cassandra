package org.apache.cassandra.utils.concurrent;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.Collections.emptyList;

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
    // the Tidy object MUST NOT contain any references to the object we are managing
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
                    if (tidy != null)
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
            if (tidy != null)
                return tidy.getClass() + "@" + System.identityHashCode(tidy) + ":" + tidy.name();
            return "@" + System.identityHashCode(this);
        }
    }

    private static final Set<GlobalState> globallyExtant = Collections.newSetFromMap(new ConcurrentHashMap<>());
    static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
    private static final ExecutorService EXEC = Executors.newFixedThreadPool(1, new NamedThreadFactory("Reference-Reaper"));
    private static final ScheduledExecutorService STRONG_LEAK_DETECTOR = !DEBUG_ENABLED ? null : Executors.newScheduledThreadPool(0, new NamedThreadFactory("Strong-Reference-Leak-Detector"));
    static
    {
        EXEC.execute(new ReferenceReaper());
        if (DEBUG_ENABLED)
        {
            STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new Visitor(), 1, 15, TimeUnit.MINUTES);
            STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new StrongLeakDetector(), 2, 15, TimeUnit.MINUTES);
        }
    }

    static final class ReferenceReaper implements Runnable
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
    }

    static class Visitor implements Runnable
    {
        final Stack<Field> path = new Stack<>();
        final Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        GlobalState visiting;

        public void run()
        {
            try
            {
                for (GlobalState globalState : globallyExtant)
                {
                    if (globalState.tidy == null)
                        continue;

                    // do a graph exploration of the GlobalState, since it should be shallow; if it references itself, we have a problem
                    path.clear();
                    visited.clear();
                    visited.add(globalState);
                    visiting = globalState;
                    visit(globalState.tidy);
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
            }
            finally
            {
                path.clear();
                visited.clear();
            }
        }

        void visit(final Object object)
        {
            for (Field field : getFields(object.getClass()))
            {
                path.push(field);
                try
                {
                    Object child = field.get(object);
                    if (child != null && visited.add(child))
                    {
                        visit(child);
                    }
                    else if (visiting == child)
                    {
                        logger.error("Strong self-ref loop detected {}", path);
                    }
                }
                catch (IllegalAccessException e)
                {
                    NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 5, TimeUnit.MINUTES, "Could not fully check for self-referential leaks", e);
                }
                catch (StackOverflowError e)
                {
                    logger.error("Stackoverflow {}", path);
                }
                path.pop();
            }
        }
    }

    static final Map<Class<?>, List<Field>> fieldMap = new HashMap<>();
    static List<Field> getFields(Class<?> clazz)
    {
        if (clazz == null || clazz == PhantomReference.class || clazz == Class.class || java.lang.reflect.Member.class.isAssignableFrom(clazz))
            return emptyList();
        List<Field> fields = fieldMap.get(clazz);
        if (fields != null)
            return fields;
        fieldMap.put(clazz, fields = new ArrayList<>());
        for (Field field : clazz.getDeclaredFields())
        {
            if (field.getType().isPrimitive() || Modifier.isStatic(field.getModifiers()))
                continue;
            field.setAccessible(true);
            fields.add(field);
        }
        fields.addAll(getFields(clazz.getSuperclass()));
        return fields;
    }

    public static class IdentityCollection
    {
        final Set<Tidy> candidates;
        public IdentityCollection(Set<Tidy> candidates)
        {
            this.candidates = candidates;
        }

        public void add(Ref<?> ref)
        {
            candidates.remove(ref.state.globalState.tidy);
        }
        public void add(SelfRefCounted<?> ref)
        {
            add(ref.selfRef());
        }
        public void add(SharedCloseable ref)
        {
            if (ref instanceof SharedCloseableImpl)
                add((SharedCloseableImpl)ref);
        }
        public void add(SharedCloseableImpl ref)
        {
            add(ref.ref);
        }
        public void add(Memory memory)
        {
            if (memory instanceof SafeMemory)
                ((SafeMemory) memory).addTo(this);
        }
    }

    private static class StrongLeakDetector implements Runnable
    {
        Set<Tidy> candidates = new HashSet<>();

        public void run()
        {
            final Set<Tidy> candidates = Collections.newSetFromMap(new IdentityHashMap<>());
            for (GlobalState state : globallyExtant)
                candidates.add(state.tidy);
            removeExpected(candidates);
            this.candidates.retainAll(candidates);
            if (!this.candidates.isEmpty())
            {
                List<String> names = new ArrayList<>();
                for (Tidy tidy : this.candidates)
                    names.add(tidy.name());
                logger.warn("Strong reference leak candidates detected: {}", names);
            }
            this.candidates = candidates;
        }

        private void removeExpected(Set<Tidy> candidates)
        {
            final Ref.IdentityCollection expected = new Ref.IdentityCollection(candidates);
            for (Keyspace ks : Keyspace.all())
            {
                for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
                {
                    View view = cfs.getTracker().getView();
                    for (SSTableReader reader : view.allKnownSSTables())
                        reader.addTo(expected);
                }
            }
        }
    }
}
