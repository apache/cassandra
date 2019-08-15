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
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import static java.util.Collections.emptyList;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor.InterruptibleRunnable;

import static org.apache.cassandra.utils.ExecutorUtils.awaitTermination;
import static org.apache.cassandra.utils.ExecutorUtils.shutdownNow;
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
 * {@code
 * Target --> selfRef --> [Ref.State] <--> Ref.GlobalState --> Tidy
 *                                             ^
 *                                             |
 * Ref ----------------------------------------
 *                                             |
 * Global -------------------------------------
 * }
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

    private static final Class<?>[] concurrentIterableClasses = new Class<?>[]
    {
        ConcurrentLinkedQueue.class,
        ConcurrentLinkedDeque.class,
        ConcurrentSkipListSet.class,
        CopyOnWriteArrayList.class,
        CopyOnWriteArraySet.class,
        DelayQueue.class,
        NonBlockingHashMap.class,
    };
    static final Set<Class<?>> concurrentIterables = Collections.newSetFromMap(new IdentityHashMap<>());
    private static final Set<GlobalState> globallyExtant = Collections.newSetFromMap(new ConcurrentHashMap<>());
    static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
    private static final InfiniteLoopExecutor EXEC = new InfiniteLoopExecutor("Reference-Reaper", Ref::reapOneReference).start();
    static final ScheduledExecutorService STRONG_LEAK_DETECTOR = !DEBUG_ENABLED ? null : Executors.newScheduledThreadPool(1, new NamedThreadFactory("Strong-Reference-Leak-Detector"));
    static
    {
        if (DEBUG_ENABLED)
        {
            STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new Visitor(), 1, 15, TimeUnit.MINUTES);
            STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new StrongLeakDetector(), 2, 15, TimeUnit.MINUTES);
        }
        concurrentIterables.addAll(Arrays.asList(concurrentIterableClasses));
    }

    private static void reapOneReference() throws InterruptedException
    {
        Object obj = referenceQueue.remove(100);
        if (obj instanceof Ref.State)
        {
            ((Ref.State) obj).release(true);
        }
    }

    static final Deque<InProgressVisit> inProgressVisitPool = new ArrayDeque<InProgressVisit>();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static InProgressVisit newInProgressVisit(Object o, List<Field> fields, Field field, String name)
    {
        Preconditions.checkNotNull(o);
        InProgressVisit ipv = inProgressVisitPool.pollLast();
        if (ipv == null)
            ipv = new InProgressVisit();

        ipv.o = o;
        if (o instanceof Object[])
            ipv.collectionIterator = Arrays.asList((Object[])o).iterator();
        else if (o instanceof ConcurrentMap)
        {
            ipv.isMapIterator = true;
            ipv.collectionIterator = ((Map)o).entrySet().iterator();
        }
        else if (concurrentIterables.contains(o.getClass()) | o instanceof BlockingQueue)
            ipv.collectionIterator = ((Iterable)o).iterator();

        ipv.fields = fields;
        ipv.field = field;
        ipv.name = name;
        return ipv;
    }

    static void returnInProgressVisit(InProgressVisit ipv)
    {
        if (inProgressVisitPool.size() > 1024)
            return;
        ipv.name = null;
        ipv.fields = null;
        ipv.o = null;
        ipv.fieldIndex = 0;
        ipv.field = null;
        ipv.collectionIterator = null;
        ipv.mapEntryValue = null;
        ipv.isMapIterator = false;
        inProgressVisitPool.offer(ipv);
    }

    /*
     * Stack state for walking an object graph.
     * Field index is the index of the current field being fetched.
     */
    @SuppressWarnings({ "rawtypes"})
    static class InProgressVisit
    {
        String name;
        List<Field> fields;
        Object o;
        int fieldIndex = 0;
        Field field;

        //Need to know if Map.Entry should be returned or traversed as an object
        boolean isMapIterator;
        //If o is a ConcurrentMap, BlockingQueue, or Object[], this is populated with an iterator over the contents
        Iterator<Object> collectionIterator;
        //If o is a ConcurrentMap the entry set contains keys and values. The key is returned as the first child
        //And the associated value is stashed here and returned next
        Object mapEntryValue;

        private Field nextField()
        {
            if (fields.isEmpty())
                return null;

            if (fieldIndex >= fields.size())
                return null;

            Field retval = fields.get(fieldIndex);
            fieldIndex++;
            return retval;
        }

        Pair<Object, Field> nextChild() throws IllegalAccessException
        {
            //If the last child returned was a key from a map, the value from that entry is stashed
            //so it can be returned next
            if (mapEntryValue != null)
            {
                Pair<Object, Field> retval = Pair.create(mapEntryValue, field);
                mapEntryValue = null;
                return retval;
            }

            //If o is a ConcurrentMap, BlockingQueue, or Object[], then an iterator will be stored to return the elements
            if (collectionIterator != null)
            {
                if (!collectionIterator.hasNext())
                    return null;
                Object nextItem = null;
                //Find the next non-null element to traverse since returning null will cause the visitor to stop
                while (collectionIterator.hasNext() && (nextItem = collectionIterator.next()) == null){}
                if (nextItem != null)
                {
                    if (isMapIterator & nextItem instanceof Map.Entry)
                    {
                        Map.Entry entry = (Map.Entry)nextItem;
                        mapEntryValue = entry.getValue();
                        return Pair.create(entry.getKey(), field);
                    }
                    return Pair.create(nextItem, field);
                }
                else
                {
                    return null;
                }
            }

            //Basic traversal of an object by its member fields
            //Don't return null values as that indicates no more objects
            while (true)
            {
                Field nextField = nextField();
                if (nextField == null)
                    return null;

                //A weak reference isn't strongly reachable
                //subclasses of WeakReference contain strong references in their fields, so those need to be traversed
                //The weak reference fields are in the common Reference class base so filter those out
                if (o instanceof WeakReference & nextField.getDeclaringClass() == Reference.class)
                    continue;

                Object nextObject = nextField.get(o);
                if (nextObject != null)
                    return Pair.create(nextField.get(o), nextField);
            }
        }

        @Override
        public String toString()
        {
            return field == null ? name : field.toString() + "-" + o.getClass().getName();
        }
    }

    static class Visitor implements Runnable
    {
        final Deque<InProgressVisit> path = new ArrayDeque<>();
        final Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        @VisibleForTesting
        int lastVisitedCount;
        @VisibleForTesting
        long iterations = 0;
        GlobalState visiting;
        Set<GlobalState> haveLoops;

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
                    lastVisitedCount = 0;
                    iterations = 0;
                    visited.add(globalState);
                    visiting = globalState;
                    traverse(globalState.tidy);
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
            }
            finally
            {
                lastVisitedCount = visited.size();
                path.clear();
                visited.clear();
            }
        }

        /*
         * Searches for an indirect strong reference between rootObject and visiting.
         */
        void traverse(final RefCounted.Tidy rootObject)
        {
            path.offer(newInProgressVisit(rootObject, getFields(rootObject.getClass()), null, rootObject.name()));

            InProgressVisit inProgress = null;
            while (inProgress != null || !path.isEmpty())
            {
                //If necessary fetch the next object to start tracing
                if (inProgress == null)
                    inProgress = path.pollLast();

                try
                {
                    Pair<Object, Field> p = inProgress.nextChild();
                    Object child = null;
                    Field field = null;

                    if (p != null)
                    {
                        iterations++;
                        child = p.left;
                        field = p.right;
                    }

                    if (child != null && visited.add(child))
                    {
                        path.offer(inProgress);
                        inProgress = newInProgressVisit(child, getFields(child.getClass()), field, null);
                        continue;
                    }
                    else if (visiting == child)
                    {
                        if (haveLoops != null)
                            haveLoops.add(visiting);
                        NoSpamLogger.log(logger,
                                NoSpamLogger.Level.ERROR,
                                rootObject.getClass().getName(),
                                1,
                                TimeUnit.SECONDS,
                                "Strong self-ref loop detected {}",
                                path);
                    }
                    else if (child == null)
                    {
                        returnInProgressVisit(inProgress);
                        inProgress = null;
                        continue;
                    }
                }
                catch (IllegalAccessException e)
                {
                    NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 5, TimeUnit.MINUTES, "Could not fully check for self-referential leaks", e);
                }
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

    @VisibleForTesting
    public static void shutdownReferenceReaper(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, EXEC, STRONG_LEAK_DETECTOR);
    }
}
