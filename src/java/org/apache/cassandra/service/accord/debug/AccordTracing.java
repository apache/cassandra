/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord.debug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Tracing;
import accord.coordinate.Coordination;
import accord.coordinate.Coordination.CoordinationKind;
import accord.local.CommandStore;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedListMap;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.accord.debug.AccordRemoteTracing.AccordTraceIn;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.LEAKY;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.SAMPLE;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.SLOWEST;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordTracing extends AccordCoordinatorMetrics.Listener
{
    private static final int MAX_EVENTS = 10000;
    private static final Logger logger = LoggerFactory.getLogger(AccordTracing.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    public enum BucketMode
    {
        LEAKY, SAMPLE, RING, SLOWEST;

        int position(int permits, int total)
        {
            switch (this)
            {
                default: throw UnhandledEnum.unknown(this);
                case SLOWEST:
                case LEAKY:
                    return Integer.MAX_VALUE;
                case RING:
                    return total % permits;
                case SAMPLE:
                    return ThreadLocalRandom.current().nextInt(total);
            }
        }
    }

    public interface ConsumeState
    {
        void accept(TxnId txnId, TxnEvents state);
    }

    public static class Message
    {
        public final long atNanos;
        public final int nodeId;
        public final int commandStoreId;
        public final String message;

        static Message withAtLeastNanos(int nodeId, int commandStoreId, String message, long atLeastNanos)
        {
            return new Message(nodeId, commandStoreId, message, Math.max(atLeastNanos, nanoTime()));
        }

        static Message withExactNanos(int nodeId, int commandStoreId, String message, long atNanos)
        {
            return new Message(nodeId, commandStoreId, message, atNanos);
        }

        private Message(int nodeId, int commandStoreId, String message, long atNanos)
        {
            this.atNanos = atNanos;
            this.nodeId = nodeId;
            this.commandStoreId = commandStoreId;
            this.message = message;
        }

        @Override
        public String toString()
        {
            return message;
        }
    }

    public static class TxnEvent implements Tracing, Comparable<TxnEvent>
    {
        public final TxnEvents parent;
        public final CoordinationKind kind;
        public final long idMicros = uniqueNowMicros();
        public final long atNanos = nanoTime();
        final List<Message> messages = new ArrayList<>();
        int index = -1, subIndex = -1;
        long elapsedNanos;

        public TxnEvent(TxnEvents parent, CoordinationKind kind)
        {
            this.parent = parent;
            this.kind = kind;
        }

        @Override
        public synchronized void trace(CommandStore commandStore, String s)
        {
            long prevNanos = messages.isEmpty() ? 0 : messages.get(messages.size() - 1).atNanos;
            int id = commandStore == null ? -1 : commandStore.id();
            // TODO (expected): make this configurable
            if (s.length() > 1000)
                s = s.substring(0, 1000);
            messages.add(Message.withAtLeastNanos(-1, id, s, prevNanos + 1));
        }

        @Override
        public int compareTo(TxnEvent that)
        {
            return Long.compareUnsigned(this.idMicros, that.idMicros);
        }

        public List<Message> messages()
        {
            return Collections.unmodifiableList(messages);
        }

        @Override
        public Tracing send()
        {
            return this;
        }

        public long elapsedNanos()
        {
            return elapsedNanos;
        }

        public long doneAtNanos()
        {
            return atNanos + elapsedNanos;
        }

        public long doneAtMicros()
        {
            return idMicros + elapsedNanos/1000;
        }

        @Override
        public void done()
        {
            elapsedNanos = nanoTime() - atNanos;
            TracePatternState pattern = parent.owner;
            if (pattern != null && pattern.bucketMode == SLOWEST && !pattern.updateSlowest(parent, elapsedNanos))
                return;

            if (index < 0)
            {
                parent.parent.txnIdMap.compute(parent.txnId, (id, events) -> {
                    TracePatternState owner = parent.owner;
                    if (owner != null)
                    {
                        synchronized (owner)
                        {
                            owner.detachedEvent.remove(idMicros);
                        }
                    }

                    if (events != parent)
                        return events;

                    TxnEventsList subList = events.subLists.get(kind);
                    TxnEventsList list = subList == null ? events : subList;
                    if (list.size < events.bucketSize && (subList == null || subList.size < events.bucketSubSize))
                    {
                        if (events.parent.globalCount.admit())
                            events.add(kind, subList, this);
                        return events;
                    }

                    for (int i = 0 ; i < list.size ; ++i)
                    {
                        TxnEvent event = list.get(i);
                        if (event.elapsedNanos < elapsedNanos)
                        {
                            events.remove(event.index);
                            events.add(kind, subList, this);
                            break;
                        }
                    }
                    return events;
                });
            }
        }

        public TxnId txnId()
        {
            return parent.txnId;
        }
    }

    private static class TxnEventsList
    {
        TxnEvent[] events;
        int size, bucketSeen;

        void addInternal(TxnEvent event)
        {
            if (events == null) events = new TxnEvent[10];
            else if (size == events.length) events = Arrays.copyOf(events, events.length * 2);
            events[size++] = event;
        }

        void truncateInternal(int eraseBefore)
        {
            if (events != null)
            {
                System.arraycopy(events, eraseBefore, events, 0, size - eraseBefore);
                Arrays.fill(events, size - eraseBefore, size, null);
                size -= eraseBefore;
            }
        }

        public TxnEvent get(int index)
        {
            return events[index];
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public int size()
        {
            return size;
        }

        void incrementSeen()
        {
            if (++bucketSeen < 0)
                bucketSeen = Integer.MAX_VALUE;
        }
    }

    public static class TxnEvents extends TxnEventsList
    {
        private final EnumMap<CoordinationKind, TxnEventsList> subLists = new EnumMap<>(CoordinationKind.class);

        final AccordTracing parent;
        final TxnId txnId;

        private CoordinationKinds traceEvents = CoordinationKinds.ALL;
        private BucketMode mode = LEAKY;
        private TracePatternState owner;
        private int bucketSize, bucketSubSize;
        private float chance = 1.0f;
        private boolean detached;

        public TxnEvents(AccordTracing parent, TxnId txnId)
        {
            this.parent = parent;
            this.txnId = txnId;
        }

        private TxnEvents setDetached()
        {
            detached = true;
            return this;
        }

        void remove(int index)
        {
            TxnEvent removing = get(index);
            TxnEventsList subList = subLists.get(removing.kind);
            if (--subList.size == 0)
            {
                subLists.remove(removing.kind);
            }
            else
            {
                if (removing.subIndex < subList.size)
                {
                    TxnEvent replaceWith = subList.events[subList.size];
                    subList.events[removing.subIndex] = replaceWith;
                    replaceWith.subIndex = removing.subIndex;
                }
                subList.events[subList.size] = null;
            }
            --size;
            if (removing.index < size)
            {
                TxnEvent replaceWith = events[size];
                events[index] = replaceWith;
                replaceWith.index = removing.index;
            }
            events[size] = null;
        }

        TxnEvents eraseEvents(CoordinationKind kind, GlobalCount globalCount)
        {
            TxnEventsList subList = subLists.remove(kind);
            if (subList == null)
                return this;

            while (!subList.isEmpty())
            {
                remove(subList.get(subList.size() - 1).index);
                globalCount.decrementAndGet();
            }

            return nullIfDefunct();
        }

        TxnEvents eraseEventsBetween(long minId, long maxId, GlobalCount globalCount)
        {
            int i = 0;
            while (i < size())
            {
                TxnEvent event = get(i);
                if (event.idMicros >= minId && event.idMicros <= maxId)
                {
                    remove(i);
                    globalCount.decrementAndGet();
                }
                else ++i;
            }
            return nullIfDefunct();
        }

        TxnEvents stopTracing()
        {
            bucketSize = bucketSubSize = 0;
            return nullIfDefunct();
        }

        TxnEvents eraseEvents(GlobalCount globalCount)
        {
            globalCount.addAndGet(-size());
            subLists.clear();
            truncateInternal(size());
            return nullIfDefunct();
        }

        TxnEvents nullIfDefunct()
        {
            return isEmpty() && bucketSize == 0 ? null : this;
        }

        TxnEvent trace(CoordinationKind kind, GlobalCount globalCount)
        {
            if (bucketSize == 0 || !traceEvents.contains(kind))
                return null;

            if (chance < 1.0f && ThreadLocalRandom.current().nextFloat() >= chance)
                return null;

            TxnEventsList subList = subLists.get(kind);
            if (subList != null && bucketSubSize <= subList.size)
            {
                subList.incrementSeen();
                int position = mode.position(bucketSubSize, subList.bucketSeen);
                if (position >= bucketSubSize)
                {
                    if (mode == SLOWEST)
                        return new TxnEvent(this, kind);
                    return null;
                }

                remove(subList.get(position).index);
            }
            else if (bucketSize <= size)
            {
                incrementSeen();
                int position = mode.position(bucketSize, bucketSeen);
                if (position >= bucketSize)
                {
                    if (mode == SLOWEST)
                        return new TxnEvent(this, kind);
                    return null;
                }

                remove(position);
            }
            else
            {
                if (globalCount != null && !globalCount.admit())
                    return null;
            }

            return newTrace(kind, subList);
        }

        TxnEvent forceTrace(CoordinationKind kind, GlobalCount globalCount)
        {
            // ignore all LOCAL accounting and filters, just add a new trace
            if (!globalCount.admit())
                return null;

            incrementSeen();
            return newTrace(kind, null);
        }

        private TxnEvent newTrace(CoordinationKind kind, TxnEventsList subList)
        {
            TxnEvent event = new TxnEvent(this, kind);
            add(kind, subList, event);
            return event;
        }

        private void add(CoordinationKind kind, TxnEventsList subList, TxnEvent event)
        {
            if (subList == null)
                subLists.put(kind, subList = new TxnEventsList());

            event.subIndex = subList.size;
            subList.addInternal(event);
            event.index = size;
            addInternal(event);
        }

        public boolean hasOwner()
        {
            return owner != null;
        }

        public CoordinationKinds traceEvents()
        {
            return traceEvents;
        }

        public int bucketSize()
        {
            return bucketSize;
        }

        public int bucketSubSize()
        {
            return bucketSubSize;
        }

        public int bucketSeen()
        {
            return bucketSeen;
        }

        public float chance()
        {
            return chance;
        }

        public BucketMode bucketMode()
        {
            return mode;
        }

        public void forEach(Consumer<TxnEvent> forEach)
        {
            for (int i = 0 ; i < size ; ++i)
            {
                synchronized (events[i])
                {
                    forEach.accept(events[i]);
                }
            }
        }
    }

    enum NewOrFailure
    {
        NEW, FAILURE
    }

    public static class TracePattern
    {
        private static final TracePattern EMPTY = new TracePattern(null, null, null, null, 1.0f);

        public final TxnKindsAndDomains kinds;
        public final Participants<?> intersects;
        public final CoordinationKinds traceNew;
        public final CoordinationKinds traceFailures;
        public final float chance;

        public TracePattern(TxnKindsAndDomains kinds, @Nullable Participants<?> intersects, CoordinationKinds traceNew, CoordinationKinds traceFailures, float chance)
        {
            this.kinds = kinds;
            this.intersects = intersects;
            this.traceNew = traceNew;
            this.traceFailures = traceFailures;
            this.chance = chance;
        }

        public TracePattern withKinds(TxnKindsAndDomains kinds)
        {
            return new TracePattern(kinds, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withIntersects(Participants<?> intersects)
        {
            return new TracePattern(kinds, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withTraceNew(CoordinationKinds traceNew)
        {
            return new TracePattern(kinds, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withTraceFailures(CoordinationKinds traceFailures)
        {
            return new TracePattern(kinds, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withChance(float chance)
        {
            return new TracePattern(kinds, intersects, traceNew, traceFailures, chance);
        }

        boolean matches(TxnId txnId, @Nullable Routables<?> participants, CoordinationKind kind, NewOrFailure newOrFailure)
        {
            if (kinds != null && !kinds.matches(txnId))
                return false;

            TinyEnumSet<CoordinationKind> testKind = newOrFailure == NewOrFailure.NEW ? traceNew : traceFailures;
            if (testKind == null || !testKind.contains(kind))
                return false;

            if (intersects != null && (participants == null || !participants.intersects(intersects)))
                return false;

            return chance >= 1.0f || ThreadLocalRandom.current().nextFloat() <= chance;
        }
    }

    private static class SlowestTxnId implements Comparable<SlowestTxnId>
    {
        final TxnId txnId;
        long elapsedNanos;

        private SlowestTxnId(TxnId txnId, long elapsedNanos)
        {
            this.txnId = txnId;
            this.elapsedNanos = elapsedNanos;
        }

        @Override
        public int compareTo(SlowestTxnId that)
        {
            int c = Long.compare(this.elapsedNanos, that.elapsedNanos);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            return c;
        }
    }

    public class TracePatternState
    {
        final int id;

        private volatile TracePattern pattern;
        private BucketMode bucketMode = SAMPLE;
        private int bucketSize;
        private int bucketSeen;
        private BucketMode traceBucketMode = SAMPLE;
        private int traceBucketSize, traceBucketSubSize;
        private CoordinationKinds traceEvents = new CoordinationKinds(false, 0);

        private final List<TxnId> txnIds = new ArrayList<>();
        private final WeakHashMap<TxnId, TxnEvents> detachedEvents = new WeakHashMap<>();
        private final WeakHashMap<Long, TxnEvent> detachedEvent = new WeakHashMap<>();
        private final Map<TxnId, SlowestTxnId> slowestLookup = new HashMap<>();
        private final TreeSet<SlowestTxnId> slowest = new TreeSet<>();

        public TracePatternState(int id)
        {
            this.pattern = TracePattern.EMPTY;
            this.id = id;
        }

        public int id() { return id; }
        public TracePattern pattern() { return pattern; }
        public int bucketSize() { return bucketSize; }
        public BucketMode mode() { return bucketMode; }
        public int bucketSeen() { return bucketSeen; }
        public BucketMode traceWithMode() { return traceBucketMode; }
        public int traceBucketSize() { return traceBucketSize; }
        public int traceBucketSubSize() { return traceBucketSubSize; }
        public CoordinationKinds traceEvents() { return traceEvents; }

        public int currentSize()
        {
            return txnIds.size();
        }

        public TxnId get(int index)
        {
            return txnIds.get(index);
        }

        TxnEvents maybeAdd(TxnId txnId, @Nullable Routables<?> participants, CoordinationKind kind, NewOrFailure newOrFailure)
        {
            if (!pattern.matches(txnId, participants, kind, newOrFailure))
                return null;

            return maybeAdd(txnId);
        }

        private TxnEvents maybeAdd(TxnId txnId)
        {
            class MaybeAdd implements BiFunction<TxnId, TxnEvents, TxnEvents>
            {
                TxnEvents result;
                TxnId untrace;

                @Override
                public TxnEvents apply(TxnId txnId, TxnEvents in)
                {
                    synchronized (TracePatternState.this)
                    {
                        if (in != null)
                            return null; // already tracing

                        if (bucketSize == 0)
                            return null;

                        if (++bucketSeen < 0)
                            bucketSeen = Integer.MAX_VALUE;

                        if (bucketSize > txnIds.size())
                        {
                            result = newEvents(txnId);
                            txnIds.add(txnId);
                            if (bucketMode == SLOWEST)
                                initSlowest(result);
                            return result;
                        }

                        int position = bucketMode.position(bucketSize, bucketSeen);
                        if (position >= bucketSize)
                        {
                            if (bucketMode == SLOWEST)
                                result = detachedEvents.computeIfAbsent(txnId, id -> newEvents(id).setDetached());
                            return null;
                        }

                        result = newEvents(txnId);
                        untrace = txnIds.get(position);
                        txnIds.set(position, txnId);
                        if (bucketMode == SLOWEST)
                            initSlowest(result);
                        return result;
                    }
                }
            }

            MaybeAdd maybeAdd = new MaybeAdd();
            txnIdMap.compute(txnId, maybeAdd);
            if (maybeAdd.untrace != null)
                untrace(maybeAdd.untrace);
            return maybeAdd.result;
        }

        private boolean initSlowest(TxnEvents events)
        {
            SlowestTxnId entry = new SlowestTxnId(events.txnId, Long.MAX_VALUE);
            if (null != slowestLookup.putIfAbsent(events.txnId, entry))
                return false;
            slowest.add(entry);
            return true;
        }

        private boolean updateSlowest(TxnEvents events, long elapsedNanos)
        {
            synchronized (this)
            {
                SlowestTxnId entry = slowestLookup.get(events.txnId);
                if (entry != null)
                {
                    if (entry.elapsedNanos < elapsedNanos)
                    {
                        slowest.remove(entry);
                        entry.elapsedNanos = elapsedNanos;
                        slowest.add(entry);
                    }
                    return true;
                }
            }

            class UpdateSlowest implements BiFunction<TxnId, TxnEvents, TxnEvents>
            {
                boolean result;
                TxnId untrace;

                @Override
                public TxnEvents apply(TxnId txnId, TxnEvents cur)
                {
                    if (cur != null && cur != events)
                        return cur;

                    synchronized (TracePatternState.this)
                    {
                        SlowestTxnId entry = slowestLookup.get(events.txnId);
                        if (entry != null)
                        {
                            if (entry.elapsedNanos < elapsedNanos)
                            {
                                slowest.remove(entry);
                                entry.elapsedNanos = elapsedNanos;
                                slowest.add(entry);
                            }
                            result = true;
                            return events;
                        }

                        int index = txnIds.size();
                        if (index >= bucketSize && !slowest.isEmpty())
                        {
                            SlowestTxnId leastSlow = slowest.first();
                            if (leastSlow.elapsedNanos >= elapsedNanos)
                                return cur;

                            SlowestTxnId removed;
                            removed = slowest.pollFirst();
                            Invariants.expect(leastSlow.equals(removed), "%s != %s", entry, removed);
                            removed = slowestLookup.remove(leastSlow.txnId);
                            Invariants.expect(leastSlow.equals(removed), "%s != %s", entry, removed);
                            index = txnIds.indexOf(leastSlow.txnId);
                            Invariants.expect(index >= 0);
                            if (index < 0)
                                return cur;

                            untrace = leastSlow.txnId;
                            txnIds.set(index, events.txnId);
                        }
                        else
                        {
                            if (index > bucketSize)
                                txnIds.remove(0);
                            txnIds.add(events.txnId);
                        }

                        if (null != slowestLookup.put(events.txnId, entry = new SlowestTxnId(events.txnId, elapsedNanos)))
                            return cur;

                        slowest.add(entry);
                        detachedEvents.remove(entry.txnId);
                        events.detached = false;
                        result = true;
                        return events;
                    }
                }
            }

            UpdateSlowest updateSlowest = new UpdateSlowest();
            txnIdMap.compute(events.txnId, updateSlowest);
            if (updateSlowest.untrace != null)
                untrace(updateSlowest.untrace);
            return updateSlowest.result;
        }

        private void untrace(TxnId txnId)
        {
            txnIdMap.compute(txnId, (ignore, cur) -> {
                if (cur == null || cur.owner != this)
                    return cur;

                cur.stopTracing();
                return cur.eraseEvents(globalCount);
            });
        }

        private TxnEvents newEvents(TxnId txnId)
        {
            TxnEvents events = new TxnEvents(AccordTracing.this, txnId);
            events.mode = traceBucketMode;
            events.bucketSize = traceBucketSize;
            events.bucketSubSize = traceBucketSubSize;
            events.owner = this;
            return events;
        }

        synchronized void set(Function<TracePattern, TracePattern> pattern, BucketMode newBucketMode, int newBucketSeen, int newBucketSize, BucketMode newTraceBucketMode, int newTraceBucketSize, int newTraceBucketSubSize, CoordinationKinds newTraceEvents)
        {
            Invariants.require(newBucketSize != 0);
            Invariants.require(newTraceBucketSize != 0);
            this.pattern = pattern.apply(this.pattern);
            if (newBucketMode != null && newBucketMode != bucketMode)
            {
                if (bucketMode == SLOWEST)
                {
                    slowest.clear();
                    slowestLookup.clear();
                    detachedEvents.clear();
                }
                this.bucketMode = newBucketMode;
            }
            if (newBucketSize >= 0)
                this.bucketSize = newBucketSize;
            if (newBucketSeen >= 0)
                this.bucketSeen = newBucketSeen;
            if (newTraceBucketMode != null)
                this.traceBucketMode = newTraceBucketMode;
            if (newTraceBucketSize >= 0)
                this.traceBucketSize = newTraceBucketSize;
            if (newTraceBucketSubSize >= 0)
                this.traceBucketSubSize = newTraceBucketSubSize;
            if (newTraceEvents != null)
                this.traceEvents = newTraceEvents;
        }

        void clear()
        {
            List<TxnId> untrace;
            synchronized (this)
            {
                untrace = new ArrayList<>(txnIds);
                txnIds.clear();
                slowest.clear();
                slowestLookup.clear();
                detachedEvents.clear();
            }
            for (TxnId txnId : untrace)
                untrace(txnId);
        }
    }

    static class GlobalCount extends AtomicInteger
    {
        public boolean admit()
        {
            if (incrementAndGet() <= MAX_EVENTS)
                return true;

            decrementAndGet();
            ClientWarn.instance.warn("Too many Accord trace events stored already; delete some to continue tracing");
            noSpamLogger.warn("Too many Accord trace events stored already; delete some to continue tracing");
            return false;
        }
    }

    private static final AtomicLong lastNowMicros = new AtomicLong();
    private static long uniqueNowMicros()
    {
        long nowMicros = Clock.Global.currentTimeMillis() * 1000;
        while (true)
        {
            long last = lastNowMicros.get();
            if (last >= nowMicros)
                return lastNowMicros.incrementAndGet();
            if (lastNowMicros.compareAndSet(last, nowMicros))
                return nowMicros;
        }
    }

    final Map<TxnId, TxnEvents> txnIdMap = new ConcurrentHashMap<>();
    final CopyOnWriteArrayList<TracePatternState> allPatterns = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<TracePatternState> traceNewPatterns = new CopyOnWriteArrayList<>();
    final GlobalCount globalCount = new GlobalCount();

    public Tracing trace(TxnId txnId, @Nullable Routables<?> participants, CoordinationKind kind)
    {
        if (kind == CoordinationKind.FetchDurableBefore)
            return (cs, msg) -> logger.info("Catchup/FetchDurableBefore: {}", msg.length() <= 100 ? msg : msg.substring(0, 100));

        if (!txnIdMap.containsKey(txnId))
        {
            TxnEvents events = maybeTrace(txnId, participants, kind, NewOrFailure.NEW, traceNewPatterns);
            if (events == null)
                return null;

            if (events.detached)
            {
                class RegisterDetached implements BiFunction<TxnId, TxnEvents, TxnEvents>
                {
                    TxnEvent event;

                    @Override
                    public TxnEvents apply(TxnId txnId, TxnEvents state)
                    {
                        if (state != null) event = state.trace(kind, globalCount);
                        else
                        {
                            TracePatternState owner = events.owner;
                            if (owner != null)
                            {
                                synchronized (owner)
                                {
                                    event = new TxnEvent(events, kind);
                                    owner.detachedEvent.put(event.idMicros, event);
                                }
                            }
                        }
                        return state;
                    }
                }
                RegisterDetached register = new RegisterDetached();
                txnIdMap.compute(txnId, register);
                return register.event;
            }
        }

        class Register implements BiFunction<TxnId, TxnEvents, TxnEvents>
        {
            TxnEvent event;

            @Override
            public TxnEvents apply(TxnId txnId, TxnEvents state)
            {
                if (state == null)
                    return null;

                event = state.trace(kind, globalCount);
                return state;
            }
        }
        Register register = new Register();
        txnIdMap.compute(txnId, register);
        return register.event;
    }

    public void report(AccordTraceIn in, int nodeId)
    {
        txnIdMap.compute(in.txnId, (ignore, events) -> {
            if (events != null)
            {
                report(in, nodeId, events);
                return events;
            }
            else
            {
                for (TracePatternState patternState : allPatterns)
                {
                    synchronized (patternState)
                    {
                        TxnEvent detached = patternState.detachedEvent.get(in.idMicros);
                        if (detached != null)
                        {
                            report(in, nodeId, detached);
                            return null;
                        }
                    }
                }
                return null;
            }
        });

    }

    private boolean report(AccordTraceIn in, int nodeId, TxnEvents events)
    {
        for (int i = 0 ; i < events.size ; ++i)
        {
            TxnEvent event = events.get(i);
            if (event.idMicros == in.idMicros)
            {
                report(in, nodeId, event);
                return true;
            }
        }
        return false;
    }

    private void report(AccordTraceIn in, int nodeId, TxnEvent event)
    {
        synchronized (event)
        {
            long atNanos = Math.max(in.atNanos, event.atNanos);
            int j = event.messages.size();
            while (j > 0 && event.messages.get(j - 1).atNanos >= atNanos)
                --j;
            while (j < event.messages.size() && event.messages.get(j).atNanos == atNanos)
            {
                atNanos++;
                j++;
            }
            event.messages.add(j, Message.withExactNanos(nodeId, in.commandStoreId, in.message, atNanos));
        }
    }

    // null values, or values < 0, are ignored
    public void set(TxnId txnId, CoordinationKinds trace, BucketMode newBucketMode, int newBucketSize, int newBucketSubSize, int newBucketSeen, float newChance, boolean unsetManagedByPattern)
    {
        Invariants.requireArgument(newBucketSize != 0);
        Invariants.requireArgument(newBucketSubSize != 0);
        Invariants.requireArgument(Float.isNaN(newChance) || (newChance <= 1.0f && newChance > 0f));
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
            {
                if (newBucketSize < 0)
                    throw new IllegalArgumentException("Must specify bucket size for new trace config.");

                cur = new TxnEvents(this, id);
                if (newBucketSubSize < 0)
                    cur.bucketSubSize = newBucketSize;
            }

            if (newBucketMode != null)
                cur.mode = newBucketMode;
            if (newBucketSize >= 0)
                cur.bucketSize = newBucketSize;
            if (newBucketSubSize >= 0)
                cur.bucketSubSize = newBucketSubSize;
            if (newBucketSeen >= 0)
                cur.bucketSeen = newBucketSeen;
            if (!Float.isNaN(newChance))
                cur.chance = newChance;
            if (trace != null)
                cur.traceEvents = trace;
            if (unsetManagedByPattern)
                cur.owner = null;
            return cur;
        });
    }

    public void stopTracing(TxnId txnId)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            return cur.stopTracing();
        });
    }

    public void eraseEvents(TxnId txnId)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            return cur.eraseEvents(globalCount);
        });
    }

    public void eraseEvents(TxnId txnId, CoordinationKind kind)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            return cur.eraseEvents(kind, globalCount);
        });
    }

    public void eraseEventsBetween(TxnId txnId, long minIdInclusive, long maxIdInclusive)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            return cur.eraseEventsBetween(minIdInclusive, maxIdInclusive, globalCount);
        });
    }

    public void eraseAllEvents()
    {
        txnIdMap.keySet().forEach(this::eraseEvents);
    }

    public void eraseAllBuckets()
    {
        txnIdMap.keySet().forEach(this::stopTracing);
    }

    public void eraseAll()
    {
        allPatterns.forEach(TracePatternState::clear);
        txnIdMap.keySet().forEach(txnId -> {
            eraseEvents(txnId);
            stopTracing(txnId);
        });
    }

    public void forEach(Predicate<TxnId> include, ConsumeState forEach)
    {
        txnIdMap.forEach((txnId, state) -> {
            if (include.test(txnId))
            {
                // ensure lock is held for duration of callback
                txnIdMap.compute(txnId, (id, cur) -> {
                    if (cur != null)
                        forEach.accept(txnId, cur);
                    return cur;
                });
            }
        });
    }

    public void forEach(TxnId txnId, Consumer<TxnEvents> forEach)
    {
        // ensure lock is held for duration of callback
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur != null)
                forEach.accept(cur);
            return cur;
        });
    }

    public void setPattern(int id, Function<TracePattern, TracePattern> pattern, BucketMode newBucketMode, int newBucketSeen, int newBucketSize, BucketMode newTraceBucketMode, int newTraceBucketSize, int newTraceBucketSubSize, CoordinationKinds newTraceEvents)
    {
        synchronized (allPatterns)
        {
            TracePatternState state = findPattern(id, false);
            TracePatternState update = state != null ? state : new TracePatternState(id);
            boolean prevTraceNew = state != null && state.pattern.traceNew != null;
            update.set(pattern, newBucketMode, newBucketSeen, newBucketSize, newTraceBucketMode, newTraceBucketSize, newTraceBucketSubSize, newTraceEvents);
            if (state == null)
                allPatterns.add(update);
            if (update.pattern.traceNew != null && !prevTraceNew)
                traceNewPatterns.add(update);
            else if (update.pattern.traceNew == null && prevTraceNew)
                traceNewPatterns.remove(update);
        }
    }

    public void erasePattern(int id)
    {
        TracePatternState removed = findPattern(id, true);
        if (removed != null)
            removed.clear();
    }


    public void erasePatternTraces(int id)
    {
        TracePatternState state = findPattern(id, false);
        if (state != null)
            state.clear();
    }

    private TracePatternState findPattern(int id, boolean remove)
    {
        synchronized (allPatterns)
        {
            for (int i = 0; i < allPatterns.size() ; ++i)
            {
                TracePatternState state = allPatterns.get(i);
                if (state.id == id)
                {
                    if (remove)
                    {
                        allPatterns.remove(i);
                        if (state.pattern.traceNew != null)
                            traceNewPatterns.remove(state);
                    }
                    return state;
                }
            }
        }
        return null;
    }

    public void eraseAllPatterns()
    {
        List<TracePatternState> removed = new ArrayList<>();
        allPatterns.removeIf(p -> { removed.add(p); return true; });
        removed.forEach(TracePatternState::clear);
    }

    public void eraseAllPatternTraces()
    {
        for (TracePatternState state : allPatterns)
            state.clear();
    }

    public void forEachPattern(Consumer<TracePatternState> consumer)
    {
        allPatterns.forEach(pattern -> {
            synchronized (pattern)
            {
                consumer.accept(pattern);
            }
        });
    }

    @Override
    public void onFailed(Throwable failure, TxnId txnId, Participants<?> participants, Coordination coordination)
    {
        TxnEvents tracing = maybeTrace(txnId, participants, coordination.kind(), NewOrFailure.FAILURE, allPatterns);
        if (tracing != null)
        {
            txnIdMap.compute(txnId, (id, cur) -> {
                if (cur != tracing)
                    return cur;

                // TODO (desired): assign an idMicros that is based on coordinationId
                TxnEvent event = tracing.forceTrace(coordination.kind(), globalCount);
                if (event == null) // we still honour global limit
                    return cur;

                event.trace(null, "Failed Coordination Dump");
                Coordination.traceStart(event, coordination);
                SortedListMap<Node.Id, ?> replies = coordination.replies();
                if (replies != null)
                {
                    for (int i = 0 ; i < replies.domainSize() ; ++i)
                        event.trace(null, "from %s: %s", replies.getKey(i), replies.getValue(i));
                }
                Coordination.traceStop(event, coordination);
                return cur;
            });
        }
    }

    private TxnEvents maybeTrace(TxnId txnId, @Nullable Routables<?> participants, CoordinationKind kind, NewOrFailure newOrFailure, List<TracePatternState> patterns)
    {
        if (patterns.isEmpty())
            return null;

        for (TracePatternState state : patterns)
        {
            TxnEvents added = state.maybeAdd(txnId, participants, kind, newOrFailure);
            if (added != null)
                return added;
        }
        return null;
    }
}
