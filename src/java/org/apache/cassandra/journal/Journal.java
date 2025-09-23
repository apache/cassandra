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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Long2ObjectHashMap;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Interruptible.TerminateException;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.journal.Segments.ReferencedSegments;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SystemThreadTag.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.SYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.concurrent.Interruptible.State.SHUTTING_DOWN;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

/**
 * A generic append-only journal with some special features:
 * <p>
 * <ul>
 * <li>Records can be looked up by key
 * <li>Invalidated records get purged during segment compaction
 * </ul>
 * </p>
 *
 * Type parameters:
 * @param <V> the type of records stored in the journal
 * @param <K> the type of keys used to address the records;
              must be fixed-size and byte-order comparable
 */
@Simulate(with=MONITORS)
public class Journal<K, V> implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);

    final String name;
    final File directory;
    final Params params;

    final KeySupport<K> keySupport;
    final ValueSerializer<K, V> valueSerializer;
    final KeyStats.Factory<K> keyStatsFactory;

    final Metrics<K, V> metrics;

    final Flusher<K, V> flusher;
    final Compactor<K, V> compactor;
    Interruptible allocator;
    SequentialExecutorPlus closer, releaser;

    volatile long replayLimit;
    final AtomicLong nextSegmentId = new AtomicLong();

    private volatile ActiveSegment<K, V> currentSegment = null;

    // segment that is ready to be used; allocator thread fills this and blocks until consumed
    private volatile ActiveSegment<K, V> availableSegment = null;

    private final AtomicReference<Segments<K, V>> segments = new AtomicReference<>(new Segments<>(new Long2ObjectHashMap<>()));

    final AtomicReference<State> state = new AtomicReference<>(State.UNINITIALIZED);

    // TODO (expected): we do not need wait queues here, we can just wait on a signal on a segment while its byte buffer is being allocated
    private final WaitQueue segmentPrepared = newWaitQueue();
    private final WaitQueue allocatorThreadWaitQueue = newWaitQueue();
    private final BooleanSupplier allocatorThreadWaitCondition = () -> (availableSegment == null);

    private final FlusherCallbacks flusherCallbacks;

    final OpOrder readOrder;

    private class FlusherCallbacks implements Flusher.Callbacks
    {
        private final MpscUnboundedArrayQueue<WaitingFor> waitingFor = new MpscUnboundedArrayQueue<>(256);
        private List<WaitingFor> drained = new ArrayList<>();

        @Override
        public void onFsync()
        {
            waitingFor.drain(drained::add);
            List<WaitingFor> remaining = new ArrayList<>();
            for (WaitingFor wait : drained)
            {
                if (flusher.isDurable(wait)) wait.run();
                else remaining.add(wait);
            }
            drained = remaining;
        }

        @Override
        public void onFlushFailed(Throwable cause)
        {
            // TODO (required): panic
        }

        private void submit(RecordPointer pointer, Runnable runnable)
        {
            if (flusher.isDurable(pointer))
                runnable.run();
            else
                waitingFor.add(new WaitingFor(pointer, runnable));
        }
    }

    private static class WaitingFor extends RecordPointer implements Runnable
    {
        private final Runnable onFlush;

        public WaitingFor(RecordPointer pointer, Runnable onFlush)
        {
            super(pointer);
            this.onFlush = onFlush;
        }

        @Override
        public void run()
        {
            onFlush.run();
        }
    }

    public static class Snapshot<K, V> implements AutoCloseable
    {
        private final Segments.ReferencedSegments<K, V> segments;

        private Snapshot(@Nonnull ReferencedSegments<K, V> segments)
        {
            this.segments = segments;
        }

        /**
         * Read all records from all segments in the journal.
         * This method provides an atomic snapshot of all segments and iterates through each one.
         *
         * @param consumer function to consume each record found
         */
        public void readAll(RecordConsumer<K> consumer)
        {
            for (Segment<K, V> segment : segments.allSorted(false))
            {
                segment.readAll(consumer);
            }
        }

        @Override
        public void close()
        {
            segments.close();
        }
    }

    public static class Builder<K, V>
    {
        private final String name;
        private final File directory;
        private final Params params;
        private final KeySupport<K> keySupport;
        private final OpOrder readOrder;

        private ValueSerializer<K, V> valueSerializer = ValueSerializer.none();
        private KeyStats.Factory<K> keyStatsFactory = KeyStats.Factory.noop();
        private SegmentCompactor<K, V> segmentCompactor = SegmentCompactor.noop();

        public Builder(String name, File directory, Params params, KeySupport<K> keySupport, OpOrder readOrder)
        {
            this.name = name;
            this.directory = directory;
            this.params = params;
            this.keySupport = keySupport;
            this.readOrder = readOrder;
        }

        public Journal<K, V> build()
        {
            return new Journal<>(name, directory, params, keySupport, valueSerializer, keyStatsFactory, segmentCompactor, readOrder);
        }

        public Builder<K, V> valueSerializer(ValueSerializer<K, V> valueSerializer)
        {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public Builder<K, V> keyStatsFactory(KeyStats.Factory<K> keyStatsFactory)
        {
            this.keyStatsFactory = keyStatsFactory;
            return this;
        }

        public Builder<K, V> segmentCompactor(SegmentCompactor<K, V> segmentCompactor)
        {
            this.segmentCompactor = segmentCompactor;
            return this;
        }
    }

    public static <K, V> Builder<K, V> builder(String name, File directory, Params params, KeySupport<K> keySupport, OpOrder readOrder)
    {
        return new Builder<>(name, directory, params, keySupport, readOrder);
    }

    public Journal(String name,
                   File directory,
                   Params params,
                   KeySupport<K> keySupport,
                   ValueSerializer<K, V> valueSerializer,
                   KeyStats.Factory<K> keyStatsFactory,
                   SegmentCompactor<K, V> segmentCompactor,
                   OpOrder readOrder)
    {
        this.name = name;
        this.directory = directory;
        this.params = params;

        this.keySupport = keySupport;
        this.valueSerializer = valueSerializer;
        this.keyStatsFactory = keyStatsFactory;
        this.readOrder = readOrder;

        this.metrics = new Metrics<>(name);
        this.flusherCallbacks = new FlusherCallbacks();
        this.flusher = new Flusher<>(this, flusherCallbacks);
        this.compactor = new Compactor<>(this, segmentCompactor);
    }

    public void onDurable(RecordPointer recordPointer, Runnable runnable)
    {
        flusherCallbacks.submit(recordPointer, runnable);
    }

    public void start()
    {
        if (state.get() == State.NORMAL)
            return;

        Invariants.require(state.compareAndSet(State.UNINITIALIZED, State.INITIALIZING),
                              "Unexpected journal state during initialization: %s", state);
        metrics.register(flusher);

        deleteTmpFiles();

        List<Descriptor> descriptors = Descriptor.list(directory);
        // find the largest existing timestamp
        descriptors.sort(null);
        long maxTimestamp = descriptors.isEmpty()
                          ? Long.MIN_VALUE
                          : descriptors.get(descriptors.size() - 1).timestamp;
        nextSegmentId.set(replayLimit = Math.max(currentTimeMillis(), maxTimestamp + 1));

        segments.set(Segments.of(StaticSegment.open(descriptors, keySupport, keyStatsFactory)));
        closer = executorFactory().sequential(name + "-closer");
        releaser = executorFactory().sequential(name + "-releaser");
        allocator = executorFactory().infiniteLoop(name + "-allocator", new AllocateRunnable(), SAFE, NON_DAEMON, SYNCHRONIZED);
        advanceSegment(null);
        Invariants.require(state.compareAndSet(State.INITIALIZING, State.NORMAL),
                              "Unexpected journal state after initialization", state);
        flusher.start();
        compactor.start();

        final int maxSegments = 100;
        if (segments.get().count(Segment::isStatic) > maxSegments)
        {
            while (true)
            {
                WaitQueue.Signal signal = compactor.compacted.register();
                int count = segments.get().count(Segment::isStatic);
                if (count <= maxSegments)
                {
                    signal.cancel();
                    logger.info("Only {} static segments; continuing with startup", count);
                    break;
                }
                else
                {
                    logger.info("Too many ({}) static segments; waiting until some compacted before starting up", count);
                    signal.awaitThrowUncheckedOnInterrupt();
                }
            }
        }
    }

    public Compactor<K, V> compactor()
    {
        return compactor;
    }

    /**
     * Cleans up unfinished component files from previous run (metadata and index)
     */
    private void deleteTmpFiles()
    {
        for (File tmpFile : directory.listUnchecked(Descriptor::isTmpFile))
            tmpFile.delete();
    }

    @Override
    public boolean isTerminated()
    {
        return state.get() == State.TERMINATED;
    }

    @Override
    public void shutdown()
    {
        try
        {
            Invariants.require(state.compareAndSet(State.NORMAL, State.SHUTDOWN),
                                  "Unexpected journal state while trying to shut down", state);
            logger.debug("Shutting down " + allocator + " and awaiting termination");
            allocator.shutdown();
            wakeAllocator(); // Wake allocator to force it into shutdown
            // TODO (expected): why are we awaitingTermination here when we have a separate method for it?
            allocator.awaitTermination(1, TimeUnit.MINUTES);
            segmentPrepared.signalAll(); // Wake up all threads waiting on the new segment
            compactor.shutdown();
            compactor.awaitTermination(1, TimeUnit.MINUTES);
            flusher.shutdown();
            closeAllSegments();
            logger.debug("Shutting down " + releaser + " and " + closer + " and awaiting termination");
            releaser.shutdown();
            closer.shutdown();
            closer.awaitTermination(1, TimeUnit.MINUTES);
            releaser.awaitTermination(1, TimeUnit.MINUTES);
            metrics.deregister();
            Invariants.require(state.compareAndSet(State.SHUTDOWN, State.TERMINATED),
                                  "Unexpected journal state while trying to shut down: %s", state);
        }
        catch (InterruptedException e)
        {
            logger.error("Could not shutdown journal", e);
        }
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        boolean r = true;
        r &= allocator.awaitTermination(timeout, units);
        r &= closer.awaitTermination(timeout, units);
        r &= releaser.awaitTermination(timeout, units);
        return r;
    }

    /**
     * Looks up a record by the provided id.
     * <p/>
     * Looking up an invalidated record may or may not return a record, depending on
     * compaction progress.
     * <p/>
     * In case multiple copies of the record exist in the log (e.g. because of user retries),
     * the first one found will be returned.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @return deserialized record if found, null otherwise
     */
    @SuppressWarnings("unused")
    public V readLast(K id)
    {
        EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();

        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().allSorted(true))
            {
                if (segment.readLast(id, holder))
                {
                    try (DataInputBuffer in = new DataInputBuffer(holder.value, false))
                    {
                        return valueSerializer.deserialize(holder.key, in, holder.userVersion);
                    }
                    catch (IOException e)
                    {
                        // can only throw if serializer is buggy
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return null;
    }

    public boolean readLast(K id, RecordConsumer<K> consumer)
    {
        EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();

        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().allSorted(true))
            {
                if (segment.readLast(id, consumer))
                {
                    return true;
                }
            }
        }
        return false;
    }

    public void readLast(K id, long segmentId, DeserializedRecordConsumer<K, V> consumer)
    {
        Segment<K, V> segment = segments.get().get(segmentId);
        try (OpOrder.Group group = readOrder.start())
        {
            segment.readLast(id, consumer);
        }
    }

    public static <K, V> void readAll(K id, RecordConsumer<K> consumer, OpOrder.Group readGroup, Segments<K, V> segments)
    {
        EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();
        for (Segment<K, V> segment : segments.allSorted(false))
        {
            segment.readAll(id, holder, consumer);
        }
    }

    public void readAll(K id, RecordConsumer<K> consumer, OpOrder.Group readGroup)
    {
        readAll(id, consumer, readGroup, segments.get());
    }

    public void readAll(K id, RecordConsumer<K> consumer)
    {
        try (OpOrder.Group readGroup = readOrder.start())
        {
            readAll(id, consumer, readGroup);
        }
    }

    @SuppressWarnings("unused")
    public List<V> readAll(K id)
    {
        List<V> res = new ArrayList<>(2);
        readAll(id, (segment, position, key, buffer, userVersion) -> {
            try (DataInputBuffer in = new DataInputBuffer(buffer, false))
            {
                res.add(valueSerializer.deserialize(key, in, userVersion));
            }
            catch (IOException e)
            {
                // can only throw if serializer is buggy
                throw new RuntimeException(e);
            }
        });
        return res;
    }

    /**
     * Read all records from all segments in the journal.
     * This method provides an atomic snapshot of all segments and iterates through each one.
     *
     * @param consumer function to consume each record found
     */
    public void readAll(RecordConsumer<K> consumer)
    {
        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().allSorted(false))
            {
                segment.readAll(consumer);
            }
        }
    }

    public Snapshot<K, V> snapshot(Predicate<Segment<K, V>> predicate)
    {
        return new Snapshot<>(selectAndReference(predicate));
    }

    /**
     * Looks up a record by the provided id, if the value satisfies the provided condition.
     * <p/>
     * Looking up an invalidated record may or may not return a record, depending on
     * compaction progress.
     * <p/>
     * In case multiple copies of the record exist in the log (e.g. because of user retries),
     * and more than one of them satisfy the provided condition, the first one found will be returned.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param condition predicate to test the record against
     * @return deserialized record if found, null otherwise
     */
    @SuppressWarnings("unused")
    public V readFirstMatching(K id, Predicate<V> condition)
    {
        EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();

        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().all())
            {
                long[] offsets = segment.index().lookUp(id);
                for (long offsetAndSize : offsets)
                {
                    int offset = Index.readOffset(offsetAndSize);
                    int size = Index.readSize(offsetAndSize);
                    holder.clear();
                    if (segment.read(offset, size, holder))
                    {
                        try (DataInputBuffer in = new DataInputBuffer(holder.value, false))
                        {
                            V record = valueSerializer.deserialize(holder.key, in, segment.descriptor.userVersion);
                            if (condition.test(record))
                                return record;
                        }
                        catch (IOException e)
                        {
                            // can only throw if serializer is buggy
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Looks up a record by the provided id.
     * <p/>
     * Looking up an invalidated record may or may not return a record, depending on
     * compaction progress.
     * <p/>
     * In case multiple copies of the record exist in the log (e.g. because of user retries),
     * only the first found record will be consumed.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param consumer function to consume the raw record (bytes and invalidation set) if found
     * @return true if the record was found, false otherwise
     */
    @SuppressWarnings("unused")
    public static <K, V> boolean readLast(K id, RecordConsumer<K> consumer, OpOrder.Group readOrder, Segments<K, V> segments)
    {
        for (Segment<K, V> segment : segments.allSorted(false))
        {
            if (!segment.index().mayContainId(id))
                continue;

            if (segment.readLast(id, consumer))
                return true;
        }
        return false;
    }

    /**
     * @param id user-provided record id
     * @return record pointer of the last entry with the provided id, or null if not found
     */
    @Nullable
    public RecordPointer lookUpLast(K id)
    {
        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().allSorted(false))
            {
                long[] offsets = segment.index().lookUp(id);
                if (offsets.length != 0)
                {
                    long offsetAndSize = offsets[offsets.length - 1];
                    int offset = Index.readOffset(offsetAndSize);
                    int size = Index.readSize(offsetAndSize);
                    return new RecordPointer(segment.descriptor.timestamp, offset, size);
                }
            }
        }
        return null;
    }

    public int sizeOfRecord(RecordPointer pointer)
    {
        Descriptor descriptor = segments.get().descriptor(pointer.segmentId);
        Invariants.nonNull(descriptor);
        return pointer.length - EntrySerializer.overheadSize(keySupport, descriptor.userVersion);
    }

    public boolean read(RecordPointer pointer, RecordConsumer<K> consumer)
    {
        try (OpOrder.Group group = readOrder.start())
        {
            Segment<K, V> segment = segments.get().get(pointer.segmentId);
            return segment != null && segment.read(pointer, consumer);
        }
    }

    /**
     * Synchronously write a record to the journal.
     * <p/>
     * Blocks until the record has been deemed durable according to the journal flush mode.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param record the record to store
     */
    public RecordPointer blockingWrite(K id, V record)
    {
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            valueSerializer.serialize(id, record, dob, params.userVersion());
            ActiveSegment<K, V>.Allocation alloc = allocate(dob.getLength());
            alloc.writeInternal(id, dob.unsafeGetBufferAndFlip());
            flusher.flushAndAwaitDurable(alloc);
            return alloc.recordPointer();
        }
        catch (IOException e)
        {
            // exception during record serialization into the scratch buffer
            throw new RuntimeException(e);
        }
    }

    /**
     * Asynchronously write a record to the journal. Writes to the journal in the calling thread,
     * but doesn't wait for flush.
     * <p/>
     * Executes the supplied callback on the executor provided once the record has been durably written to disk
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param record the record to store
     */
    public RecordPointer asyncWrite(K id, V record)
    {
        return asyncWrite(id, (out, userVersion) -> valueSerializer.serialize(id, record, out, userVersion));
    }

    public RecordPointer asyncWrite(K id, Writer writer)
    {
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            writer.write(dob, params.userVersion());
            ActiveSegment<K, V>.Allocation alloc = allocate(dob.getLength());
            alloc.write(id, dob.unsafeGetBufferAndFlip());
            return flusher.flush(alloc);
        }
        catch (IOException e)
        {
            // exception during record serialization into the scratch buffer
            throw new RuntimeException(e);
        }
    }

    private ActiveSegment<K, V>.Allocation allocate(int entrySize)
    {
        ActiveSegment<K, V> segment = currentSegment;
        ActiveSegment<K, V>.Allocation alloc;
        while (null == (alloc = segment.allocate(entrySize)))
        {
            if (entrySize >= (params.segmentSize() * 3) / 4)
                throw new IllegalStateException("entrySize " + entrySize + " too large for a segmentSize of " + params.segmentSize());
            // failed to allocate; move to a new segment with enough room
            advanceSegment(segment);
            segment = currentSegment;
        }
        return alloc;
    }

    @VisibleForTesting
    public void advanceSegment()
    {
        advanceSegment(currentSegment);
    }

    /*
     * Segment allocation logic.
     */

    private void advanceSegment(ActiveSegment<K, V> oldSegment)
    {
        while (true)
        {
            synchronized (this)
            {
                // do this in a critical section, so we can maintain the order of
                // segment construction when moving to allocatingFrom/activeSegments
                if (currentSegment != oldSegment)
                    return;

                // if a segment is ready, take it now, otherwise wait for the allocator thread to construct it
                if (availableSegment != null)
                {
                    // success - change allocatingFrom and activeSegments (which must be kept in order) before leaving the critical section
                    addNewActiveSegment(currentSegment = availableSegment);
                    availableSegment = null;
                    break;
                }
            }

            awaitAvailableSegment(oldSegment);
        }

        // signal the allocator thread to prepare a new segment
        wakeAllocator();

        // request that the journal be flushed out-of-band, as we've finished a segment
        flusher.requestExtraFlush();
    }

    private void awaitAvailableSegment(ActiveSegment<K, V> currentActiveSegment)
    {
        do
        {
            WaitQueue.Signal prepared = segmentPrepared.register(metrics.waitingOnSegmentAllocation.time(), Context::stop);
            if (availableSegment == null && currentSegment == currentActiveSegment)
            {
                prepared.awaitThrowUncheckedOnInterrupt();

                // In case we woke up due to shutdown signal or interrupt, check mode
                State state = this.state.get();
                if (state.ordinal() > State.NORMAL.ordinal())
                    throw new IllegalStateException("Can not obtain allocated segment due to shutdown " + state);
            }
            else
                prepared.cancel();
        }
        while (availableSegment == null && currentSegment == currentActiveSegment);
    }

    private void wakeAllocator()
    {
        allocatorThreadWaitQueue.signalAll();
    }

    private void discardAvailableSegment()
    {
        ActiveSegment<K, V> next;
        synchronized (this)
        {
            next = availableSegment;
            availableSegment = null;
        }
        if (next != null)
            next.closeAndDiscard(this);
    }

    private class AllocateRunnable implements Interruptible.Task
    {
        @Override
        public void run(Interruptible.State state) throws InterruptedException
        {
            if (state == NORMAL)
                runNormal();
            else if (state == SHUTTING_DOWN)
                shutDown();
        }

        private void runNormal() throws InterruptedException
        {
            boolean interrupted = false;
            try
            {
                if (availableSegment != null)
                    throw new IllegalStateException("availableSegment is not null");

                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in createSegment()
                synchronized (this)
                {
                    interrupted = Thread.interrupted();
                    availableSegment = createSegment();

                    segmentPrepared.signalAll();
                    Thread.yield();
                }
            }
            catch (JournalWriteError e)
            {
                if (!(e.getCause() instanceof ClosedByInterruptException))
                    throw e;
            }
            catch (Throwable t)
            {
                if (!handleError("Failed allocating journal segments", t))
                {
                    discardAvailableSegment();
                    throw new TerminateException();
                }
                TimeUnit.SECONDS.sleep(1L); // sleep for a second to avoid log spam
            }

            interrupted = interrupted || Thread.interrupted();
            if (!interrupted)
            {
                try
                {
                    // If we offered a segment, wait for it to be taken before reentering the loop.
                    // There could be a new segment in next not offered, but only on failure to discard it while
                    // shutting down-- nothing more can or needs to be done in that case.
                    WaitQueue.waitOnCondition(allocatorThreadWaitCondition, allocatorThreadWaitQueue);
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }

            if (interrupted)
            {
                discardAvailableSegment();
                throw new InterruptedException();
            }
        }

        private void shutDown() throws InterruptedException
        {
            try
            {
                // if shutdown() started and finished during segment creation, we'll be left with a
                // segment that no one will consume; discard it
                discardAvailableSegment();
            }
            catch (Throwable t)
            {
                handleError("Failed shutting down segment allocator", t);
                throw new TerminateException();
            }
        }
    }

    private ActiveSegment<K, V> createSegment()
    {
        Descriptor descriptor = Descriptor.create(directory, nextSegmentId.getAndIncrement(), params.userVersion());
        return ActiveSegment.create(descriptor, params, keySupport, keyStatsFactory);
    }

    private void closeAllSegments()
    {
        Segments<K, V> segments = swapSegments(ignore -> Segments.none());

        for (Segment<K, V> segment : segments.all())
        {
            if (segment.isActive())
                ((ActiveSegment<K, V>) segment).closeAndIfEmptyDiscard(this);
            else
                segment.close(this);
        }
    }

    @SuppressWarnings("unused")
    ReferencedSegments<K, V> selectAndReference(Predicate<Segment<K,V>> selector)
    {
        while (true)
        {
            ReferencedSegments<K, V> referenced = segments().selectAndReference(selector);
            if (null != referenced)
                return referenced;
        }
    }

    public Segments<K, V> segments()
    {
        return segments.get();
    }

    private Segments<K, V> swapSegments(Function<Segments<K, V>, Segments<K, V>> transformation)
    {
        Segments<K, V> currentSegments, newSegments;
        do
        {
            currentSegments = segments();
            newSegments = transformation.apply(currentSegments);
        }
        while (!segments.compareAndSet(currentSegments, newSegments));
        return currentSegments;
    }

    private void addNewActiveSegment(ActiveSegment<K, V> activeSegment)
    {
        swapSegments(current -> current.withNewActiveSegment(activeSegment));
    }

    private void removeEmptySegment(ActiveSegment<K, V> activeSegment)
    {
        swapSegments(current -> current.withoutEmptySegment(activeSegment));
    }

    private void removeStaticSegments(Collection<StaticSegment<K, V>> staticSegments)
    {
        swapSegments(current -> current.withoutStaticSegments(staticSegments));
    }

    private void replaceCompletedSegment(ActiveSegment<K, V> activeSegment, StaticSegment<K, V> staticSegment)
    {
        swapSegments(current -> current.withCompletedSegment(activeSegment, staticSegment));
    }

    void replaceCompactedSegments(Collection<StaticSegment<K, V>> oldSegments, Collection<StaticSegment<K, V>> compactedSegments)
    {
        swapSegments(current -> current.withCompactedSegments(oldSegments, compactedSegments));
    }

    void selectSegmentToFlush(Collection<ActiveSegment<K, V>> into)
    {
        segments().selectActive(currentSegment.descriptor.timestamp, into);
    }

    ActiveSegment<K, V> oldestActiveSegment()
    {
        ActiveSegment<K, V> current = currentSegment;
        if (current == null)
            return null;

        ActiveSegment<K, V> oldest = segments().oldestActive();
        if (oldest == null || oldest.descriptor.timestamp > current.descriptor.timestamp)
            return current;

        return oldest;
    }

    public List<Segment<K, V>> getSegments(long lowerBound, long upperBound)
    {
        List<Segment<K, V>> res = new ArrayList<>();
        segments().select(lowerBound, upperBound, res);
        return res;
    }

    public ActiveSegment<K, V> currentActiveSegment()
    {
        return currentSegment;
    }

    @Nullable protected Segment<K, V> getSegment(long timestamp)
    {
        return segments().get(timestamp);
    }

    ActiveSegment<K, V> getActiveSegment(long timestamp)
    {
        // we can race with segment addition to the segments() collection, with a new segment appearing in currentSegment first
        // since we are most likely to be requesting the currentSegment anyway, we resolve this case by checking currentSegment first
        // and resort to the segments() collection only if we do not match
        ActiveSegment<K, V> currentSegment = this.currentSegment;
        if (currentSegment == null)
            throw new IllegalArgumentException("Requested an active segment with timestamp " + timestamp + " but there is no currently active segment");
        long currentSegmentTimestamp = currentSegment.descriptor.timestamp;
        if (timestamp == currentSegmentTimestamp)
        {
            return currentSegment;
        }
        else if (timestamp > currentSegmentTimestamp)
        {
            throw new IllegalArgumentException("Requested a newer timestamp " + timestamp + " than the current active segment " + currentSegmentTimestamp);
        }
        else
        {
            Segment<K, V> segment = segments().get(timestamp);
            Invariants.require(segment != null, "Segment %d expected to be found, but neither current segment %d nor in active segments", timestamp, currentSegmentTimestamp);
            if (segment == null)
                throw new IllegalArgumentException("Request the active segment " + timestamp + " but this segment does not exist");
            if (!segment.isActive())
                throw new IllegalArgumentException(String.format("Request the active segment %d but this segment is not active: %s", timestamp, segment));
            return segment.asActive();
        }
    }

    /**
     * Take care of a finished active segment:
     * 1. discard tail
     * 2. flush to disk
     * 3. persist index and metadata
     * 4. open the segment as static
     * 5. replace the finished active segment with the opened static one in Segments view
     * 6. release the Ref so the active segment will be cleaned up by its Tidy instance
     */
    private class CloseActiveSegmentRunnable implements Runnable
    {
        private final ActiveSegment<K, V> activeSegment;
        private final Runnable onDone;

        CloseActiveSegmentRunnable(ActiveSegment<K, V> activeSegment, @Nullable Runnable onDone)
        {
            this.activeSegment = activeSegment;
            this.onDone = onDone;
        }

        @Override
        public void run()
        {
            activeSegment.discardUnusedTail();
            activeSegment.updateWrittenTo();
            activeSegment.fsync();
            activeSegment.persistComponents();
            replaceCompletedSegment(activeSegment, StaticSegment.open(activeSegment.descriptor, keySupport, keyStatsFactory));
            activeSegment.release(Journal.this);
            if (onDone != null) onDone.run();
        }
    }

    protected void closeActiveSegmentAndOpenAsStatic(ActiveSegment<K, V> activeSegment)
    {
        closeActiveSegmentAndOpenAsStatic(activeSegment, null);
    }

    protected void closeActiveSegmentAndOpenAsStatic(ActiveSegment<K, V> activeSegment, @Nullable Runnable onDone)
    {
        if (activeSegment.isEmpty())
        {
            removeEmptySegment(activeSegment);
            activeSegment.closeAndDiscard(this);
            return;
        }

        closer.execute(new CloseActiveSegmentRunnable(activeSegment, onDone));
    }

    public int dropStaticSegments(Predicate<StaticSegment<K, V>> dropIf)
    {
        Set<StaticSegment<K, V>> toDrop = new HashSet<>();
        segments().selectStatic(dropIf, toDrop);
        if (toDrop.isEmpty())
            return 0;
        removeStaticSegments(toDrop);
        for (StaticSegment<K, V> segment : toDrop)
            segment.discard(this);
        return toDrop.size();
    }

    /*
     * Error handling
     */

    /**
     * @return true if the invoking thread should continue, or false if it should terminate itself
     */
    public boolean handleError(String message, Throwable t)
    {
        Params.FailurePolicy policy = params.failurePolicy();
        JVMStabilityInspector.inspectJournalThrowable(t, name, policy);

        switch (policy)
        {
            default:
                throw new AssertionError(policy);
            case DIE:
            case STOP:
                StorageService.instance.stopTransports();
                //$FALL-THROUGH$
            case STOP_JOURNAL:
                message = format("%s. Journal %s failure policy is %s; terminating thread.", message, name, policy);
                logger.error(maybeAddDiskSpaceContext(message), t);
                return false;
            case ALLOW_UNSAFE_STARTUP:
            case IGNORE:
                message = format("%s. Journal %s failure policy is %s; ignoring excepton.", message, name, policy);
                logger.error(maybeAddDiskSpaceContext(message), t);
                return true;
        }
    }

    /**
     * Add additional information to the error message if the journal directory does not have enough free space.
     *
     * @param message the original error message
     * @return the message with additional information if possible
     */
    private String maybeAddDiskSpaceContext(String message)
    {
        long availableDiskSpace = PathUtils.tryGetSpace(directory.toPath(), FileStore::getTotalSpace);
        int segmentSize = params.segmentSize();

        if (availableDiskSpace >= segmentSize)
            return message;

        return format("%s. %d bytes required for next journal segment but only %d bytes available. " +
                      "Check %s to see if not enough free space is the reason for this error.",
                      message, segmentSize, availableDiskSpace, directory);
    }

    public interface Writer
    {
        void write(DataOutputPlus out, int userVersion) throws IOException;
    }

    /**
     * segment iterator iterates all keys in order.
     */
    public SegmentKeyIterator segmentKeyIterator(K min, K max, Predicate<Segment<K, V>> include)
    {
        return new SegmentKeyIterator(min, max, include);
    }

    /**
     * Static segment iterator iterates all keys in selected segments in order.
     */
    public SegmentKeyIterator staticSegmentKeyIterator(Predicate<Segment<K, V>> predicate)
    {
        return new SegmentKeyIterator(null, null, predicate.and(s -> s.isStatic()));
    }

    /**
     * List of key and a list of segment descriptors referencing this key
     */
    public static class KeyRefs<K>
    {
        long[] segments;
        K key;
        int size;

        public KeyRefs(K key)
        {
            this.key = key;
        }

        private KeyRefs(int maxSize)
        {
            this.segments = new long[maxSize];
        }

        public void segments(LongConsumer consumer)
        {
            for (int i = 0; i < size; i++)
                consumer.accept(segments[i]);
        }

        public long lastSegment()
        {
            return segments[segments.length - 1];
        }

        public long[] copyOfSegments()
        {
            return segments == null ? new long[0] : Arrays.copyOf(segments, size);
        }

        public K key()
        {
            return key;
        }

        public void ensureSorted()
        {
            Arrays.sort(segments);
        }

        private void add(K key, long segment)
        {
            Invariants.require(this.key == null || key.equals(this.key));
            this.key = key;
            segments[size++] = segment;
        }

        private void reset()
        {
            key = null;
            size = 0;
            Arrays.fill(segments, 0);
        }

        @Override
        public String toString()
        {
            return "KeyRefs{" +
                   "segments=" + Arrays.toString(segments) +
                   ", key=" + key +
                   ", size=" + size +
                   '}';
        }
    }

    public class SegmentKeyIterator implements CloseableIterator<KeyRefs<K>>
    {
        private final ReferencedSegments<K, V> segments;
        private final MergeIterator<Head, KeyRefs<K>> iterator;

        public SegmentKeyIterator(K min, K max)
        {
            this(min, max, s -> true);
        }

        public SegmentKeyIterator(K min, K max, Predicate<Segment<K, V>> include)
        {
            this.segments = selectAndReference(s -> include.test(s) && !s.isEmpty()
                                                    && (min == null || keySupport.compare(s.index().lastId(), min) >= 0)
                                                    && (max == null || keySupport.compare(s.index().firstId(), max) <= 0));

            List<Iterator<Head>> iterators = new ArrayList<>(segments.count());

            for (Segment<K, V> segment : segments.allSorted(true))
            {
                if (segment.isStatic())
                {
                    final StaticSegment<K, V> staticSegment = (StaticSegment<K, V>) segment;
                    final OnDiskIndex<K>.IndexReader iter = staticSegment.index().reader();
                    if (min != null) iter.seek(min);
                    if (max != null) iter.seekEnd(max);
                    if (iter.hasNext())
                        iterators.add(keyIterator(segment.descriptor.timestamp, iter));
                }
                else
                {
                    final ActiveSegment<K, V> activeSegment = (ActiveSegment<K, V>) segment;
                    final Iterator<K> iter = activeSegment.index().keyIterator(min, max);
                    if (iter.hasNext())
                        iterators.add(keyIterator(segment.descriptor.timestamp, iter));
                }
            }

            this.iterator = MergeIterator.get(iterators,
                                              (r1, r2) -> keySupport.compare(r1.key, r2.key),
                                              new MergeIterator.Reducer<>()
                                              {
                                                  final KeyRefs<K> ret = new KeyRefs<>(segments.count());

                                                  @Override
                                                  public void reduce(int idx, Head head)
                                                  {
                                                      ret.add(head.key, head.segment);
                                                  }

                                                  @Override
                                                  protected KeyRefs<K> getReduced()
                                                  {
                                                      ret.ensureSorted();
                                                      return ret;
                                                  }

                                                  @Override
                                                  protected void onKeyChange()
                                                  {
                                                      ret.reset();
                                                      super.onKeyChange();
                                                  }
                                              });
        }

        private Iterator<Head> keyIterator(long segment, Iterator<K> iter)
        {
            final Head head = new Head(segment);
            return new AbstractIterator<>()
            {
                @Override
                protected Head computeNext()
                {
                    if (!iter.hasNext())
                        return endOfData();

                    K next = iter.next();
                    while (next.equals(head.key))
                    {
                        if (!iter.hasNext())
                            return endOfData();

                        next = iter.next();
                    }

                    Invariants.require(!next.equals(head.key),
                                       "%s == %s", next, head.key);
                    head.key = next;
                    return head;
                }
            };
        }

        @Override
        public void close()
        {
            segments.close();
        }

        public KeyRefs<K> peek()
        {
            if (iterator.hasNext())
                return iterator.peek();
            return null;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public KeyRefs<K> next()
        {
            return iterator.next();
        }

        class Head
        {
            final long segment;
            K key;
            Head(long segment) { this.segment = segment; }
        }
    }

    enum State
    {
        UNINITIALIZED,
        INITIALIZING,
        NORMAL,
        SHUTDOWN,
        TERMINATED
    }

    /*
     * Test helpers
     */

    @VisibleForTesting
    public void unsafeConsumeBytesForTesting(int entrySize, Consumer<ByteBuffer> corrupt)
    {
        // TODO (require): Find a better way to test unwritten allocations and/or corruption
        allocate(entrySize).consumeBufferUnsafe(corrupt);
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        ActiveSegment<?, ?> discarding = currentSegment;
        if (!discarding.isEmpty()) // if there is no data in the segment then ignore it
        {
            closeCurrentSegmentForTestingIfNonEmpty();
            //TODO (desired): wait for the ActiveSegment to get released, else can see weird race conditions;
            // this thread will see the static segmenet and will release it (which will delete the file),
            // and the sync thread will then try to release and will fail as the file no longer exists...
            while (discarding.selfRef().globalCount() > 0) {}
        }

        Segments<K, V> statics = swapSegments(s -> s.select(Segment::isActive)).select(Segment::isStatic);
        for (Segment<K, V> segment : statics.all())
            ((StaticSegment) segment).discard(this);
    }

    @VisibleForTesting
    public void runCompactorForTesting()
    {
        compactor.run();
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTestingIfNonEmpty()
    {
        ActiveSegment<K, V> segment = currentSegment;
        if (segment.isEmpty())
            return;
        advanceSegment(segment);
        while (!segments().isSwitched(segment))
        {
            LockSupport.parkNanos(1000);
        }
    }

    @VisibleForTesting
    public void clearNeedsReplayForTesting()
    {
        Set<StaticSegment<K, V>> toReset = new HashSet<>();
        segments().selectStatic(toReset);
        toReset.forEach(s -> s.metadata().clearNeedsReplay());
    }

    @VisibleForTesting
    public int countStaticSegmentsForTesting()
    {
        return segments.get().count(Segment::isStatic);
    }
}
