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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.zip.CRC32;

import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;

import org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Interruptible.TerminateException;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.journal.Segments.ReferencedSegments;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Crc;
import org.apache.cassandra.utils.ExecutorUtils;
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
import static org.apache.cassandra.journal.Params.RecoverableCrcFailurePolicy.FAIL;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

/**
 * A generic append-only journal with some special features:
 * <p><ul>
 * <li>Records can be looked up by key
 * <li>Invalidated records get purged during segment compaction
 * </ul><p>
 *
 * Type parameters:
 * @param <V> the type of records stored in the journal
 * @param <K> the type of keys used to address the records;
              must be fixed-size and byte-order comparable
 */
@Simulate(with=MONITORS)
public class Journal<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);

    final String name;
    final File directory;
    final Params params;

    final KeySupport<K> keySupport;
    final ValueSerializer<K, V> valueSerializer;

    final Metrics<K, V> metrics;

    final Flusher<K, V> flusher;
    final Compactor<K, V> compactor;
    final AllocateRunnable allocateRunnable = new AllocateRunnable();
    Interruptible allocator;
    SequentialExecutorPlus closer, releaser;

    final AtomicLong nextSegmentId = new AtomicLong();

    private volatile ActiveSegment<K, V> currentSegment = null;

    // segment that is ready to be used; allocator thread fills this and blocks until consumed
    private volatile ActiveSegment<K, V> availableSegment = null;

    private final AtomicReference<Segments<K, V>> segments = new AtomicReference<>();

    private volatile State state = State.UNINITIALIZED;
    private static final AtomicReferenceFieldUpdater<Journal, State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(Journal.class, State.class, "state");

    private final WaitQueue segmentPrepared = newWaitQueue();
    private volatile Thread waitingAllocatorThread;

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

        public void run()
        {
            onFlush.run();
        }
    }

    public Journal(String name,
                   File directory,
                   Params params,
                   KeySupport<K> keySupport,
                   ValueSerializer<K, V> valueSerializer,
                   SegmentCompactor<K, V> segmentCompactor,
                   OpOrder readOrder)
    {
        this.name = name;
        this.directory = directory;
        this.params = params;

        this.keySupport = keySupport;
        this.valueSerializer = valueSerializer;
        this.readOrder = readOrder;

        this.metrics = new Metrics<>(name);
        this.flusherCallbacks = new FlusherCallbacks();
        this.flusher = new Flusher<>(this, flusherCallbacks);
        this.compactor = new Compactor<>(this, segmentCompactor);
    }

    public long peekSegmentId()
    {
        return nextSegmentId.get();
    }

    public void onDurable(RecordPointer recordPointer, Runnable runnable)
    {
        flusherCallbacks.submit(recordPointer, runnable);
    }

    public void open()
    {
        Invariants.require(stateUpdater.compareAndSet(this, State.UNINITIALIZED, State.OPENING),
                           "Unexpected journal state before opening", state);

        deleteTmpFiles();
        List<Descriptor> descriptors = Descriptor.list(directory);
        segments.set(Segments.of(StaticSegment.open(descriptors, keySupport, params.crcFailureOnRebuildPolicy())));

        Invariants.require(stateUpdater.compareAndSet(this, State.OPENING, State.OPEN_READABLE),
                           "Unexpected journal state once opened", state);
    }

    public void start(long maxTableDescriptor)
    {
        if (state == State.UNINITIALIZED)
            open();

        Invariants.require(stateUpdater.compareAndSet(this, State.OPEN_READABLE, State.STARTING),
                              "Unexpected journal state before starting", state);

        nextSegmentId.set(Math.max(currentTimeMillis(), Math.max(maxDescriptor(), maxTableDescriptor) + 1));

        closer = executorFactory().sequential(name + "-closer");
        releaser = executorFactory().sequential(name + "-releaser");
        allocator = executorFactory().infiniteLoop(name + "-allocator", allocateRunnable, SAFE, NON_DAEMON, SYNCHRONIZED);

        // we use these metrics when advancing segments, so must register first
        metrics.register(flusher);
        advanceSegment(null);

        flusher.start();
        compactor.start();

        Invariants.require(stateUpdater.compareAndSet(this, State.STARTING, State.WRITEABLE),
                           "Unexpected journal state once started", state);

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

    public long maxDescriptor()
    {
        List<Segment<K, V>> existingSegments = segments.get().allSorted(false);
        return existingSegments.isEmpty() ? 0 : existingSegments.get(0).descriptor.timestamp;
    }

    public State getState()
    {
        return state;
    }

    public boolean isReadable()
    {
        State state = this.state;
        return state.compareTo(State.OPEN_READABLE) >= 0 && state.compareTo(State.STOPPED_READABLE) <= 0;
    }

    // package-private: the compactor checks this under the compaction lock before it starts a compaction
    boolean isNotStopped()
    {
        State state = this.state;
        return state.compareTo(State.STARTING) >= 0 && state.compareTo(State.STOPPING) <= 0;
    }

    @VisibleForTesting
    public void runCompactorForTesting()
    {
        compactor.run();
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

    public boolean hasBeenOpened()
    {
        return state.compareTo(State.OPEN_READABLE) >= 0;
    }

    public boolean isTerminated()
    {
        return state == State.STOPPED_READABLE;
    }

    public void fsync()
    {
        ActiveSegment<K, V> active = currentSegment;
        int position = active.writtenToAtLeast();
        flusher.requestExtraFlush();
        flusher.awaitFsync(active, position);
    }

    // return the last segment that was written to
    public Descriptor stop()
    {
        logger.info("Stopping journal");
        logger.debug("Shutting down " + allocator);
        boolean stop;
        synchronized (allocateRunnable)
        {
            // we synchronize on allocateRunnable to ensure it witnesses this change before the next attempt to allocate a segment
            stop = stateUpdater.compareAndSet(this, State.WRITEABLE, State.STOPPING);
        }
        Invariants.require(stop, "Unexpected journal state before stopping", state);

        // ensure prompt shutdown, though the above state change suffices semantically
        allocator.shutdown();
        wakeAllocator();
        discardAvailableSegment();
        segmentPrepared.signalAll(); // Wake up all threads waiting on the new segment

        compactor.shutdown();
        compactor.awaitQuiescence();

        currentSegment.discardUnusedTail();
        flusher.requestExtraFlush();

        Descriptor lastSegment = finaliseSegments(); // this flushes any pending writes

        flusher.shutdown();
        logger.debug("Shutting down " + releaser + " and " + closer);
        releaser.shutdown();
        closer.shutdown();
        metrics.deregister();
        Invariants.require(stateUpdater.compareAndSet(this, State.STOPPING, State.STOPPED_READABLE),
                           "Unexpected journal state after stopping", state);
        return lastSegment;
    }

    public void close()
    {
        logger.info("Closing journal");
        stateUpdater.compareAndSet(this, State.STOPPED_READABLE, State.CLOSING);
        compactor.withoutCompaction(this::closeAllSegments);
        stateUpdater.compareAndSet(this, State.CLOSING, State.CLOSED);
    }

    public void awaitTerminationUntil(long deadlineNanos) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.awaitTerminationUntil(deadlineNanos, Arrays.asList(allocator, compactor, closer, releaser));
        ExecutorUtils.awaitTerminationUntil(deadlineNanos, flusher.executors());
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
            if (segment.readLast(id, consumer))
                return true;
        }
        return false;
    }

    /**
     * Synchronously write a record to the journal.
     * <p/>
     * Blocks until the record has been deemed durable according to the journal flush mode.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param record the record to store
     */
    public void blockingWrite(K id, V record)
    {
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            valueSerializer.serialize(id, record, dob, params.userVersion());
            ActiveSegment<K, V>.Allocation alloc = allocate(dob.getLength());
            alloc.writeInternal(id, dob.unsafeGetBufferAndFlip());
            flusher.flushAndAwaitDurable(alloc);
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

    // TODO (require): Find a better way to test unwritten allocations and/or corruption
    @VisibleForTesting
    public void unsafeConsumeBytesForTesting(int entrySize, Consumer<ByteBuffer> corrupt)
    {
        allocate(entrySize).consumeBufferUnsafe(corrupt);
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
                // In case we woke up due to shutdown signal or interrupt, check mode
                State state = this.state;
                if (state.ordinal() > State.WRITEABLE.ordinal())
                    throw new IllegalStateException("Can not obtain allocated segment due to shutdown " + state);

                prepared.awaitThrowUncheckedOnInterrupt();
            }
            else
                prepared.cancel();
        }
        while (availableSegment == null && currentSegment == currentActiveSegment);
    }

    private void wakeAllocator()
    {
        Thread wake = waitingAllocatorThread;
        if (wake != null)
            LockSupport.unpark(wake);
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
        }

        private void runNormal() throws InterruptedException
        {
            try
            {
                if (availableSegment != null)
                    throw new IllegalStateException("availableSegment is not null");

                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in createSegment()
                boolean interrupted;
                synchronized (this)
                {
                    if (state.compareTo(State.STOPPING) >= 0)
                        throw new TerminateException();

                    interrupted = Thread.interrupted();
                    availableSegment = createSegment();
                }

                segmentPrepared.signalAll();
                if (interrupted) throw new InterruptedException();
                else Thread.yield();
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

            // If we offered a segment, wait for it to be taken before reentering the loop.
            // There could be a new segment in next not offered, but only on failure to discard it while
            // shutting down-- nothing more can or needs to be done in that case.
            if (availableSegment != null)
            {
                waitingAllocatorThread = Thread.currentThread();
                boolean interrupted = false;
                while (availableSegment != null && !(interrupted = Thread.interrupted()))
                    LockSupport.park();
                waitingAllocatorThread = null;
                if (interrupted)
                    throw new InterruptedException();
            }
        }
    }

    private ActiveSegment<K, V> createSegment()
    {
        Descriptor descriptor = Descriptor.create(directory, nextSegmentId.getAndIncrement(), params.userVersion());
        return ActiveSegment.create(descriptor, params, keySupport);
    }

    private void closeAllSegments()
    {
        Segments<K, V> segments = swapSegments(ignore -> Segments.none());

        List<Segment<K, V>> all = segments.allSorted(false);
        for (Segment<K, V> segment : all)
        {
            if (segment.isActive())
                ((ActiveSegment<K, V>) segment).closeAndIfEmptyDiscard(this);
            else
                segment.close(this);
        }
    }

    private Descriptor finaliseSegments()
    {
        while (true)
        {
            ActiveSegment<K, V> oldestActive = oldestActiveSegment();
            oldestActive.discardUnusedTail();
            flusher.awaitFsync(oldestActive, oldestActive.writtenToAtLeast());
            if (oldestActive == currentSegment)
                break;
        }

        currentSegment.persistComponents();
        List<Segment<K, V>> all = segments().allSorted(false);
        if (all.isEmpty())
            return null;
        return all.get(0).descriptor;
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
        Invariants.require(isNotStopped());
        swapSegments(current -> current.withNewActiveSegment(activeSegment));
    }

    private void removeEmptySegment(ActiveSegment<K, V> activeSegment)
    {
        Invariants.require(isNotStopped());
        swapSegments(current -> current.withoutEmptySegment(activeSegment));
    }

    private void replaceCompletedSegment(ActiveSegment<K, V> activeSegment, StaticSegment<K, V> staticSegment)
    {
        Invariants.require(isNotStopped());
        swapSegments(current -> current.withCompletedSegment(activeSegment, staticSegment));
    }

    void replaceCompactedSegments(Collection<StaticSegment<K, V>> oldSegments, Collection<StaticSegment<K, V>> compactedSegments)
    {
        Invariants.require(isNotStopped());
        swapSegments(current -> current.withCompactedSegments(oldSegments, compactedSegments));
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

    public ActiveSegment<K, V> currentActiveSegment()
    {
        return currentSegment;
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

        CloseActiveSegmentRunnable(ActiveSegment<K, V> activeSegment)
        {
            this.activeSegment = activeSegment;
        }

        @Override
        public void run()
        {
            activeSegment.discardUnusedTail();
            activeSegment.updateWrittenTo();
            activeSegment.fsync();
            activeSegment.persistComponents();
            replaceCompletedSegment(activeSegment, StaticSegment.open(activeSegment.descriptor, keySupport, FAIL));
            activeSegment.release(Journal.this);
        }
    }

    void closeActiveSegmentAndOpenAsStatic(ActiveSegment<K, V> activeSegment)
    {
        if (activeSegment.isEmpty())
        {
            removeEmptySegment(activeSegment);
            activeSegment.closeAndDiscard(this);
            return;
        }

        closer.execute(new CloseActiveSegmentRunnable(activeSegment));
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

    /*
     * Static helper methods used by journal components
     */

    static void validateCRC(CRC32 crc, int readCRC) throws Crc.InvalidCrc
    {
        if (readCRC != (int)crc.getValue())
            throw new Crc.InvalidCrc(readCRC, (int)crc.getValue());
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

    @VisibleForTesting
    public void truncateForTesting()
    {
        Invariants.require(isNotStopped());
        ActiveSegment<?, ?> discarding = currentSegment;
        if (!discarding.isEmpty()) // if there is no data in the segement then ignore it
        {
            closeCurrentSegmentForTestingIfNonEmpty();
            //TODO (desired): wait for the ActiveSegment to get released, else can see weird race conditions;
            // this thread will see the static segmenet and will release it (which will delete the file),
            // and the sync thread will then try to release and will fail as the file no longer exists...
            while (discarding.selfRef().globalCount() > 0) {}
        }

        // a compaction reads mmapped segments, and a discarded segment is unmapped (CASSANDRA-21412)
        compactor.withoutCompaction(() -> {
            Segments<K, V> statics = swapSegments(s -> s.select(Segment::isActive)).select(Segment::isStatic);
            for (Segment<K, V> segment : statics.all())
                ((StaticSegment<K, V>) segment).discard(this);
        });
    }

    public interface Writer
    {
        void write(DataOutputPlus out, int userVersion) throws IOException;
    }

    /**
     * segment iterator iterates all keys in order.
     */
    public SegmentKeyIterator segmentKeyIterator(K min, K max, Predicate<Segment<?, ?>> include)
    {
        return new SegmentKeyIterator(min, max, include);
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

        public SegmentKeyIterator(K min, K max, Predicate<Segment<?, ?>> include)
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

    public enum State
    {
        UNINITIALIZED,
        OPENING,
        OPEN_READABLE,
        STARTING,
        WRITEABLE,
        STOPPING,
        STOPPED_READABLE,
        CLOSING,
        CLOSED
    }
}
