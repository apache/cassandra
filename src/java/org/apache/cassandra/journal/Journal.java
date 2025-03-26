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
import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
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
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Crc;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.LazyToString;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Promise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.journal.ActiveSegment.RecordPointer;
import static org.apache.cassandra.journal.ActiveSegment.create;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;

/**
 * A generic append-only journal with some special features:
 * <p><ul>
 * <li>Records can be looked up by key
 * <li>Invalidated records get purged during segment compaction
 * </ul><p>
 *
 * Segment lifecycle:
 *
 * ... --+------------------------/--------------------/------ ...
 *       |  fsynced allocation  / current allocation /
 * ... --+--------------------/--------------------/---------- ...
 *                                     |
 *                                     |
 *                         +-----------+-----------+
 *                         |    WriteAllocation    |   <-- Owned by writer thread
 *                         +-----------+-----------+
 *                                     |
 *                                     | <-- When fully written, notify Flusher about this allocation
 *                                     |
 *                         +-----------+-----------+
 *                         |        Flusher        |  <-- Collects consecutive write allocations and fsyncs file channel
 *                         +-----------+-----------+
 *                                     |
 *                                     | <-- When fsynced, notify writer thread about persistence
 *                                     |
 *                         +-----------+-----------+
 *                         |     Writer Thread     |  <-- Executes write callbacks
 *                         +-----------+-----------+
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

    final Metrics<K, V> metrics;

    final Compactor<K, V> compactor;

    final AtomicLong nextSegmentId = new AtomicLong();

    private volatile ActiveSegment<K, V> currentSegment = null;
    private volatile Promise<ActiveSegment<K, V>> availableSegment = null;

    // segment that is ready to be used; allocator thread fills this and blocks until consumed
    private final Allocator allocator = new Allocator();

    private final AtomicReference<Segments<K, V>> segments = new AtomicReference<>();

    final AtomicReference<State> state = new AtomicReference<>(State.UNINITIALIZED);

    private final Flusher flusher;
    final SequentialExecutorPlus maintenance;
    final SequentialExecutorPlus tidy;

    final OpOrder readOrder = new OpOrder();

    public Journal(String name,
                   File directory,
                   Params params,
                   KeySupport<K> keySupport,
                   ValueSerializer<K, V> valueSerializer,
                   SegmentCompactor<K, V> segmentCompactor)
    {
        this.name = name;
        this.directory = directory;
        this.params = params;

        this.keySupport = keySupport;
        this.valueSerializer = valueSerializer;

        this.metrics = new Metrics<>(name);
        this.compactor = new Compactor<>(this, segmentCompactor);
        this.maintenance = executorFactory().sequential(name + "-maintenance");
        this.tidy = executorFactory().sequential(name + "-tidy");
        this.flusher = new Flusher(name, params, this);
    }

    public void start()
    {
        Invariants.require(state.compareAndSet(State.UNINITIALIZED, State.INITIALIZING),
                              "Unexpected journal state during initialization", state);
        metrics.register(flusher);

        deleteTmpFiles();

        List<Descriptor> descriptors = Descriptor.list(directory);
        // find the largest existing timestamp
        descriptors.sort(null);
        long maxTimestamp = descriptors.isEmpty()
                          ? Long.MIN_VALUE
                          : descriptors.get(descriptors.size() - 1).timestamp;
        nextSegmentId.set(Math.max(currentTimeMillis(), maxTimestamp + 1));
        segments.set(Segments.of(StaticSegment.open(descriptors, keySupport)));

        requestSegmentAllocation();
        advanceSegment(null);
        Invariants.require(state.compareAndSet(State.INITIALIZING, State.NORMAL),
                              "Unexpected journal state after initialization", state);
        compactor.start();
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

    @Override
    public boolean isTerminated()
    {
        return state.get() == State.TERMINATED;
    }

    public void shutdown()
    {
        Invariants.require(state.compareAndSet(State.NORMAL, State.SHUTDOWN),
                           "Unexpected journal state while trying to shut down", state);
        compactor.shutdown();
        flusher.shutdown();
        metrics.deregister();
        maintenance.shutdown();
        allocator.shutdown();
        Invariants.require(state.compareAndSet(State.SHUTDOWN, State.TERMINATED),
                           "Unexpected journal state while trying to shut down", state);
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
        r &= compactor.awaitTermination(1, TimeUnit.MINUTES);
        r &= flusher.awaitTermination(timeout, units);
        flusher.requestExtraFlush();
        r &= maintenance.awaitTermination(timeout, units);

        // We can only close segments after we have fully shut down maintenance thread, and we can not shut down tidy earlier since closing requires tidy pool
        closeAllSegments();
        tidy.shutdown();
        r &= tidy.awaitTermination(timeout, units);
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

    public void readAll(K id, RecordConsumer<K> consumer)
    {
        EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();
        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().allSorted(false))
            {
                segment.readAll(id, holder, consumer);
            }
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
    public boolean readLast(K id, RecordConsumer<K> consumer)
    {
        try (OpOrder.Group group = readOrder.start())
        {
            for (Segment<K, V> segment : segments.get().allSorted(false))
            {
                if (!segment.index().mayContainId(id))
                    continue;

                if (segment.readLast(id, consumer))
                    return true;
            }
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
        asyncWrite(id, record).awaitUninterruptibly();
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
        return writeInternal(id, writer);
    }

    private ActiveSegment<K, V>.Allocation writeInternal(K id, Writer writer)
    {
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            writer.write(dob, params.userVersion());
            ActiveSegment<K, V>.Allocation alloc = allocate(dob.getLength());
            alloc.write(id, dob.unsafeGetBufferAndFlip());
            flusher.flush(alloc);
            return alloc;
        }
        catch (IOException e)
        {
            // exception during record serialization into the scratch buffer
            throw new UncheckedIOException(e);
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

    /*
     * Segment allocation logic.
     */

    private void requestSegmentAllocation()
    {
        Promise<ActiveSegment<K, V>> promise = new AsyncPromise<>();
        synchronized (this)
        {
            if (availableSegment == null)
            {
                availableSegment = promise;
                promise = null;
            }
        }
        if (promise == null)
            maintenance.execute(allocator);
        else
            promise.cancel(true);
    }

    private void advanceSegment(ActiveSegment<K, V> oldSegment)
    {
        while (true)
        {
            Promise<ActiveSegment<K, V>> waitOn = null;
            synchronized (this)
            {
                // do this in a critical section, so we can maintain the order of
                // segment construction when moving to allocatingFrom/activeSegments
                if (currentSegment != oldSegment)
                    return;

                // if a segment is ready, take it now, otherwise wait for the allocator thread to construct it
                if (availableSegment != null)
                {
                    if (availableSegment.isDone())
                    {
                        Invariants.require(availableSegment.isSuccess());
                        // success - first add the next segment to segments, and only then make it available for writes
                        ActiveSegment<K, V> nextActive = availableSegment.getNow();
                        availableSegment = null;
                        addNewActiveSegment(nextActive);
                        currentSegment = nextActive;
                        break;
                    }
                    else
                    {
                        waitOn = availableSegment;
                    }
                }
            }

            if (waitOn != null)
                waitOn.awaitThrowUncheckedOnInterrupt();
        }

        // signal the allocator thread to prepare a new segment
        requestSegmentAllocation();

        // request that the journal be flushed out-of-band, as we've finished a segment
        flusher.requestExtraFlush();
    }

    private void discardAvailableSegment()
    {
        Promise<ActiveSegment<K, V>> next;
        synchronized (this)
        {
            next = availableSegment;
            availableSegment = null;
        }

        if (next != null)
        {
            ActiveSegment<K, V> segment = next.awaitThrowUncheckedOnInterrupt().getNow();
            if (segment != null) // can be null during shutdown
                segment.closeAndDiscard(this);
        }
    }

    private class Allocator implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                State state = Journal.this.state.get();
                switch (state)
                {
                    case UNINITIALIZED:
                        throw new IllegalStateException("Should not be in uninitialized state");
                    case INITIALIZING:
                    case NORMAL:
                        runNormal();
                        break;
                    case SHUTDOWN:
                    case TERMINATED:
                        shutdown();
                        break;
                }
            }
            catch (InterruptedException e)
            {
                discardAvailableSegment();
                throw new UncheckedInterruptedException();
            }
        }

        private void runNormal() throws InterruptedException
        {
            boolean interrupted = false;
            try
            {
                Invariants.require(availableSegment != null);
                Invariants.require(!availableSegment.isDone());

                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in createSegment()
                synchronized (this)
                {
                    interrupted = Thread.interrupted();
                    availableSegment.setSuccess(createSegment());
                }

                while (availableSegment != null && Thread.interrupted())
                    LockSupport.parkNanos(100);
            }
            catch (JournalWriteError e)
            {
                if (!(e.getCause() instanceof ClosedByInterruptException))
                    throw e;
            }
            catch (Throwable t)
            {
                if (!handleError("Failed allocating a journal segment", t))
                {
                    discardAvailableSegment();
                    throw new TerminateException();
                }
                TimeUnit.SECONDS.sleep(1L); // sleep for a second to avoid log spam
            }

            if (interrupted || Thread.interrupted())
            {
                discardAvailableSegment();
                throw new InterruptedException();
            }
        }

        private void shutdown()
        {
            Promise<ActiveSegment<K, V>> next;
            synchronized (this)
            {
                next = availableSegment;
                availableSegment = null;
            }

            if (next != null)
            {
                if (next.isDone())
                    next.getNow().closeAndDiscard(Journal.this);
                else
                    next.cancel(true);
            }
        }
    }

    private ActiveSegment<K, V> createSegment()
    {
        Descriptor descriptor = Descriptor.create(directory, nextSegmentId.getAndIncrement(), params.userVersion());
        return create(descriptor, params, keySupport);
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

    Segments<K, V> segments()
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

    private void replaceCompletedSegment(ActiveSegment<K, V> activeSegment, StaticSegment<K, V> staticSegment)
    {
        swapSegments(current -> current.withCompletedSegment(activeSegment, staticSegment));
    }

    void replaceCompactedSegments(Collection<StaticSegment<K, V>> oldSegments, Collection<StaticSegment<K, V>> compactedSegments)
    {
        swapSegments(current -> current.withCompactedSegments(oldSegments, compactedSegments));
    }

    public ActiveSegment<K, V> currentActiveSegment()
    {
        return currentSegment;
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
        private final ActiveSegment<K, V> completedSegment;

        CloseActiveSegmentRunnable(ActiveSegment<K, V> activeSegment)
        {
            this.completedSegment = activeSegment;
        }

        @Override
        public void run()
        {
            completedSegment.discardUnusedTail();
            completedSegment.fsync();
            completedSegment.persistComponents();
            replaceCompletedSegment(completedSegment, StaticSegment.open(completedSegment.descriptor, keySupport));
            completedSegment.release(Journal.this);
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

        maintenance.execute(new CloseActiveSegmentRunnable(activeSegment));
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
        if (readCRC != (int) crc.getValue())
            throw new Crc.InvalidCrc(readCRC, (int) crc.getValue());
    }

    /*
     * Error handling
     */

    /**
     * @return true if the invoking thread should continue, or false if it should terminate itself
     */
    boolean handleError(String message, Throwable t)
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
        ActiveSegment<?, ?> discarding = currentSegment;
        if (!discarding.isEmpty()) // if there is no data in the segement then ignore it
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

    public interface Writer
    {
        void write(DataOutputPlus out, int userVersion) throws IOException;
    }

    /**
     * Static segment iterator iterates all keys in _static_ segments in order.
     */
    public StaticSegmentKeyIterator staticSegmentKeyIterator()
    {
        return new StaticSegmentKeyIterator();
    }

    /**
     * List of key and a list of segment descriptors referencing this key
     */
    public static class KeyRefs<K>
    {
        long segments[];
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

        public K key()
        {
            return key;
        }

        private void add(K key, long segment)
        {
            this.key = key;
            if (size == 0 || segments[size - 1] < segment)
                segments[size++] = segment;
            else
                Invariants.require(segments[size - 1] == segment,
                                   "Tried to add an out-of-order segment: %d, %s", segment,
                                   LazyToString.lazy(() -> Arrays.toString(Arrays.copyOf(segments, size))));
        }

        private void reset()
        {
            key = null;
            size = 0;
            Arrays.fill(segments, 0);
        }
    }

    public class StaticSegmentKeyIterator implements CloseableIterator<KeyRefs<K>>
    {
        private final ReferencedSegments<K, V> segments;
        private final MergeIterator<Head, KeyRefs<K>> iterator;

        public StaticSegmentKeyIterator()
        {
            this.segments = selectAndReference(Segment::isStatic);
            List<Iterator<Head>> iterators = new ArrayList<>(segments.count());

            for (Segment<K, V> segment : segments.allSorted(true))
            {
                StaticSegment<K, V> staticSegment = (StaticSegment<K, V>) segment;
                Iterator<K> iter = staticSegment.index().reader();
                Head head = new Head(staticSegment.descriptor.timestamp);
                iterators.add(new Iterator<>()
                {
                    public boolean hasNext()
                    {
                        return iter.hasNext();
                    }

                    public Head next()
                    {
                        head.key = iter.next();
                        return head;
                    }
                });
            }

            this.iterator = MergeIterator.get(iterators,
                                              (r1, r2) -> {
                                                  int keyCmp = keySupport.compare(r1.key, r2.key);
                                                  if (keyCmp != 0)
                                                      return keyCmp;
                                                  return Long.compare(r1.segment, r2.segment);
                                              },
                                              new MergeIterator.Reducer<Head, KeyRefs<K>>()
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
}
