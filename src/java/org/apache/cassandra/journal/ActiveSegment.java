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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.Simulate.With.MONITORS;

@Simulate(with=MONITORS)
public final class ActiveSegment<K, V> extends Segment<K, V>
{
    final FileChannel channel;

    // OpOrder used to order appends wrt flush
    private final OpOrder appendOrder = new OpOrder();

    // position in the buffer we are allocating from
    private volatile long allocateOffset = 0;
    private static final AtomicLongFieldUpdater<ActiveSegment> allocateOffsetUpdater = AtomicLongFieldUpdater.newUpdater(ActiveSegment.class, "allocateOffset");

    /*
     * Allocation started at fsyncedTo or any earlier offset are guaranteed to be both written and flushed.
     */
    private volatile int fsyncedTo = 0;
    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<ActiveSegment> fsyncedToUpdater = AtomicIntegerFieldUpdater.newUpdater(ActiveSegment.class, "fsyncedTo");

    /*
     * End position of the buffer; initially set to its capacity and
     * updated to point to the last written position as the segment is being closed
     * no need to be volatile as writes are protected by appendOrder barrier.
     */
    private int endOfBuffer;

    private final Ref<Segment<K, V>> selfRef;

    private final InMemoryIndex<K> index;
    private Params.FlushMode flushMode;
    private ActiveSegment(Descriptor descriptor, Params params, InMemoryIndex<K> index, Metadata metadata, KeySupport<K> keySupport)
    {
        super(descriptor, metadata, keySupport);
        this.index = index;
        this.flushMode = params.flushMode();

        try
        {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, params.segmentSize());
            endOfBuffer = buffer.capacity();
            selfRef = new Ref<>(this, new Tidier(descriptor, channel, buffer));
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, file, e);
        }
    }

    static <K, V> ActiveSegment<K, V> create(Descriptor descriptor, Params params, KeySupport<K> keySupport)
    {
        InMemoryIndex<K> index = InMemoryIndex.create(keySupport);
        Metadata metadata = Metadata.create();
        return new ActiveSegment<>(descriptor, params, index, metadata, keySupport);
    }

    @Override
    public InMemoryIndex<K> index()
    {
        return index;
    }

    boolean isEmpty()
    {
        return allocateOffset == 0;
    }

    @Override
    boolean isActive()
    {
        return true;
    }

    @Override
    ActiveSegment<K, V> asActive()
    {
        return this;
    }

    @Override
    StaticSegment<K, V> asStatic()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Read the entry and specified offset into the entry holder.
     * Expects the caller to acquire the ref to the segment and the record to exist.
     */
    @Override
    boolean read(int offset, int size, EntrySerializer.EntryHolder<K> into)
    {
        ByteBuffer duplicate = buffer.duplicate().position(offset).limit(offset + size);
        try
        {
            EntrySerializer.read(into, keySupport, duplicate, descriptor.userVersion);
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, file, e);
        }
        return true;
    }

    /**
     * Stop writing to this file, flush and close it. Does nothing if the file is already closed.
     */
    public synchronized void close(Journal<K, V> journal)
    {
        close(journal, true);
    }

    /**
     * @return true if the closed segment was definitely empty, false otherwise
     */
    private synchronized boolean close(Journal<K, V> journal, boolean persistComponents)
    {
        boolean isEmpty = discardUnusedTail();
        if (!isEmpty)
        {
            fsync();
            if (persistComponents) persistComponents();
        }
        release(journal);
        return isEmpty;
    }

    /**
     * Close and discard a pre-allocated, available segment, that's never been exposed
     */
    void closeAndDiscard(Journal<K, V> journal)
    {
        boolean isEmpty = close(journal, false);
        if (!isEmpty) throw new IllegalStateException();
        discard();
    }

    void closeAndIfEmptyDiscard(Journal<K, V> journal)
    {
        boolean isEmpty = close(journal, true);
        if (isEmpty) discard();
    }

    void persistComponents()
    {
        index.persist(descriptor);
        metadata.persist(descriptor);
        SyncUtil.trySyncDir(descriptor.directory);
    }

    private void discard()
    {
        selfRef.ensureReleased();

        descriptor.fileFor(Component.DATA).deleteIfExists();
        descriptor.fileFor(Component.INDEX).deleteIfExists();
        descriptor.fileFor(Component.METADATA).deleteIfExists();
    }

    @Override
    public Ref<Segment<K, V>> tryRef()
    {
        return selfRef.tryRef();
    }

    @Override
    public Ref<Segment<K, V>> ref()
    {
        return selfRef.ref();
    }

    @Override
    public Ref<Segment<K, V>> selfRef()
    {
        return selfRef;
    }

    private static final class Tidier extends Segment.Tidier implements Tidy
    {
        private final Descriptor descriptor;
        private final FileChannel channel;
        private final ByteBuffer buffer;

        Tidier(Descriptor descriptor, FileChannel channel, ByteBuffer buffer)
        {
            this.descriptor = descriptor;
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        void onUnreferenced()
        {
            FileUtils.clean(buffer);
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, Component.DATA, e);
            }
        }

        @Override
        public String name()
        {
            return descriptor.toString();
        }
    }

    public boolean fullyFlushed()
    {
        return fsyncedTo == allocateOffset;
    }

    /**
     * We are _always_ flushing upon full write, which is why we are tracking only lower allocation bounds.
     */
    void fsync(int upTo)
    {
        fsyncInternal();
        fsyncedToUpdater.accumulateAndGet(this, upTo, Math::max);
    }

    void fsync()
    {
        if (fullyFlushed())
            return;

        waitForModifications();
        fsyncInternal();
    }

    /**
     * Wait for any appends or discardUnusedTail() operations started before this method was called
     */
    private void waitForModifications()
    {
        // issue a barrier and wait for it
        appendOrder.awaitNewBarrier();
    }

    private void fsyncInternal()
    {
        try
        {
            SyncUtil.force((MappedByteBuffer) buffer);
        }
        catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
        {
            throw new JournalWriteError(descriptor, file, e);
        }
    }

    /**
     * Ensures no more of this segment is writeable, by allocating any unused section at the end
     * and marking it discarded void discartUnusedTail()
     *
     * @return true if the segment was empty, false otherwise
     */
    boolean discardUnusedTail()
    {
        try (OpOrder.Group ignored = appendOrder.start())
        {
            while (true)
            {
                long prev = completeInProgress();
                int next = endOfBuffer + 1;

                if ((int)prev >= next)
                {
                    // already stopped allocating, might also be closed
                    assert buffer == null || prev == buffer.capacity() + 1;
                    return false;
                }

                if (allocateOffsetUpdater.compareAndSet(this, prev, next))
                {
                    // stopped allocating now; can only succeed once, no further allocation or discardUnusedTail can succeed
                    endOfBuffer = (int)prev;
                    assert buffer != null && next == buffer.capacity() + 1;
                    return prev == 0;
                }
                LockSupport.parkNanos(1);
            }
        }
    }

    /*
     * Entry/bytes allocation logic
     */

    @SuppressWarnings({ "resource", "RedundantSuppression" }) // op group will be closed by Allocation#write()
    Allocation allocate(int entrySize)
    {
        int totalSize = totalEntrySize(entrySize);
        OpOrder.Group opGroup = appendOrder.start();
        try
        {
            int position = allocateBytes(totalSize);
            if (position < 0)
            {
                opGroup.close();
                return null;
            }
            return new Allocation(opGroup, buffer.duplicate().position(position).limit(position + totalSize), totalSize);
        }
        catch (Throwable t)
        {
            opGroup.close();
            throw t;
        }
    }

    private int totalEntrySize(int recordSize)
    {
        return EntrySerializer.headerSize(keySupport, descriptor.userVersion)
             + recordSize
             + TypeSizes.INT_SIZE // CRC
        ;
    }

    // allocate bytes in the segment, or return -1 if not enough space
    private int allocateBytes(int size)
    {
        while (true)
        {
            long prev = maybeCompleteInProgress();
            if (prev < 0)
            {
                LockSupport.parkNanos(1); // ConstantBackoffCAS Algorithm from https://arxiv.org/pdf/1305.5800.pdf
                continue;
            }

            long next = prev + size;
            if (next >= endOfBuffer)
                return -1;

            // TODO (expected): if we write a "safe shutdown" marker we don't need this,
            //  but this provides safe restart in the event the process terminates abruptly but the host remains stable
            long inProgress = prev | (next << 32);
            if (!allocateOffsetUpdater.compareAndSet(this, prev, inProgress))
            {
                LockSupport.parkNanos(1); // ConstantBackoffCAS Algorithm from https://arxiv.org/pdf/1305.5800.pdf
                continue;
            }

            assert buffer != null;
            buffer.putInt((int)prev, (int)next);
            allocateOffsetUpdater.compareAndSet(this, inProgress, next);
            return (int) prev;
        }
    }

    public interface RecordPointer
    {
        long segment();
        int start();
        void awaitUninterruptibly();
        void onFlush(OnFlush onFlush);
    }

    public interface OnFlush
    {
        void success();
        void failure(Throwable t);
    }

    final class Allocation implements RecordPointer, Comparable<Allocation>
    {
        private final OpOrder.Group appendOp;
        private final ByteBuffer buffer;
        private final int start;
        private final int length;
        private final AsyncPromise<Void> future;

        public final long writtenAtNanos; // only set for periodic mode

        Allocation(OpOrder.Group appendOp, ByteBuffer buffer, int length)
        {
            this.appendOp = appendOp;
            this.buffer = buffer;
            this.start = buffer.position();
            this.length = length;
            this.future = new AsyncPromise<>();
            this.writtenAtNanos = Clock.Global.nanoTime();
        }

        void flushed()
        {
            // in PERIODIC, we notify about persistence after successful write to memory buffer, which means we may call this method twice
            if (flushMode == Params.FlushMode.PERIODIC)
                future.trySuccess(null);
            else
                future.setSuccess(null);
        }

        void flushFailed(Throwable t)
        {
            if (flushMode == Params.FlushMode.PERIODIC)
                future.tryFailure(t);
            else
                future.setFailure(t);
        }

        ActiveSegment<?, ?> holder()
        {
            return ActiveSegment.this;
        }

        void write(K id, ByteBuffer record)
        {
            try
            {
                EntrySerializer.write(id, record, keySupport, buffer, descriptor.userVersion);
                metadata.update();
                index.update(id, start, length);
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, file, e);
            }
            finally
            {
                appendOp.close();
            }
        }

        @Override
        public int start()
        {
            return start;
        }

        @Override
        public void awaitUninterruptibly()
        {
            future.awaitUninterruptibly();
        }

        @Override
        public void onFlush(OnFlush onFlush)
        {
            future.addCallback((ignore_) -> onFlush.success(), onFlush::failure);
        }

        int end()
        {
            return start + length;
        }

        public long segment()
        {
            return descriptor.timestamp;
        }

        @Override
        public int compareTo(Allocation o)
        {
            int cmp = Long.compare(segment(), o.segment());
            if (cmp != 0)
                return cmp;
            return Integer.compare(start, o.start);
        }

        public String toString()
        {
            return "Allocation{" +
                   "segment=" + segment() +
                   ", start=" + start +
                   ", end=" + end() +
                   '}';
        }
    }

    private int maybeCompleteInProgress()
    {
        long cur = allocateOffset;
        int inProgress = (int) (cur >>> 32);
        if (inProgress == 0) return (int) cur;
        // finish up the in-progress allocation
        buffer.putInt((int)cur, inProgress);
        if (!allocateOffsetUpdater.compareAndSet(this, cur, inProgress))
            return -1;

        return inProgress;
    }

    private int completeInProgress()
    {
        int result = maybeCompleteInProgress();
        while (result < 0)
            result = maybeCompleteInProgress();
        return result;
    }

    public String toString()
    {
        return "ActiveSegment{" +
               "descriptor=" + descriptor.timestamp +
               '}';
    }
}
