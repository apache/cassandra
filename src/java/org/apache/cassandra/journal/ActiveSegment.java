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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.codahale.metrics.Timer;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.utils.Simulate.With.MONITORS;

@Simulate(with=MONITORS)
final class ActiveSegment<K, V> extends Segment<K, V>
{
    final FileChannel channel;

    // OpOrder used to order appends wrt flush
    private final OpOrder appendOrder = new OpOrder();

    // position in the buffer we are allocating from
    private final AtomicInteger allocatePosition = new AtomicInteger(0);

    /*
     * Everything before this offset has been written and flushed.
     */
    private volatile int lastFlushedOffset = 0;

    /*
     * End position of the buffer; initially set to its capacity and
     * updated to point to the last written position as the segment is being closed
     * no need to be volatile as writes are protected by appendOrder barrier.
     */
    private int endOfBuffer;

    // a signal that writers can wait on to be notified of a completed flush in BATCH and GROUP FlushMode
    private final WaitQueue flushComplete = WaitQueue.newWaitQueue();

    private final Ref<Segment<K, V>> selfRef;

    final InMemoryIndex<K> index;

    private ActiveSegment(
        Descriptor descriptor, Params params, SyncedOffsets syncedOffsets, InMemoryIndex<K> index, Metadata metadata, KeySupport<K> keySupport)
    {
        super(descriptor, syncedOffsets, metadata, keySupport);
        this.index = index;
        try
        {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, params.segmentSize());
            endOfBuffer = buffer.capacity();
            selfRef = new Ref<>(this, new Tidier(descriptor, channel, buffer, syncedOffsets));
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, file, e);
        }
    }

    @SuppressWarnings("resource")
    static <K, V> ActiveSegment<K, V> create(Descriptor descriptor, Params params, KeySupport<K> keySupport)
    {
        SyncedOffsets syncedOffsets = SyncedOffsets.active(descriptor, true);
        InMemoryIndex<K> index = InMemoryIndex.create(keySupport);
        Metadata metadata = Metadata.create();
        return new ActiveSegment<>(descriptor, params, syncedOffsets, index, metadata, keySupport);
    }

    @Override
    InMemoryIndex<K> index()
    {
        return index;
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
    boolean read(int offset, EntrySerializer.EntryHolder<K> into)
    {
        ByteBuffer duplicate = buffer.duplicate().position(offset).limit(buffer.capacity());
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
    @Override
    public synchronized void close()
    {
        close(true);
    }

    /**
     * @return true if the closed segment was definitely empty, false otherwise
     */
    private synchronized boolean close(boolean persistComponents)
    {
        boolean isEmpty = discardUnusedTail();
        if (!isEmpty)
        {
            flush();
            if (persistComponents) persistComponents();
        }
        release();
        return isEmpty;
    }

    /**
     * Close and discard a pre-allocated, available segment, that's never been exposed
     */
    void closeAndDiscard()
    {
        boolean isEmpty = close(false);
        if (!isEmpty) throw new IllegalStateException();
        discard();
    }

    void closeAndIfEmptyDiscard()
    {
        boolean isEmpty = close(true);
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
        descriptor.fileFor(Component.SYNCED_OFFSETS).deleteIfExists();
    }

    void release()
    {
        selfRef.release();
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

    private static final class Tidier implements Tidy
    {
        private final Descriptor descriptor;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private final SyncedOffsets syncedOffsets;

        Tidier(Descriptor descriptor, FileChannel channel, ByteBuffer buffer, SyncedOffsets syncedOffsets)
        {
            this.descriptor = descriptor;
            this.channel = channel;
            this.buffer = buffer;
            this.syncedOffsets = syncedOffsets;
        }

        @Override
        public void tidy()
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
            syncedOffsets.close();
        }

        @Override
        public String name()
        {
            return descriptor.toString();
        }
    }

    /*
     * Flush logic; closing and component flushing
     */

    boolean shouldFlush()
    {
        int allocatePosition = this.allocatePosition.get();
        return lastFlushedOffset < allocatePosition;
    }

    /**
     * Possibly force a disk flush for this segment file.
     * TODO FIXME: calls from outside Flusher + callbacks
     * @return last synced offset
     */
    synchronized int flush()
    {
        int allocatePosition = this.allocatePosition.get();
        if (lastFlushedOffset >= allocatePosition)
            return lastFlushedOffset;

        waitForModifications();
        flushInternal();
        lastFlushedOffset = allocatePosition;
        int syncedOffset = Math.min(allocatePosition, endOfBuffer);
        syncedOffsets.mark(syncedOffset);
        flushComplete.signalAll();
        return syncedOffset;
    }

    private void waitForFlush(int position)
    {
        while (lastFlushedOffset < position)
        {
            WaitQueue.Signal signal = flushComplete.register();
            if (lastFlushedOffset < position)
                signal.awaitThrowUncheckedOnInterrupt();
            else
                signal.cancel();
        }
    }

    /**
     * Wait for any appends or discardUnusedTail() operations started before this method was called
     */
    private void waitForModifications()
    {
        // issue a barrier and wait for it
        appendOrder.awaitNewBarrier();
    }

    private void flushInternal()
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

    boolean isCompletedAndFullyFlushed(int syncedOffset)
    {
        return syncedOffset >= endOfBuffer;
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
                int prev = allocatePosition.get();
                int next = endOfBuffer + 1;

                if (prev >= next)
                {
                    // already stopped allocating, might also be closed
                    assert buffer == null || prev == buffer.capacity() + 1;
                    return false;
                }

                if (allocatePosition.compareAndSet(prev, next))
                {
                    // stopped allocating now; can only succeed once, no further allocation or discardUnusedTail can succeed
                    endOfBuffer = prev;
                    assert buffer != null && next == buffer.capacity() + 1;
                    return prev == 0;
                }
            }
        }
    }

    /*
     * Entry/bytes allocation logic
     */

    @SuppressWarnings({ "resource", "RedundantSuppression" }) // op group will be closed by Allocation#write()
    Allocation allocate(int entrySize, Set<Integer> hosts)
    {
        int totalSize = totalEntrySize(hosts, entrySize);
        OpOrder.Group opGroup = appendOrder.start();
        try
        {
            int position = allocateBytes(totalSize);
            if (position < 0)
            {
                opGroup.close();
                return null;
            }
            return new Allocation(opGroup, buffer.duplicate().position(position).limit(position + totalSize));
        }
        catch (Throwable t)
        {
            opGroup.close();
            throw t;
        }
    }

    private int totalEntrySize(Set<Integer> hosts, int recordSize)
    {
        return EntrySerializer.fixedEntrySize(keySupport, descriptor.userVersion)
             + EntrySerializer.variableEntrySize(hosts.size(), recordSize);
    }

    // allocate bytes in the segment, or return -1 if not enough space
    private int allocateBytes(int size)
    {
        while (true)
        {
            int prev = allocatePosition.get();
            int next = prev + size;
            if (next >= endOfBuffer)
                return -1;
            if (allocatePosition.compareAndSet(prev, next))
            {
                assert buffer != null;
                return prev;
            }
            LockSupport.parkNanos(1); // ConstantBackoffCAS Algorithm from https://arxiv.org/pdf/1305.5800.pdf
        }
    }

    final class Allocation
    {
        private final OpOrder.Group appendOp;
        private final ByteBuffer buffer;
        private final int position;
        private final int size;

        Allocation(OpOrder.Group appendOp, ByteBuffer buffer)
        {
            this.appendOp = appendOp;
            this.buffer = buffer;
            this.position = buffer.position();
            this.size = buffer.remaining();
        }

        void write(K id, ByteBuffer record, Set<Integer> hosts)
        {
            try (BufferedDataOutputStreamPlus out = new DataOutputBufferFixed(buffer))
            {
                EntrySerializer.write(id, record, hosts, keySupport, out, descriptor.userVersion);
                index.update(id, position);
                metadata.update(hosts);
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

        void asyncWrite(K id, V record, ByteBuffer bytes, Set<Integer> hosts, Object writeContext, AsyncCallbacks<K, V> callbacks) throws IOException
        {
            try (BufferedDataOutputStreamPlus out = new DataOutputBufferFixed(buffer))
            {
                EntrySerializer.write(id, bytes, hosts, keySupport, out, descriptor.userVersion);
                index.update(id, position);
                metadata.update(hosts);
                callbacks.onWrite(descriptor.timestamp, position, size, id, record, writeContext);
            }
            finally
            {
                appendOp.close();
            }
        }

        void awaitFlush(Timer waitingOnFlush)
        {
            try (Timer.Context ignored = waitingOnFlush.time())
            {
                waitForFlush(position);
            }
        }
    }
}
