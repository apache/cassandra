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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * A shared buffer that temporarily holds the serialized hints before they are flushed to disk.
 *
 * Consists of :
 * - a ByteBuffer holding the serialized hints (length, length checksum and total checksum included)
 * - a pointer to the current allocation offset
 * - an {@link OpOrder} appendOrder for {@link HintsWriteExecutor} to wait on for all writes completion
 * - a map of (host id -> offset queue) for the hints written
 *
 * It's possible to write a single hint for two or more hosts at the same time, in which case the same offset will be put
 * into two or more offset queues.
 */
final class HintsBuffer
{
    // hint entry overhead in bytes (int length, int length checksum, int body checksum)
    static final int ENTRY_OVERHEAD_SIZE = 12;
    static final int CLOSED = -1;

    private final ByteBuffer slab; // the underlying backing ByteBuffer for all the serialized hints
    private final AtomicInteger position; // the position in the slab that we currently allocate from

    private final ConcurrentMap<UUID, Queue<Integer>> offsets;
    private final OpOrder appendOrder;

    private HintsBuffer(ByteBuffer slab)
    {
        this.slab = slab;

        position = new AtomicInteger();
        offsets = new ConcurrentHashMap<>();
        appendOrder = new OpOrder();
    }

    static HintsBuffer create(int slabSize)
    {
        return new HintsBuffer(ByteBuffer.allocateDirect(slabSize));
    }

    boolean isClosed()
    {
        return position.get() == CLOSED;
    }

    int capacity()
    {
        return slab.capacity();
    }

    int remaining()
    {
        int pos = position.get();
        return pos == CLOSED ? 0 : capacity() - pos;
    }

    HintsBuffer recycle()
    {
        slab.clear();
        return new HintsBuffer(slab);
    }

    void free()
    {
        FileUtils.clean(slab);
    }

    /**
     * Wait for any appends started before this method was called.
     */
    void waitForModifications()
    {
        appendOrder.awaitNewBarrier(); // issue a barrier and wait for it
    }

    Set<UUID> hostIds()
    {
        return offsets.keySet();
    }

    /**
     * Coverts the queue of offsets for the selected host id into an iterator of hints encoded as ByteBuffers.
     */
    Iterator<ByteBuffer> consumingHintsIterator(UUID hostId)
    {
        final Queue<Integer> bufferOffsets = offsets.get(hostId);

        if (bufferOffsets == null)
            return Collections.emptyIterator();

        return new AbstractIterator<ByteBuffer>()
        {
            private final ByteBuffer flyweight = slab.duplicate();

            protected ByteBuffer computeNext()
            {
                Integer offset = bufferOffsets.poll();

                if (offset == null)
                    return endOfData();

                int totalSize = slab.getInt(offset) + ENTRY_OVERHEAD_SIZE;

                return (ByteBuffer) flyweight.clear().position(offset).limit(offset + totalSize);
            }
        };
    }

    @SuppressWarnings("resource")
    Allocation allocate(int hintSize)
    {
        int totalSize = hintSize + ENTRY_OVERHEAD_SIZE;

        if (totalSize > slab.capacity() / 2)
        {
            throw new IllegalArgumentException(String.format("Hint of %s bytes is too large - the maximum size is %s",
                                                             hintSize,
                                                             slab.capacity() / 2));
        }

        OpOrder.Group opGroup = appendOrder.start(); // will eventually be closed by the receiver of the allocation
        try
        {
            return allocate(totalSize, opGroup);
        }
        catch (Throwable t)
        {
            opGroup.close();
            throw t;
        }
    }

    private Allocation allocate(int totalSize, OpOrder.Group opGroup)
    {
        int offset = allocateBytes(totalSize);
        if (offset < 0)
        {
            opGroup.close();
            return null;
        }
        return new Allocation(offset, totalSize, opGroup);
    }

    private int allocateBytes(int totalSize)
    {
        while (true)
        {
            int prev = position.get();
            int next = prev + totalSize;

            if (prev == CLOSED) // the slab has been 'closed'
                return CLOSED;

            if (next > slab.capacity())
            {
                position.set(CLOSED); // mark the slab as no longer allocating if we've exceeded its capacity
                return CLOSED;
            }

            if (position.compareAndSet(prev, next))
                return prev;
        }
    }

    private void put(UUID hostId, int offset)
    {
        // we intentionally don't just return offsets.computeIfAbsent() because it's expensive compared to simple get(),
        // and the method is on a really hot path
        Queue<Integer> queue = offsets.get(hostId);
        if (queue == null)
            queue = offsets.computeIfAbsent(hostId, (id) -> new ConcurrentLinkedQueue<>());
        queue.offer(offset);
    }

    /**
     * A placeholder for hint serialization. Should always be used in a try-with-resources block.
     */
    final class Allocation implements AutoCloseable
    {
        private final Integer offset;
        private final int totalSize;
        private final OpOrder.Group opGroup;

        Allocation(int offset, int totalSize, OpOrder.Group opGroup)
        {
            this.offset = offset;
            this.totalSize = totalSize;
            this.opGroup = opGroup;
        }

        void write(Iterable<UUID> hostIds, Hint hint)
        {
            write(hint);
            for (UUID hostId : hostIds)
                put(hostId, offset);
        }

        public void close()
        {
            opGroup.close();
        }

        private void write(Hint hint)
        {
            ByteBuffer buffer = (ByteBuffer) slab.duplicate().position(offset).limit(offset + totalSize);
            CRC32 crc = new CRC32();
            int hintSize = totalSize - ENTRY_OVERHEAD_SIZE;
            try (DataOutputBuffer dop = new DataOutputBufferFixed(buffer))
            {
                dop.writeInt(hintSize);
                updateChecksumInt(crc, hintSize);
                dop.writeInt((int) crc.getValue());

                Hint.serializer.serialize(hint, dop, MessagingService.current_version);
                updateChecksum(crc, buffer, buffer.position() - hintSize, hintSize);
                dop.writeInt((int) crc.getValue());
            }
            catch (IOException e)
            {
                throw new AssertionError(); // cannot happen
            }
        }
    }
}
