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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;

import org.apache.cassandra.db.rows.AbstractBufferCellData;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;

/// [CellData] objects stored in in-memory tries.
/// Uses one 32-byte cell of an in-memory trie's buffer to store the data of a cell (without path and column id). This
/// includes liveness (timestamp/ttl/local deletion time) and value. If the value is small enough to fit, it is placed
/// directly inside the 32-byte cell; otherwise we use the given saver/loader to map it to a long integer handle and
/// store the handle.
public abstract class TrieCellData extends AbstractBufferCellData
{
    public interface ExternalBufferHandler
    {
        /// Store the data in the given buffer and return an integer handle for it (e.g. a native address).
        long store(ByteBuffer buffer, int length) throws TrieSpaceExhaustedException;

        /// Load the data from the given handle (e.g. a native address) and return it in a buffer.
        ByteBuffer load(long handle, int length);

        /// Release a handle which will no longer be used.
        void release(long handle, int length);
    }

    public static final int OFFSET_TIMESTAMP = 24;
    public static final int OFFSET_LOCAL_DELETION_TIME = 20;
    public static final int OFFSET_TTL = 16;
    /// If the value fits, it is placed starting from this offset in the trie cell.
    public static final int OFFSET_DATA = 0;
    /// If the value does not fit, these 8 bytes hold its external handle.
    public static final int OFFSET_EXTERNAL_HANDLE = 8;
    /// If the value does not fit, these 4 bytes hold its length.
    public static final int OFFSET_EXTERNAL_LENGTH = 4;
    /// Byte storing whether an externally-stored cell is a counter
    public static final int OFFSET_EXTERNAL_IS_COUNTER = 0;

    /// Length of an embedded counter
    private static final int OFFSET_COUNTER_LENGTH = 15;

    final UnsafeBuffer buffer;
    final int inBufferPos;

    /// Store the given cell data in the 32 bytes of `buffer` starting at position `inBufferPos`. If the value cannot fit in
    /// this space, use the given external saver to store it, and save the resulting handle and the length of the value.
    public static int serialize(CellData<?, ?> cell,
                                UnsafeBuffer buffer, int inBufferPos,
                                ExternalBufferHandler externalBufferSaver)
    throws TrieSpaceExhaustedException
    {
        ByteBuffer value = cell.buffer();
        int length = value.remaining();
        buffer.putLong(inBufferPos + OFFSET_TIMESTAMP, cell.timestamp());
        buffer.putInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME, CellData.deletionTimeLongToUnsignedInteger(cell.localDeletionTime()));
        buffer.putInt(inBufferPos + OFFSET_TTL, cell.ttl());

        boolean isCounterCell = cell.isCounterCell();
        if (isCounterCell && length <= OFFSET_COUNTER_LENGTH)
        {
            assert length <= OFFSET_COUNTER_LENGTH;
            buffer.putByte(inBufferPos + OFFSET_COUNTER_LENGTH, (byte) length);
            buffer.putBytes(inBufferPos + OFFSET_DATA, value, value.position(), length);
            return TrieMemtable.TrieSerializer.TYPE_CELL_COUNTER;
        }

        if (!isCounterCell && cellValueCanBeEmbedded(cell, length))
        {
            // Storing value embedded in trie cell. This may overwrite the TTL/local deletion, which we won't read if
            // the length is above OFFSET_TTL.

            // using the inBufferPos, length version of putBytes to make sure the source buffer's position is not touched
            buffer.putBytes(inBufferPos + OFFSET_DATA, value, value.position(), length);
            return length;
        }

        // stored externally
        long handle = externalBufferSaver.store(value, length);
        buffer.putLong(inBufferPos + OFFSET_EXTERNAL_HANDLE, handle);
        buffer.putInt(inBufferPos + OFFSET_EXTERNAL_LENGTH, length);
        buffer.putByte(inBufferPos + OFFSET_EXTERNAL_IS_COUNTER, (byte) (isCounterCell ? 1 : 0));
        return TrieMemtable.TrieSerializer.TYPE_CELL_EXTERNAL_VALUE;
    }

    private static boolean cellValueCanBeEmbedded(CellData<?, ?> cell, int length)
    {
        // No expiration time implies no TTL
        return length <= OFFSET_TTL || length <= OFFSET_TIMESTAMP && cell.localDeletionTime() == NO_DELETION_TIME;
    }

    TrieCellData(UnsafeBuffer buffer, int inBufferPos)
    {
        this.buffer = buffer;
        this.inBufferPos = inBufferPos;
    }

    @Override
    public boolean isCounterCell()
    {
        return false;
    }

    @Override
    public long timestamp()
    {
        return buffer.getLong(inBufferPos + OFFSET_TIMESTAMP);
    }

    @Override
    public int ttl()
    {
        return buffer.getInt(inBufferPos + OFFSET_TTL);
    }

    @Override
    public int localDeletionTimeAsUnsignedInt()
    {
        return buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        // Managed separately by trie/external handler
        return 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        // Managed separately by trie/external handler
        return 0;
    }

    public static long offTrieSize(CellData<?, ?> cell)
    {
        int sz = cell.valueSize();
        return cellValueCanBeEmbedded(cell, sz)
               ? 0
               : sz;
    }

    public static TrieCellData embedded(UnsafeBuffer buffer, int inBufferPos, int length)
    {
        return length <= OFFSET_TTL ? new Embedded(buffer, inBufferPos, length)
                                    : new EmbeddedNoTTL(buffer, inBufferPos, length);
    }

    public static class Embedded extends TrieCellData
    {
        final int length;

        public Embedded(UnsafeBuffer buffer, int inBufferPos, int length)
        {
            super(buffer, inBufferPos);
            this.length = length;
        }


        @Override
        public int valueSize()
        {
            return length;
        }

        @Override
        public ByteBuffer value()
        {
            ByteBuffer buf = buffer.byteBuffer().duplicate();
            buf.position(inBufferPos + OFFSET_DATA);
            buf.limit(inBufferPos + OFFSET_DATA + length);
            return buf; // we don't need to slice
        }
    }

    public static class EmbeddedNoTTL extends Embedded
    {
        public EmbeddedNoTTL(UnsafeBuffer buffer, int inBufferPos, int length)
        {
            super(buffer, inBufferPos, length);
        }

        @Override
        public int ttl()
        {
            return NO_TTL;
        }

        @Override
        public int localDeletionTimeAsUnsignedInt()
        {
            return NO_DELETION_TIME_UNSIGNED_INTEGER;
        }
    }

    public static class Counter extends Embedded
    {
        public Counter(UnsafeBuffer buffer, int inBufferPos)
        {
            super(buffer, inBufferPos, buffer.getByte(inBufferPos + OFFSET_COUNTER_LENGTH));
        }

        @Override
        public boolean isCounterCell()
        {
            return true;
        }
    }

    public static class External extends TrieCellData
    {
        final ExternalBufferHandler handler;
        final boolean isCounterCell;

        public External(UnsafeBuffer buffer, int inBufferPos, ExternalBufferHandler handler)
        {
            super(buffer, inBufferPos);
            this.handler = handler;
            this.isCounterCell = buffer.getByte(inBufferPos + OFFSET_EXTERNAL_IS_COUNTER) != 0;
        }

        @Override
        public boolean isCounterCell()
        {
            return isCounterCell;
        }

        @Override
        public int valueSize()
        {
            return buffer.getInt(inBufferPos + OFFSET_EXTERNAL_LENGTH);
        }

        @Override
        public ByteBuffer value()
        {
            long handle = buffer.getLong(inBufferPos + OFFSET_EXTERNAL_HANDLE);
            int length = buffer.getInt(inBufferPos + OFFSET_EXTERNAL_LENGTH);
            return handler.load(handle, length);
        }

        public static void release(UnsafeBuffer buffer, int inBufferPos, ExternalBufferHandler handler)
        {
            long handle = buffer.getLong(inBufferPos + OFFSET_EXTERNAL_HANDLE);
            int length = buffer.getInt(inBufferPos + OFFSET_EXTERNAL_LENGTH);
            handler.release(handle, length);
        }
    }
}
