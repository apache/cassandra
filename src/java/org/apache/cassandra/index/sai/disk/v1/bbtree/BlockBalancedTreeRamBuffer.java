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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.io.IOException;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * On-heap buffer for point values that provides a sortable view of itself as {@link IntersectingPointValues}.
 */
public class BlockBalancedTreeRamBuffer implements Accountable
{
    private final Counter bytesUsed;
    private final ByteBlockPool bytes;
    private final byte[] packedValue;
    private final PackedLongValues.Builder rowIDsBuilder;
    private int numPoints;
    private int numRows;
    private int lastSegmentRowID = -1;
    private boolean closed = false;

    public BlockBalancedTreeRamBuffer(int pointNumBytes)
    {
        this.bytesUsed = Counter.newCounter();

        this.bytes = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));

        packedValue = new byte[pointNumBytes];

        rowIDsBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        bytesUsed.addAndGet(rowIDsBuilder.ramBytesUsed());
    }

    @Override
    public long ramBytesUsed()
    {
        return bytesUsed.get();
    }

    public int numRows()
    {
        return numRows;
    }

    public long addPackedValue(int segmentRowId, BytesRef value)
    {
        ensureOpen();

        assert value.length == packedValue.length : "The value has length=" + value.length + " but should be " + packedValue.length;

        long startingBytesUsed = bytesUsed.get();
        long startingRowIDsBytesUsed = rowIDsBuilder.ramBytesUsed();

        rowIDsBuilder.add(segmentRowId);
        bytes.append(value);

        if (segmentRowId != lastSegmentRowID)
        {
            numRows++;
            lastSegmentRowID = segmentRowId;
        }

        numPoints++;

        long rowIDsAllocatedBytes = rowIDsBuilder.ramBytesUsed() - startingRowIDsBytesUsed;
        long endingBytesAllocated = bytesUsed.addAndGet(rowIDsAllocatedBytes);
        
        return endingBytesAllocated - startingBytesUsed;
    }

    public IntersectingPointValues asPointValues()
    {
        ensureOpen();
        // building packed longs is destructive
        closed = true;
        final PackedLongValues rowIDs = rowIDsBuilder.build();
        return new IntersectingPointValues()
        {
            final int[] ords = IntStream.range(0, numPoints).toArray();

            @Override
            public void getValue(int i, BytesRef bytesRef)
            {
                final long offset = (long) packedValue.length * (long) ords[i];
                bytesRef.length = packedValue.length;
                bytes.setRawBytesRef(bytesRef, offset);
            }

            @Override
            public byte getByteAt(int i, int k)
            {
                final long offset = (long) packedValue.length * (long) ords[i] + (long) k;

                return bytes.readByte(offset);
            }

            @Override
            public int getDocID(int i)
            {
                return Math.toIntExact(rowIDs.get(ords[i]));
            }

            @Override
            public void swap(int i, int j)
            {
                int tmp = ords[i];
                ords[i] = ords[j];
                ords[j] = tmp;
            }

            @Override
            public void intersect(IntersectVisitor visitor) throws IOException
            {
                final BytesRef scratch = new BytesRef();
                for (int i = 0; i < numPoints; i++)
                {
                    getValue(i, scratch);
                    assert scratch.length == packedValue.length;
                    System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedValue.length);
                    visitor.visit(getDocID(i), packedValue);
                }
            }

            @Override
            public int getBytesPerDimension()
            {
                return packedValue.length;
            }

            @Override
            public long size()
            {
                return numPoints;
            }

            @Override
            public int getDocCount()
            {
                return numRows;
            }
        };
    }

    private void ensureOpen()
    {
        Preconditions.checkState(!closed, "Expected open buffer.");
    }
}
