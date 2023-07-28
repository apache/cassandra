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
package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.common.base.Preconditions;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Iterator over a token range bounded segment of a Memtable index. Used to flush Memtable index segments to disk.
 */
public class MemtableTermsIterator implements TermsIterator
{
    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final Iterator<Pair<ByteComparable, IntArrayList>> iterator;

    private Pair<ByteComparable, IntArrayList> current;

    private long maxSSTableRowId = -1;
    private long minSSTableRowId = Long.MAX_VALUE;

    public MemtableTermsIterator(ByteBuffer minTerm,
                                 ByteBuffer maxTerm,
                                 Iterator<Pair<ByteComparable, IntArrayList>> iterator)
    {
        Preconditions.checkArgument(iterator != null);
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.iterator = iterator;
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    @Override
    public void close() {}

    @Override
    public PostingList postings()
    {
        //TODO Confirm that this can stay an IntArray post DSP-19608
        final IntArrayList list = current.right;

        assert list.size() > 0;

        final int minSegmentRowID = list.get(0);
        final int maxSegmentRowID = list.get(list.size() - 1);

        minSSTableRowId = Math.min(minSSTableRowId, minSegmentRowID);
        maxSSTableRowId = Math.max(maxSSTableRowId, maxSegmentRowID);

        final Iterator<IntCursor> it = list.iterator();

        return new PostingList()
        {
            @Override
            public long nextPosting()
            {
                if (!it.hasNext())
                {
                    return END_OF_STREAM;
                }

                return it.next().value;
            }

            @Override
            public long size()
            {
                return list.size();
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public ByteComparable next()
    {
        current = iterator.next();
        return current.left;
    }

    long getMaxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    long getMinSSTableRowId()
    {
        return minSSTableRowId;
    }
}
