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
package org.apache.cassandra.index.sai.memory;

import java.util.Collections;
import java.util.Iterator;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.disk.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * In memory representation of {@link PrimaryKey} to row ID mappings which only contains
 * {@link Row} regardless it's live or deleted. ({@link RangeTombstoneMarker} is not included.)
 *
 * For JBOD, we can make use of sstable min/max partition key to filter irrelevant {@link MemtableIndex} subranges.
 * For Tiered Storage, in most cases, it flushes to tiered 0.
 */
public class RowMapping
{
    public static final RowMapping DUMMY = new RowMapping()
    {
        @Override
        public Iterator<Pair<ByteComparable, IntArrayList>> merge(MemtableIndex index) { return Collections.emptyIterator(); }

        @Override
        public void complete() {}

        @Override
        public void add(DecoratedKey key, Unfiltered unfiltered, long sstableRowId) {}
    };

    private final MemtableTrie<Integer> rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);

    private volatile boolean complete = false;

    public DecoratedKey minKey;
    public DecoratedKey maxKey;

    public int maxSegmentRowId = -1;

    private RowMapping()
    {
    }

    /**
     * Create row mapping for FLUSH operation only.
     */
    public static RowMapping create(OperationType opType)
    {
        if (opType == OperationType.FLUSH)
            return new RowMapping();
        return DUMMY;
    }

    /**
     * Merge IndexMemtable(index term to PrimaryKeys mappings) with row mapping of a sstable
     * (PrimaryKey to RowId mappings).
     *
     * @param index a Memtable-attached column index
     *
     * @return iterator of index term to postings mapping exists in the sstable
     */
    public Iterator<Pair<ByteComparable, IntArrayList>> merge(MemtableIndex index)
    {
        assert complete : "RowMapping is not built.";

        Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        return new AbstractIterator<Pair<ByteComparable, IntArrayList>>()
        {
            @Override
            protected Pair<ByteComparable, IntArrayList> computeNext()
            {
                while (iterator.hasNext())
                {
                    Pair<ByteComparable, PrimaryKeys> pair = iterator.next();

                    IntArrayList postings = null;
                    Iterator<PrimaryKey> primaryKeys = pair.right.iterator();

                    while (primaryKeys.hasNext())
                    {
                        PrimaryKey primaryKey = primaryKeys.next();
                        ByteComparable byteComparable = asComparableBytes(primaryKey.partitionKey(), primaryKey.clustering());
                        Integer segmentRowId = rowMapping.get(byteComparable);

                        if (segmentRowId != null)
                        {
                            postings = postings == null ? new IntArrayList() : postings;
                            postings.add(segmentRowId);
                        }
                    }
                    if (postings != null && !postings.isEmpty())
                        return Pair.create(pair.left, postings);
                }
                return endOfData();
            }
        };
    }

    /**
     * Complete building in memory RowMapping, mark it as immutable.
     */
    public void complete()
    {
        assert !complete : "RowMapping can only be built once.";
        this.complete = true;
    }

    /**
     * Include PrimaryKey to RowId mapping
     */
    public void add(DecoratedKey key, Unfiltered unfiltered, long sstableRowId)
    {
        assert !complete : "Cannot modify built RowMapping.";

        if (unfiltered.isRangeTombstoneMarker())
        {
            // currently we don't record range tombstones..
        }
        else
        {
            assert unfiltered.isRow();
            Row row = (Row) unfiltered;

            ByteComparable byteComparable = asComparableBytes(key, row.clustering());
            int segmentRowId = SegmentBuilder.castToSegmentRowId(sstableRowId, 0);
            try
            {
                rowMapping.apply(Trie.singleton(byteComparable, segmentRowId), (existing, neww) -> neww);
            }
            catch (MemtableTrie.SpaceExhaustedException e)
            {
                //TODO Work out how to handle this properly
                throw new RuntimeException(e);
            }

            maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

            // data is written in token sorted order
            if (minKey == null)
                minKey = key;
            maxKey = key;
        }
    }

    public boolean hasRows()
    {
        return maxSegmentRowId >= 0;
    }

    private ByteComparable asComparableBytes(DecoratedKey key, Clustering clustering)
    {
        return v -> new ByteSource()
        {
            ByteSource source = key.asComparableBytes(v);
            int index = -1;

            @Override
            public int next()
            {
                if (index == clustering.size())
                    return END_OF_STREAM;

                int b = source.next();
                if (b > END_OF_STREAM)
                    return b;

                if (++index == clustering.size())
                    return v == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM : ByteSource.TERMINATOR;
                source = ByteSource.of(clustering.accessor(), clustering.get(index), v);
                return NEXT_COMPONENT;
            }
        };
    }
}
