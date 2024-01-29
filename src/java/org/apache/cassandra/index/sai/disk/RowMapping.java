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

import java.util.Collections;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * In memory representation of {@link PrimaryKey} to row ID mappings which only contains
 * {@link Row} regardless of whether it's live or deleted. ({@link RangeTombstoneMarker} is not included.)
 * <p>
 * While this inherits the threading behaviour of {@link InMemoryTrie} of single-writer / multiple-reader,
 * since it is only used by {@link StorageAttachedIndexWriter}, which is not threadsafe, we can consider
 * this class not threadsafe as well.
 */
@NotThreadSafe
public class RowMapping
{
    private static final InMemoryTrie.UpsertTransformer<Long, Long> OVERWRITE_TRANSFORMER = (existing, update) -> update;

    public static final RowMapping DUMMY = new RowMapping()
    {
        @Override
        public Iterator<Pair<ByteComparable, LongArrayList>> merge(MemtableIndex index) { return Collections.emptyIterator(); }

        @Override
        public void complete() {}

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public void add(PrimaryKey key, long sstableRowId) {}

        @Override
        public int get(PrimaryKey key)
        {
            return -1;
        }
    };

    private final InMemoryTrie<Long> rowMapping = new InMemoryTrie<>(BufferType.OFF_HEAP);

    private boolean complete = false;

    public PrimaryKey minKey;
    public PrimaryKey maxKey;
    public PrimaryKey minStaticKey;
    public PrimaryKey maxStaticKey;

    public long maxSSTableRowId = -1;
    public long maxStaticSSTableRowId = -1;

    public int rowCount;
    public int staticRowCount;

    private RowMapping()
    {}

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
     * Link the term -> {@link PrimaryKeys} mappings from a provided {@link MemtableIndex} to
     * the {@link PrimaryKey} -> row ID mappings maintained here in {@link #rowMapping} to produce
     * mappings of terms to their postings lists.
     *
     * @param index a Memtable-attached column index
     *
     * @return an iterator of term -> postings list {@link Pair}s
     */
    public Iterator<Pair<ByteComparable, LongArrayList>> merge(MemtableIndex index)
    {
        assert complete : "RowMapping is not built.";

        Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        return new AbstractGuavaIterator<>()
        {
            @Override
            protected Pair<ByteComparable, LongArrayList> computeNext()
            {
                while (iterator.hasNext())
                {
                    Pair<ByteComparable, PrimaryKeys> pair = iterator.next();

                    LongArrayList postings = null;
                    Iterator<PrimaryKey> primaryKeys = pair.right.iterator();

                    while (primaryKeys.hasNext())
                    {
                        Long sstableRowId = rowMapping.get(primaryKeys.next());

                        // The in-memory index does not handle deletions, so it is possible to
                        // have a primary key in the index that doesn't exist in the row mapping
                        if (sstableRowId != null)
                        {
                            postings = postings == null ? new LongArrayList() : postings;
                            postings.add(sstableRowId);
                        }
                    }
                    if (postings != null)
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

    public boolean isComplete()
    {
        return complete;
    }

    /**
     * Include PrimaryKey to RowId mapping
     */
    public void add(PrimaryKey key, long sstableRowId) throws InMemoryTrie.SpaceExhaustedException
    {
        assert !complete : "Cannot modify built RowMapping.";

        rowMapping.putSingleton(key, sstableRowId, OVERWRITE_TRANSFORMER);

        // Data is written in primary key order. If a schema contains clustering keys, it may also contain static
        // columns. We track min, max, and count for static keys separately here so that we can pass them to the segment
        // metadata for indexes on static columns.
        if (key.kind() == PrimaryKey.Kind.STATIC)
        {
            if (minStaticKey == null)
                minStaticKey = key;
            maxStaticKey = key;
            staticRowCount++;
            maxStaticSSTableRowId = Math.max(maxStaticSSTableRowId, sstableRowId);
        }
        else
        {
            if (minKey == null)
                minKey = key;
            maxKey = key;
            rowCount++;
            maxSSTableRowId = Math.max(maxSSTableRowId, sstableRowId);
        }
    }

    /**
     * Returns the SSTable row ID for a {@link PrimaryKey}
     *
     * @param key the {@link PrimaryKey}
     * @return a valid SSTable row ID for the {@link PrimaryKey} or -1 if the {@link PrimaryKey} doesn't exist
     * in the {@link RowMapping}
     */
    public int get(PrimaryKey key)
    {
        Long sstableRowId = rowMapping.get(key);
        return sstableRowId == null ? -1 : Math.toIntExact(sstableRowId);
    }

    public boolean hasRows()
    {
        return maxSSTableRowId >= 0 || maxStaticSSTableRowId >= 0;
    }
}
