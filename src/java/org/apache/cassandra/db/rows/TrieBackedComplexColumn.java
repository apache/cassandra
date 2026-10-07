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
package org.apache.cassandra.db.rows;

import java.util.Iterator;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieEntriesWalker;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.mergeTombstoneRanges;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noExistingSelfDeletion;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noIncomingSelfDeletion;

/**
 * The data for a complex column, that is its cells and potential complex deletion time.
 */
public class TrieBackedComplexColumn extends ComplexColumnData
{
    private final DeletionAwareTrie<Object, TrieTombstoneMarker> data;
    private volatile int cellCount = -1;

    TrieBackedComplexColumn(ColumnMetadata column, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        super(column);
        assert column.isComplex();
        this.data = data;
    }

    @Override
    public boolean hasCells() {
        return cellsCount() > 0;
    }

    @Override
    public int cellsCount()
    {
        // If this class is used by multiple threads (which would be quite rare), the code below can race but the race
        // is benign -- at worst we can walk the trie multiple times.
        if (cellCount < 0)
            cellCount = Iterators.size(data.contentOnlyTrie().filteredValuesIterator(Direction.FORWARD, CellData.class));
        return cellCount;
    }

    @Override
    public Cell<?> getCell(CellPath path)
    {
        Object cell = data.get(TrieBackedRow.cellKey(-1, column, path));
        if (cell == null || cell instanceof Cell)
            return (Cell<?>) cell;
        return ((CellData<?, ?>) cell).toCell(column, path);
    }

    /// @inheritDoc The implementation is O(idx) as it has to walk the trie to find the value. Use sparingly.
    @Override
    public Cell<?> getCellByIndex(int idx)
    {
        Map.Entry<ByteComparable.Preencoded, CellData> entry = Iterators.get(data.contentOnlyTrie().filteredEntryIterator(Direction.FORWARD, CellData.class), idx, null);
        if (entry == null)
            throw new IndexOutOfBoundsException();
        return cellDataToCell(entry.getValue(), entry.getKey());
    }

    private Cell<?> cellDataToCell(CellData<?, ?> value, byte[] keyBytes, int keyLength)
    {
        if (value instanceof Cell || value == null)
            return (Cell<?>) value;
        return value.toCell(column, TrieBackedRow.cellPath(column, ByteSource.preencoded(keyBytes, 0, keyLength)));
    }

    private Cell<?> cellDataToCell(CellData<?, ?> value, ByteComparable.Preencoded key)
    {
        if (value instanceof Cell || value == null)
            return (Cell<?>) value;
        return value.toCell(column, TrieBackedRow.cellPath(column, key.getPreencodedBytes()));
    }

    @VisibleForTesting
    public CellData<?, ?> getCellWithoutPath(CellPath path)
    {
        return (CellData<?, ?>) data.contentOnlyTrie().get(TrieBackedRow.cellKey(-1, column, path));
    }

    @Override
    public DeletionTime complexDeletion()
    {
        return TrieTombstoneMarker.applicableDeletionOrLiveAtRoot(data);
    }

    class CellsWithPath extends TrieEntriesIterator.WithNullFiltering<Object, Cell<?>>
    {
        protected CellsWithPath(Trie<Object> trie, Direction direction)
        {
            super(trie, direction);
        }

        @Override
        protected Cell<?> mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (!(content instanceof CellData))
                return null;

            return cellDataToCell((CellData<?, ?>) content, bytes, byteLength);
        }
    }

    @Override
    public Iterator<Cell<?>> iterator()
    {
        return new CellsWithPath(data.contentOnlyTrie(), Direction.FORWARD);
    }

    @Override
    public Iterator<Cell<?>> reverseIterator()
    {
        return new CellsWithPath(data.contentOnlyTrie(), Direction.REVERSE);
    }

    @Override
    public long accumulate(LongAccumulator<Cell<?>> accumulator, long initialValue)
    {
        class Accumulator extends TrieEntriesWalker<Object, Accumulator>
        {
            long longValue = initialValue;

            @Override
            protected void content(Object content, byte[] bytes, int byteLength)
            {
                if (!(content instanceof CellData))
                    return;

                Cell<?> c = cellDataToCell((CellData<?, ?>) content, bytes, byteLength);
                longValue = accumulator.apply(c, longValue);
            }

            @Override
            public Accumulator complete()
            {
                return this;
            }
        }
        return data.process(Direction.FORWARD, new Accumulator()).longValue;
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, Cell<?>> accumulator, A arg, long initialValue)
    {
        class Accumulator extends TrieEntriesWalker<Object, Accumulator>
        {
            long longValue = initialValue;

            @Override
            protected void content(Object content, byte[] bytes, int byteLength)
            {
                if (!(content instanceof CellData))
                    return;

                Cell<?> c = cellDataToCell((CellData<?, ?>) content, bytes, byteLength);
                longValue = accumulator.apply(arg, c, longValue);
            }

            @Override
            public Accumulator complete()
            {
                return this;
            }
        }
        return data.process(Direction.FORWARD, new Accumulator()).longValue;
    }

    @Override
    public int dataSize()
    {
        int size = complexDeletion().dataSize();
        for (Cell<?> cell : this)
            size += cell.dataSize();
        return size;
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return accumulate((cell, v) -> v + cell.unsharedHeapSizeExcludingData(), 
                          complexDeletion().unsharedHeapSize());
    }

    @Override
    public long unsharedHeapSize()
    {
        return accumulate((cell, v) -> v + cell.unsharedHeapSize(), 
                          complexDeletion().unsharedHeapSize());
    }

    @Override
    public void validate()
    {
        DeletionTime complexDel = complexDeletion();
        if (!complexDel.validate())
            throw new AssertionError("Invalid complex deletion: " + complexDel);
        
        for (Cell<?> cell : this)
            cell.validate();
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        DeletionTime complexDel = complexDeletion();
        if (!complexDel.validate())
            return true;
        
        for (Cell<?> cell : this)
            if (cell.hasInvalidDeletions())
                return true;
        
        return false;
    }

    @Override
    public TrieBackedComplexColumn markCounterLocalToBeCleared()
    {
        if (!column.isCounterColumn())
            return this;
        
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValues(
            (Object x) ->
            {
                if (x instanceof CellData)
                {
                    CellData<?, ?> c = (CellData<?, ?>) x;
                    return c.markCounterLocalToBeCleared();
                }
                else
                    return x;   // complex column marker
            });
        
        return new TrieBackedComplexColumn(column, mappedData);
    }

    @Override
    public TrieBackedComplexColumn purge(DeletionPurger purger, long nowInSec)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof CellData)
                {
                    CellData<?, ?> c = (CellData<?, ?>) x;
                    return c.purge(purger, nowInSec);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(deletion -> purger.shouldPurge(deletion) ? null : deletion));

        return new TrieBackedComplexColumn(column, mappedData);
    }

    @Override
    public TrieBackedComplexColumn purgeDataOlderThan(long timestamp)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof CellData)
                {
                    CellData<?, ?> c = (CellData<?, ?>) x;
                    return c.purgeDataOlderThan(timestamp);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(deletion -> deletion.markedForDeleteAt() < timestamp ? null : deletion));
        
        return new TrieBackedComplexColumn(column, mappedData);
    }

    @Override
    public int estimateCloneSize(Cloner cloner)
    {
        return (int) accumulate((c, v) -> v + cloner.estimateCloneSize(c), 0);
    }

    @Override
    public ComplexColumnData clone(Cloner cloner)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie = TrieBackedRow.newTrie();
        try
        {
            newTrie.mutator(((ex, toClone) -> toClone instanceof CellData ? ((CellData<?, ?>) toClone).clone(cloner) : toClone),
                            mergeTombstoneRanges(),
                            noIncomingSelfDeletion(),
                            noExistingSelfDeletion(),
                            true,
                            Predicates.alwaysFalse())
                   .apply(data);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        return new TrieBackedComplexColumn(column, newTrie);
    }

    @Override
    public TrieBackedComplexColumn updateAllTimestamp(long newTimestamp)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof CellData)
                {
                    CellData<?, ?> c = (CellData<?, ?>) x;
                    return c.updateAllTimestamp(newTimestamp);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(deletion -> deletion.isLive() ? deletion : DeletionTime.build(newTimestamp - 1, deletion.localDeletionTime())));
        
        return new TrieBackedComplexColumn(column, mappedData);
    }

    @Override
    public long maxTimestamp()
    {
        long maxTs = complexDeletion().markedForDeleteAt();
        return accumulate((cell, v) -> Math.max(v, cell.timestamp()), maxTs);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if(!(other instanceof ComplexColumnData))
            return false;

        ComplexColumnData that = (ComplexColumnData)other;
        return this.column().equals(that.column())
               && this.complexDeletion().equals(that.complexDeletion())
               && Iterables.elementsEqual(this, that);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return String.format("[%s=%s %s]",
                             column().name,
                             complexDeletion(),
                             Iterators.toString(iterator()));
    }
}
