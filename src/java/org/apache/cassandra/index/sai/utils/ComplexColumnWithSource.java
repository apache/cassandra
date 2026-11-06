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

package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.CellSourceIdentifier;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.memory.Cloner;

public class ComplexColumnWithSource extends ComplexColumnData
{
    private final ComplexColumnData wrapped;
    private final CellSourceIdentifier sourceTable;

    public ComplexColumnWithSource(ComplexColumnData wrapped, CellSourceIdentifier sourceTable)
    {
        super(wrapped.column());
        this.wrapped = wrapped;
        this.sourceTable = sourceTable;
    }

    @Override
    public int dataSize()
    {
        return wrapped.dataSize();
    }

    @Override
    public long unsharedHeapSize()
    {
        return wrapped.unsharedHeapSize();
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return wrapped.unsharedHeapSizeExcludingData();
    }

    @Override
    public void validate()
    {
        wrapped.validate();
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        return wrapped.hasInvalidDeletions();
    }

    @Override
    public void digest(Digest digest)
    {
        wrapped.digest(digest);
    }

    @Override
    public ComplexColumnData clone(Cloner cloner)
    {
        return wrapIfNew(wrapped.clone(cloner));
    }

    @Override
    public int estimateCloneSize(Cloner cloner)
    {
        return wrapped.estimateCloneSize(cloner);
    }

    @Override
    public ComplexColumnData updateAllTimestamp(long newTimestamp)
    {
        return wrapIfNew(((ComplexColumnData) wrapped.updateAllTimestamp(newTimestamp)));
    }

    @Override
    public ComplexColumnData markCounterLocalToBeCleared()
    {
        return wrapIfNew(((ComplexColumnData) wrapped.markCounterLocalToBeCleared()));
    }

    @Override
    public boolean hasCells()
    {
        return wrapped.hasCells();
    }

    @Override
    public int cellsCount()
    {
        return wrapped.cellsCount();
    }

    private Cell<?> wrapCell(Cell<?> c)
    {
        return c != null ? new CellWithSource<>(c, sourceTable) : null;
    }

    @Override
    public Cell<?> getCell(CellPath path)
    {
        return wrapCell(wrapped.getCell(path));
    }

    @Override
    public Cell<?> getCellByIndex(int idx)
    {
        return wrapCell(wrapped.getCellByIndex(idx));
    }

    @Override
    public DeletionTime complexDeletion()
    {
        return wrapped.complexDeletion();
    }

    @Override
    public Iterator<Cell<?>> iterator()
    {
        return Iterators.transform(wrapped.iterator(), this::wrapCell);
    }

    @Override
    public Iterator<Cell<?>> reverseIterator()
    {
        return Iterators.transform(wrapped.reverseIterator(), this::wrapCell);
    }

    @Override
    public long accumulate(LongAccumulator<Cell<?>> accumulator, long initialValue)
    {
        return wrapped.accumulate((cell, v) -> accumulator.apply(wrapCell(cell), v), initialValue);
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, Cell<?>> accumulator, A arg, long initialValue)
    {
        return wrapped.accumulate((a, cell, v) -> accumulator.apply(a, wrapCell(cell), v), arg, initialValue);
    }

    @Override
    public ComplexColumnData purge(DeletionPurger purger, long nowInSec)
    {
        return wrapIfNew(wrapped.purge(purger, nowInSec));
    }

    @Override
    public ComplexColumnData purgeDataOlderThan(long timestamp)
    {
        return wrapIfNew(wrapped.purgeDataOlderThan(timestamp));
    }

    @Override
    public long maxTimestamp()
    {
        return wrapped.maxTimestamp();
    }


    private ComplexColumnData wrapIfNew(ComplexColumnData maybeNewCell)
    {
        if (maybeNewCell == null)
            return null;
        // If the source's method returned a reference to the same source, then
        // we can skip creating a new wrapper.
        if (maybeNewCell == this.wrapped)
            return this;
        return new ComplexColumnWithSource(maybeNewCell, sourceTable);
    }
}
