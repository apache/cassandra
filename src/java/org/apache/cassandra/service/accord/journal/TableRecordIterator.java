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

package org.apache.cassandra.service.accord.journal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.StorageHook;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.io.sstable.SSTableReadsListener.NOOP_LISTENER;

class TableRecordIterator implements Closeable, RecordConsumer<JournalKey>
{
    final JournalKey key;
    final List<UnfilteredRowIterator> unmerged;
    final UnfilteredRowIterator merged;

    long segment;
    int offset;
    ByteBuffer value;
    int userVersion;

    TableRecordIterator(JournalKey key, List<UnfilteredRowIterator> unmerged, UnfilteredRowIterator merged)
    {
        this.key = key;
        this.unmerged = unmerged;
        this.merged = merged;
    }

    static TableRecordIterator all(ColumnFamilyStore cfs, JournalKey key, OpOrder.Group readOrder)
    {
        DecoratedKey pk = AccordKeyspace.JournalColumns.decorate(key);
        List<UnfilteredRowIterator> iters = new ArrayList<>(3);
        try
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, pk));
            for (SSTableReader sstable : view.sstables)
            {
                if (!sstable.mayContainAssumingKeyIsInRange(pk))
                    continue;

                UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs, sstable, pk, Slices.ALL, ColumnFilter.all(cfs.metadata()), false, NOOP_LISTENER);
                if (iter.getClass() != EmptyIterators.EmptyUnfilteredRowIterator.class)
                    iters.add(iter);
            }

            return new TableRecordIterator(key, iters, iters.isEmpty() ? null : UnfilteredRowIterators.merge(iters));
        }
        catch (Throwable t)
        {
            for (UnfilteredRowIterator iter : iters)
            {
                try { iter.close(); }
                catch (Throwable t2) { t.addSuppressed(t2); }
            }
            throw t;
        }
    }

    @Override
    public void accept(long segment, int offset, JournalKey key, ByteBuffer buffer, int userVersion)
    {
        this.segment = segment;
        this.offset = offset;
        this.value = buffer;
        this.userVersion = userVersion;
    }

    boolean advance()
    {
        if (merged == null || !merged.hasNext())
            return false;

        try
        {
            Row row = (Row) merged.next();
            segment = LongType.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(0)));
            offset = Int32Type.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(1)));
            value = row.getCell(AccordKeyspace.JournalColumns.record).buffer();
            userVersion = Int32Type.instance.compose(row.getCell(AccordKeyspace.JournalColumns.user_version).buffer());
            return true;
        }
        catch (Throwable t)
        {
            throw new FSReadError("Failed to read from " + unmerged, t);
        }
    }

    @Override
    public void close()
    {
        if (merged != null)
            merged.close();
    }
}
