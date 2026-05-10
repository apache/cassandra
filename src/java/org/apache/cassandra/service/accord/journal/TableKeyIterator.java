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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;

import com.google.common.collect.AbstractIterator;

import accord.utils.Invariants;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.service.accord.AccordKeyspace.JournalColumns.getJournalKey;
import static org.apache.cassandra.service.accord.JournalKey.SUPPORT;

class TableKeyIterator extends AbstractIterator<JournalKey> implements CloseableIterator<JournalKey>
{
    private final UnfilteredPartitionIterator mergeIterator;
    private final ColumnFamilyStore.RefViewFragment view;

    TableKeyIterator(ColumnFamilyStore table, JournalKey min, JournalKey max, long minSegment)
    {
        Invariants.require((min != null && max != null) || min == max);
        view = table.selectAndReference(View.select(SSTableSet.LIVE, r -> (max == null || SUPPORT.compare(getJournalKey(r.getFirst()), max) <= 0)
                                                                          && (min == null || SUPPORT.compare(getJournalKey(r.getLast()), min) >= 0)
                                                                          && (r.getSSTableMetadata().coveredClustering.end().isArtificial() || LongType.instance.compose(r.getSSTableMetadata().coveredClustering.end().bufferAt(0)) >= minSegment)
        ));
        List<ISSTableScanner> scanners = new ArrayList<>();
        for (SSTableReader sstable : view.sstables)
        {

            if (min == null) scanners.add(sstable.getScanner());
            else
                scanners.add(sstable.getScanner(new Bounds(AccordKeyspace.JournalColumns.decorate(min), AccordKeyspace.JournalColumns.decorate(max))));
        }

        mergeIterator = view.sstables.isEmpty()
                        ? EmptyIterators.unfilteredPartition(table.metadata())
                        : UnfilteredPartitionIterators.merge(scanners, UnfilteredPartitionIterators.MergeListener.NOOP);
    }

    @CheckForNull
    protected JournalKey computeNext()
    {
        JournalKey ret = null;
        if (mergeIterator.hasNext())
        {
            try (UnfilteredRowIterator partition = mergeIterator.next())
            {
                ret = getJournalKey(partition.partitionKey());
                while (partition.hasNext())
                    partition.next();
            }
        }

        if (ret != null)
            return ret;
        else
            return endOfData();
    }

    @Override
    public void close()
    {
        mergeIterator.close();
        view.close();
    }
}
