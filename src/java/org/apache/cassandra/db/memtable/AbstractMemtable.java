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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class AbstractMemtable implements Memtable
{
    protected final AtomicLong liveDataSize = new AtomicLong(0);
    protected final AtomicLong currentOperations = new AtomicLong(0);
    protected final ColumnsCollector columnsCollector;
    protected final StatsCollector statsCollector = new StatsCollector();
    // The smallest timestamp for all partitions stored in this memtable
    protected AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);
    private final AtomicReference<LifecycleTransaction> flushTransaction = new AtomicReference<>(null);
    protected TableMetadataRef metadata;

    public AbstractMemtable(TableMetadataRef metadataRef)
    {
        this.metadata = metadataRef;
        this.columnsCollector = new ColumnsCollector(metadata.get().regularAndStaticColumns());
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    public long getMinTimestamp()
    {
        return minTimestamp.get();
    }

    protected void updateMin(AtomicLong minTracker, long newValue)
    {
        while (true)
        {
            long memtableMinTimestamp = minTracker.get();
            if (memtableMinTimestamp <= newValue)
                break;
            if (minTracker.compareAndSet(memtableMinTimestamp, newValue))
                break;
        }
    }

    RegularAndStaticColumns columns()
    {
        return columnsCollector.get();
    }

    EncodingStats encodingStats()
    {
        return statsCollector.get();
    }

    public LifecycleTransaction getFlushTransaction()
    {
        return flushTransaction.get();
    }

    public LifecycleTransaction setFlushTransaction(LifecycleTransaction flushTransaction)
    {
        return this.flushTransaction.getAndSet(flushTransaction);
    }

    protected static class ColumnsCollector
    {
        private final HashMap<ColumnMetadata, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnMetadata> extra = new ConcurrentSkipListSet<>();

        ColumnsCollector(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnMetadata def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata s : columns.statics)
                update(s);
            for (ColumnMetadata r : columns.regulars)
                update(r);
        }

        private void update(ColumnMetadata definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        /**
         * Get the current state of the columns set.
         *
         * Note: If this is executed while mutations are still being performed on the table (e.g. to prepare
         * an sstable for streaming when Memtable.Factory.streamFromMemtable() is true), the resulting view may be
         * in a somewhat inconsistent state (it may include partial updates, as well as miss updates older than
         * ones it does include).
         */
        public RegularAndStaticColumns get()
        {
            RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
            for (Map.Entry<ColumnMetadata, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }

    protected static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }

    protected abstract class AbstractFlushCollection<P extends Partition> implements FlushCollection<P>
    {
        public long dataSize()
        {
            return getLiveDataSize();
        }

        public CommitLogPosition commitLogLowerBound()
        {
            return AbstractMemtable.this.getCommitLogLowerBound();
        }

        public CommitLogPosition commitLogUpperBound()
        {
            return AbstractMemtable.this.getCommitLogUpperBound();
        }

        public EncodingStats encodingStats()
        {
            return AbstractMemtable.this.encodingStats();
        }

        public RegularAndStaticColumns columns()
        {
            return AbstractMemtable.this.columns();
        }
    }
}
