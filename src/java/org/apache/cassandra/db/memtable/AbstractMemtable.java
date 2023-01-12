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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

public abstract class AbstractMemtable implements Memtable
{
    protected final AtomicLong currentOperations = new AtomicLong(0);
    protected final ColumnsCollector columnsCollector;
    protected final StatsCollector statsCollector = new StatsCollector();
    // The smallest timestamp for all partitions stored in this memtable
    protected AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);
    // The smallest local deletion time for all partitions in this memtable
    protected AtomicInteger minLocalDeletionTime = new AtomicInteger(Integer.MAX_VALUE);
    // Note: statsCollector has corresponding statistics to the two above, but starts with an epoch value which is not
    // correct for their usage.

    protected TableMetadataRef metadata;

    public AbstractMemtable(TableMetadataRef metadataRef)
    {
        this.metadata = metadataRef;
        this.columnsCollector = new ColumnsCollector(metadata.get().regularAndStaticColumns());
    }

    @VisibleForTesting
    public AbstractMemtable(TableMetadataRef metadataRef, long minTimestamp)
    {
        this.metadata = metadataRef;
        this.columnsCollector = new ColumnsCollector(metadata.get().regularAndStaticColumns());
        this.minTimestamp = new AtomicLong(minTimestamp);
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public long operationCount()
    {
        return currentOperations.get();
    }

    /**
     * Returns the minTS if one available, otherwise NO_MIN_TIMESTAMP.
     *
     * EncodingStats uses a synthetic epoch TS at 2015. We don't want to leak that (CASSANDRA-18118) so we return NO_MIN_TIMESTAMP instead.
     *
     * @return The minTS or NO_MIN_TIMESTAMP if none available
     */
    public long getMinTimestamp()
    {
        return minTimestamp.get() != EncodingStats.NO_STATS.minTimestamp ? minTimestamp.get() : NO_MIN_TIMESTAMP;
    }

    public int getMinLocalDeletionTime()
    {
        return minLocalDeletionTime.get();
    }

    protected static void updateMin(AtomicLong minTracker, long newValue)
    {
        while (true)
        {
            long existing = minTracker.get();
            if (existing <= newValue)
                break;
            if (minTracker.compareAndSet(existing, newValue))
                break;
        }
    }

    protected static void updateMin(AtomicInteger minTracker, int newValue)
    {
        while (true)
        {
            int existing = minTracker.get();
            if (existing <= newValue)
                break;
            if (minTracker.compareAndSet(existing, newValue))
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

        public void update(ColumnsCollector other)
        {
            for (Map.Entry<ColumnMetadata, AtomicBoolean> v : other.predefined.entrySet())
                if (v.getValue().get())
                    update(v.getKey());

            extra.addAll(other.extra);
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

    protected abstract class AbstractFlushablePartitionSet<P extends Partition> implements FlushablePartitionSet<P>
    {
        public long dataSize()
        {
            return getLiveDataSize();
        }

        public CommitLogPosition commitLogLowerBound()
        {
            return AbstractMemtable.this.getCommitLogLowerBound();
        }

        public LastCommitLogPosition commitLogUpperBound()
        {
            return AbstractMemtable.this.getFinalCommitLogUpperBound();
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
