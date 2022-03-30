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
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TimeUUID;

public final class CompactionInfo
{
    public static final String ID = "id";
    public static final String KEYSPACE = "keyspace";
    public static final String COLUMNFAMILY = "columnfamily";
    public static final String COMPLETED = "completed";
    public static final String TOTAL = "total";
    public static final String TASK_TYPE = "taskType";
    public static final String UNIT = "unit";
    public static final String COMPACTION_ID = "compactionId";
    public static final String SSTABLES = "sstables";

    private final TableMetadata metadata;
    private final OperationType tasktype;
    private final long completed;
    private final long total;
    private final Unit unit;
    private final TimeUUID compactionId;
    private final ImmutableSet<SSTableReader> sstables;

    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long bytesComplete, long totalBytes, TimeUUID compactionId, Collection<SSTableReader> sstables)
    {
        this(metadata, tasktype, bytesComplete, totalBytes, Unit.BYTES, compactionId, sstables);
    }

    private CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, TimeUUID compactionId, Collection<SSTableReader> sstables)
    {
        this.tasktype = tasktype;
        this.completed = completed;
        this.total = total;
        this.metadata = metadata;
        this.unit = unit;
        this.compactionId = compactionId;
        this.sstables = ImmutableSet.copyOf(sstables);
    }

    /**
     * Special compaction info where we always need to cancel the compaction - for example ViewBuilderTask and AutoSavingCache where we don't know
     * the sstables at construction
     */
    public static CompactionInfo withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, TimeUUID compactionId)
    {
        return new CompactionInfo(metadata, tasktype, completed, total, unit, compactionId, ImmutableSet.of());
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long complete, long total)
    {
        return new CompactionInfo(metadata, tasktype, complete, total, unit, compactionId, sstables);
    }

    public Optional<String> getKeyspace()
    {
        return Optional.ofNullable(metadata != null ? metadata.keyspace : null);
    }

    public Optional<String> getTable()
    {
        return Optional.ofNullable(metadata != null ? metadata.name : null);
    }

    public TableMetadata getTableMetadata()
    {
        return metadata;
    }

    public long getCompleted()
    {
        return completed;
    }

    public long getTotal()
    {
        return total;
    }

    public OperationType getTaskType()
    {
        return tasktype;
    }

    public TimeUUID getTaskId()
    {
        return compactionId;
    }

    public Unit getUnit()
    {
        return unit;
    }

    public Set<SSTableReader> getSSTables()
    {
        return sstables;
    }

    @Override
    public String toString()
    {
        if (metadata != null)
        {
            return String.format("%s(%s, %s / %s %s)@%s(%s, %s)",
                                 tasktype, compactionId, completed, total, unit,
                                 metadata.id, metadata.keyspace, metadata.name);
        }
        else
        {
            return String.format("%s(%s, %s / %s %s)",
                                 tasktype, compactionId, completed, total, unit);
        }
    }

    public Map<String, String> asMap()
    {
        Map<String, String> ret = new HashMap<String, String>();
        ret.put(ID, metadata != null ? metadata.id.toString() : "");
        ret.put(KEYSPACE, getKeyspace().orElse(null));
        ret.put(COLUMNFAMILY, getTable().orElse(null));
        ret.put(COMPLETED, Long.toString(completed));
        ret.put(TOTAL, Long.toString(total));
        ret.put(TASK_TYPE, tasktype.toString());
        ret.put(UNIT, unit.toString());
        ret.put(COMPACTION_ID, compactionId == null ? "" : compactionId.toString());
        ret.put(SSTABLES, Joiner.on(',').join(sstables));
        return ret;
    }

    boolean shouldStop(Predicate<SSTableReader> sstablePredicate)
    {
        if (sstables.isEmpty())
        {
            return true;
        }
        return sstables.stream().anyMatch(sstablePredicate);
    }

    public static abstract class Holder
    {
        private volatile boolean stopRequested = false;
        public abstract CompactionInfo getCompactionInfo();

        public void stop()
        {
            stopRequested = true;
        }

        /**
         * if this compaction involves several/all tables we can safely check globalCompactionsPaused
         * in isStopRequested() below
         */
        public abstract boolean isGlobal();

        public boolean isStopRequested()
        {
            return stopRequested || (isGlobal() && CompactionManager.instance.isGlobalCompactionPaused());
        }
    }

    public enum Unit
    {
        BYTES("bytes"), RANGES("token range parts"), KEYS("keys");

        private final String name;

        Unit(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return this.name;
        }

        public static boolean isFileSize(String unit)
        {
            return BYTES.toString().equals(unit);
        }
    }
}
