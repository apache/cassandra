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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
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
    public static final String TARGET_DIRECTORY = "targetDirectory";

    private final TableMetadata metadata;
    private final OperationType tasktype;
    private final long completed;
    private final long total;
    private final Unit unit;
    private final TimeUUID compactionId;
    private final ImmutableSet<SSTableReader> sstables;
    private final String targetDirectory;

    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, TimeUUID compactionId, Collection<? extends SSTableReader> sstables, String targetDirectory)
    {
        this.tasktype = tasktype;
        this.completed = completed;
        this.total = total;
        this.metadata = metadata;
        this.unit = unit;
        this.compactionId = compactionId;
        this.sstables = ImmutableSet.copyOf(sstables);
        this.targetDirectory = targetDirectory;
    }

    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, TimeUUID compactionId, Collection<SSTableReader> sstables, String targetDirectory)
    {
        this(metadata, tasktype, completed, total, Unit.BYTES, compactionId, sstables, targetDirectory);
    }

    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, TimeUUID compactionId, Collection<? extends SSTableReader> sstables)
    {
        this(metadata, tasktype, completed, total, Unit.BYTES, compactionId, sstables, null);
    }

    /**
     * Special compaction info where we always need to cancel the compaction - for example ViewBuilderTask where we don't know
     * the sstables at construction
     */
    public static CompactionInfo withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, TimeUUID compactionId)
    {
        return withoutSSTables(metadata, tasktype, completed, total, unit, compactionId, null);
    }

    /**
     * Special compaction info where we always need to cancel the compaction - for example AutoSavingCache where we don't know
     * the sstables at construction
     */
    public static CompactionInfo withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, TimeUUID compactionId, String targetDirectory)
    {
        return new CompactionInfo(metadata, tasktype, completed, total, unit, compactionId, ImmutableSet.of(), targetDirectory);
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long complete, long total)
    {
        return new CompactionInfo(metadata, tasktype, complete, total, unit, compactionId, sstables, targetDirectory);
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

    /**
     * Get the directories this compaction could possibly write to.
     *
     * @return the directories that we might write to, or empty list if we don't know the metadata
     * (like for index summary redistribution), or null if we don't have any disk boundaries
     */
    public List<File> getTargetDirectories()
    {
        if (metadata != null && !metadata.isIndex())
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(metadata.id);
            if (cfs != null)
                return cfs.getDirectoriesForFiles(sstables);
        }
        return Collections.emptyList();
    }

    public String targetDirectory()
    {
        if (targetDirectory == null)
            return "";

        try
        {
            return new File(targetDirectory).canonicalPath();
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Unable to resolve canonical path for " + targetDirectory);
        }
    }

    /**
     * Note that this estimate is based on the amount of data we have left to read - it assumes input
     * size == output size for a compaction, which is not really true, but should most often provide a worst case
     * remaining write size.
     */
    public long estimatedRemainingWriteBytes()
    {
        if (unit == Unit.BYTES && tasktype.writesData)
            return getTotal() - getCompleted();
        return 0;
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
        ret.put(TARGET_DIRECTORY, targetDirectory());
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
