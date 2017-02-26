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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.schema.TableMetadata;

/** Implements serializable to allow structured info to be returned via JMX. */
public final class CompactionInfo implements Serializable
{
    private static final long serialVersionUID = 3695381572726744816L;

    public static final String ID = "id";
    public static final String KEYSPACE = "keyspace";
    public static final String COLUMNFAMILY = "columnfamily";
    public static final String COMPLETED = "completed";
    public static final String TOTAL = "total";
    public static final String TASK_TYPE = "taskType";
    public static final String UNIT = "unit";
    public static final String COMPACTION_ID = "compactionId";
    public static final String TARGET_DIRECTORY = "targetDirectory";

    // NON_COMPACTION_OPERATION is going to be used for printouts of the compactions activities that are not actual compactions
    private static final String NON_COMPACTION_OPERATION = "-------- Other operations --------";
    private final TableMetadata metadata;
    private final OperationType tasktype;
    private final long completed;
    private final long total;
    private final Unit unit;
    private final UUID compactionId;
    private final String targetDirectory;

    /**
     * Allow instantiating CompactionInfo object without targetDirectory; for operations without actual compactions
     * @params metadata, tasktype, how many bytes done, total bytes, compaction ID
     * @return CompactionInfo object
     */
    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long bytesComplete, long totalBytes, UUID compactionId)
    {
        this(metadata, tasktype, bytesComplete, totalBytes, Unit.BYTES, compactionId, NON_COMPACTION_OPERATION);
    }

    public static enum Unit
    {
        BYTES("bytes"), RANGES("token range parts"), KEYS("keys");

        private final String name;

        private Unit(String name)
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

    /**
     * Allow instantiating CompactionInfo object with targetDirectory; for operations with compactions, used in `nodetool compactionstats`
     * @params tasktype, how many bytes done, total bytes, compaction ID and targetDirectory
     * @return CompactionInfo object
     */
    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long bytesComplete, long totalBytes, UUID compactionId, String targetDirectory)
    {
        this(metadata, tasktype, bytesComplete, totalBytes, Unit.BYTES, compactionId, targetDirectory);
    }

    /**
     * Allow instantiating CompactionInfo object without targetDirectory; for operations without actual compactions
     * @params tasktype, how many bytes done, total bytes, unit and compaction ID
     * @return CompactionInfo object
     */
    public CompactionInfo(OperationType tasktype, long completed, long total, Unit unit, UUID compactionId)
    {
        this(null, tasktype, completed, total, unit, compactionId, NON_COMPACTION_OPERATION);
    }

    /**
     * Allow instantiating CompactionInfo object with targetDirectory; for operations with compactions, used in `nodetool compactionstats`
     * @params tasktype, how many bytes done, total bytes, unit, compaction ID and targetDirectory
     * @return CompactionInfo object
     */
    public CompactionInfo(OperationType tasktype, long completed, long total, Unit unit, UUID compactionId, String targetDirectory)
    {
        this(null, tasktype, completed, total, unit, compactionId, targetDirectory);
    }

    /**
     * Allow instantiating CompactionInfo object without targetDirectory; for operations without actual compactions
     * @params metadata, tasktype, how many bytes done, total bytes, unit and compaction ID
     * @return CompactionInfo object
     */
    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, UUID compactionId) {
        this(metadata, tasktype, completed, total, unit, compactionId, NON_COMPACTION_OPERATION);
    }

    // This is the constructor that will always get called eventually to initialise all fields.
    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, UUID compactionId, String targetDirectory)
    {
        this.tasktype = tasktype;
        this.completed = completed;
        this.total = total;
        this.metadata = metadata;
        this.unit = unit;
        this.compactionId = compactionId;

        // Will hold a target directory for compaction, used for printing in `nodetool compactionstats` to provide more info on compactions
        this.targetDirectory = targetDirectory;
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long complete, long total)
    {
        return new CompactionInfo(metadata, tasktype, complete, total, unit, compactionId, targetDirectory);
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

    public UUID getTaskId()
    {
        return compactionId;
    }

    public Unit getUnit()
    {
        return unit;
    }

    public String targetDirectory() { return targetDirectory; }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(getTaskType());
        if (metadata != null)
        {
            buff.append('@').append(metadata.id).append('(');
            buff.append(metadata.keyspace).append(", ").append(metadata.name).append(", ");
        }
        else
        {
            buff.append('(');
        }
        buff.append(getCompleted()).append('/').append(getTotal());
        return buff.append(')').append(unit).toString();
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
        ret.put(TARGET_DIRECTORY, targetDirectory == null ? "" : targetDirectory);
        return ret;
    }

    public static abstract class Holder
    {
        private volatile boolean stopRequested = false;
        public abstract CompactionInfo getCompactionInfo();

        public void stop()
        {
            stopRequested = true;
        }

        public boolean isStopRequested()
        {
            return stopRequested;
        }
    }
}
