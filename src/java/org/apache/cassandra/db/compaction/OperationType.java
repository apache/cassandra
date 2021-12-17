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

import com.google.common.base.Predicate;

/**
 * The types of operations that can be observed with {@link AbstractTableOperation} and tracked by
 * {@link org.apache.cassandra.db.lifecycle.LifecycleTransaction}.
 * <p/>
 * Historically these operations have been broadly described as "compactions", even though they have
 * nothing to do with actual compactions. Any operation that can report progress and that normally
 * involves files, either for reading or writing, is a valid operation.
 */
public enum OperationType
{
    /** Each modification here should be also applied to {@link org.apache.cassandra.tools.nodetool.Stop#compactionType} */
    COMPACTION("Compaction"),
    VALIDATION("Validation"),
    KEY_CACHE_SAVE("Key cache save"),
    ROW_CACHE_SAVE("Row cache save"),
    COUNTER_CACHE_SAVE("Counter cache save"),
    CLEANUP("Cleanup"),
    SCRUB("Scrub"),
    UPGRADE_SSTABLES("Upgrade sstables"),
    INDEX_BUILD("Secondary index build"),
    /** Compaction for tombstone removal */
    TOMBSTONE_COMPACTION("Tombstone Compaction"),
    UNKNOWN("Unknown compaction type"),
    ANTICOMPACTION("Anticompaction after repair"),
    VERIFY("Verify"),
    FLUSH("Flush"),
    STREAM("Stream"),
    WRITE("Write"),
    VIEW_BUILD("View build"),
    INDEX_SUMMARY("Index summary redistribution"),
    RELOCATE("Relocate sstables to correct disk"),
    GARBAGE_COLLECT("Remove deleted data"),
    RESTORE("Restore"),
    // operations used for sstables on remote storage
    REMOTE_RELOAD("Remote reload", true), // reload locally sstables that already exist remotely
    REMOTE_COMPACTION("Remote compaction", true), // no longer used, kept for backward compatibility
    TRUNCATE_TABLE("Table truncated"),
    DROP_TABLE("Table dropped"),
    REMOVE_UNREADEABLE("Remove unreadable sstables"),
    REGION_BOOTSTRAP("Region Bootstrap"),
    REGION_DECOMMISSION("Region Decommission"),
    REGION_REPAIR("Region Repair"),
    SSTABLE_DISCARD("Local-only sstable discard", true);

    public final String type;
    public final String fileName;
    /** true if the transaction of this type should NOT be uploaded remotely */
    public final boolean localOnly;

    OperationType(String type)
    {
        this(type, false);
    }

    OperationType(String type, boolean localOnly)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
        this.localOnly = localOnly;
    }

    public static OperationType fromFileName(String fileName)
    {
        for (OperationType opType : OperationType.values())
            if (opType.fileName.equals(fileName))
                return opType;

        throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
    }

    public boolean isCacheSave()
    {
        return this == COUNTER_CACHE_SAVE || this == KEY_CACHE_SAVE || this == ROW_CACHE_SAVE;
    }

    public String toString()
    {
        return type;
    }

    public static final Predicate<OperationType> EXCEPT_VALIDATIONS = o -> o != VALIDATION;
    public static final Predicate<OperationType> COMPACTIONS_ONLY = o -> o == COMPACTION || o == TOMBSTONE_COMPACTION;
    public static final Predicate<OperationType> REWRITES_SSTABLES = o -> o == COMPACTION || o == CLEANUP || o == SCRUB ||
                                                                          o == TOMBSTONE_COMPACTION || o == ANTICOMPACTION ||
                                                                          o == UPGRADE_SSTABLES || o == RELOCATE ||
                                                                          o == GARBAGE_COLLECT;
}
