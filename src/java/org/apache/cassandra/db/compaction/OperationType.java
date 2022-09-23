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

public enum OperationType
{
    /** Each modification here should be also applied to {@link org.apache.cassandra.tools.nodetool.Stop#compactionType} */
    P0("Cancel all operations", 0),

    // Automation or operator-driven tasks
    CLEANUP("Cleanup", 1),
    SCRUB("Scrub", 1),
    UPGRADE_SSTABLES("Upgrade sstables", 1),
    VERIFY("Verify", 1),
    MAJOR_COMPACTION("Major compaction", 1),
    RELOCATE("Relocate sstables to correct disk", 1),
    GARBAGE_COLLECT("Remove deleted data", 1),

    // Internal SSTable writing
    FLUSH("Flush", 1),
    WRITE("Write", 1),

    ANTICOMPACTION("Anticompaction after repair", 2),
    VALIDATION("Validation", 3),

    INDEX_BUILD("Secondary index build", 4),
    VIEW_BUILD("View build", 4),

    COMPACTION("Compaction", 5),
    TOMBSTONE_COMPACTION("Tombstone Compaction", 5), // Compaction for tombstone removal
    UNKNOWN("Unknown compaction type", 5),

    STREAM("Stream", 6),
    KEY_CACHE_SAVE("Key cache save", 6),
    ROW_CACHE_SAVE("Row cache save", 6),
    COUNTER_CACHE_SAVE("Counter cache save", 6),
    INDEX_SUMMARY("Index summary redistribution", 6);

    public final String type;
    public final String fileName;

    // As of now, priority takes part only for interrupting tasks to give way to operator-driven tasks.
    // Operation types that have a smaller number will be allowed to cancel ones that have larger numbers.
    //
    // Submitted tasks may be prioritised differently when forming a queue, if/when CASSANDRA-11218 is implemented.
    public final int priority;

    OperationType(String type, int priority)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
        this.priority = priority;
    }

    public static OperationType fromFileName(String fileName)
    {
        for (OperationType opType : OperationType.values())
            if (opType.fileName.equals(fileName))
                return opType;

        throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
    }

    public String toString()
    {
        return type;
    }

    public boolean writesData()
    {
        switch(this)
        {
            case COMPACTION:
            case CLEANUP:
            case SCRUB:
            case UPGRADE_SSTABLES:
            case TOMBSTONE_COMPACTION:
            case ANTICOMPACTION:
            case FLUSH:
            case STREAM:
            case WRITE:
            case RELOCATE:
            case GARBAGE_COLLECT:
                return true;
            default:
                return false;
        }
    }
}
