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
    P0("Cancel all operations", false, 0),

    // Automation or operator-driven tasks
    CLEANUP("Cleanup", true, 1),
    SCRUB("Scrub", true, 1),
    UPGRADE_SSTABLES("Upgrade sstables", true, 1),
    VERIFY("Verify", false, 1),
    MAJOR_COMPACTION("Major compaction", true, 1),
    RELOCATE("Relocate sstables to correct disk", false, 1),
    GARBAGE_COLLECT("Remove deleted data", true, 1),

    // Internal SSTable writing
    FLUSH("Flush", true, 1),
    WRITE("Write", true, 1),

    ANTICOMPACTION("Anticompaction after repair", true, 2),
    VALIDATION("Validation", false, 3),

    INDEX_BUILD("Secondary index build", false, 4),
    VIEW_BUILD("View build", false, 4),

    COMPACTION("Compaction", true, 5),
    TOMBSTONE_COMPACTION("Tombstone Compaction", true, 5), // Compaction for tombstone removal
    UNKNOWN("Unknown compaction type", false, 5),

    STREAM("Stream", true, 6),
    KEY_CACHE_SAVE("Key cache save", false, 6),
    ROW_CACHE_SAVE("Row cache save", false, 6),
    COUNTER_CACHE_SAVE("Counter cache save", false, 6),
    INDEX_SUMMARY("Index summary redistribution", false, 6);

    public final String type;
    public final String fileName;

    /**
     * For purposes of calculating space for interim compactions in flight, whether or not this OperationType is expected
     * to write data to disk
     */
    public final boolean writesData;

    // As of now, priority takes part only for interrupting tasks to give way to operator-driven tasks.
    // Operation types that have a smaller number will be allowed to cancel ones that have larger numbers.
    //
    // Submitted tasks may be prioritised differently when forming a queue, if/when CASSANDRA-11218 is implemented.
    public final int priority;

    OperationType(String type, boolean writesData, int priority)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
        this.writesData = writesData;
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
}
