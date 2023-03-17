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
    GARBAGE_COLLECT("Remove deleted data");

    public final String type;
    public final String fileName;

    OperationType(String type)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
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
