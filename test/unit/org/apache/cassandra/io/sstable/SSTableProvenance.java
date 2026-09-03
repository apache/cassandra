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

package org.apache.cassandra.io.sstable;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

/**
 * Convenience enum for testing sstable metadata indicating if it came from the journal, the commit log, or both
 */
public enum SSTableProvenance
{
    MUTATION_JOURNAL,
    COMMIT_LOG,
    INDETERMINATE,
    BOTH;

    public static SSTableProvenance of(SSTableReader sstable)
    {
        return of(sstable.getSSTableMetadata());
    }

    public static SSTableProvenance of(StatsMetadata metadata)
    {
        boolean journal = !metadata.coordinatorLogOffsets.isEmpty();
        boolean commitLog = !metadata.commitLogIntervals.isEmpty();

        if (journal && commitLog)
            return BOTH;
        if (journal)
            return MUTATION_JOURNAL;
        if (commitLog)
            return COMMIT_LOG;
        return INDETERMINATE;
    }
}
