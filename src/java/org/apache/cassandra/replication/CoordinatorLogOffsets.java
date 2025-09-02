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

package org.apache.cassandra.replication;

import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

/**
 * Mutation ID offsets present in this SSTable for each coordinator log, to determine whether an SSTable is reconciled
 * or not. This includes "plain" mutation IDs for regular writes, and "activation" mutation IDs for bulk transfers.
 * Bulk transfer IDs are kept separately because we expect to have very few of them, and they're materialized for fast
 * access on the read path.
 * <p>
 * Note that peers may have reconciled all mutations included in an SSTable, but {@link StatsMetadata#repairedAt} is
 * dependent on compaction timing, so "nodetool repair --validate" may report temporary disagreements on the repaired
 * set.
 */
public interface CoordinatorLogOffsets<O extends Offsets>
{
    /**
     * Iterable over {@link CoordinatorLogId}.
     */
    interface Mutations<O> extends Iterable<Long>
    {
        O offsets(long logId);
        int size();
        default boolean isEmpty()
        {
            return size() == 0;
        }
    }

    Mutations<O> mutations();

    default ActivatedTransfers transfers()
    {
        return ActivatedTransfers.EMPTY;
    }

    ImmutableCoordinatorLogOffsets NONE = new ImmutableCoordinatorLogOffsets.Builder(0).build();
}
