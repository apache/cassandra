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

import java.util.Set;
import javax.annotation.Nullable;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;

public interface ShardTracker
{
    Token shardStart();

    @Nullable
    Token shardEnd();

    Range<Token> shardSpan();

    double shardSpanSize();

    /**
     * Advance to the given token (e.g. before writing a key). Returns true if this resulted in advancing to a new
     * shard, and false otherwise.
     */
    boolean advanceTo(Token nextToken);

    int count();

    /**
     * Returns the fraction of the given token range's coverage that falls within this shard.
     * E.g. if the span covers two shards exactly and the current shard is one of them, it will return 0.5.
     */
    double fractionInShard(Range<Token> targetSpan);

    double rangeSpanned(PartitionPosition first, PartitionPosition last);

    int shardIndex();

    default long shardAdjustedKeyCount(Set<SSTableReader> sstables)
    {
        // Note: computationally non-trivial; can be optimized if we save start/stop shards and size per table.
        long shardAdjustedKeyCount = 0;
        for (SSTableReader sstable : sstables)
            shardAdjustedKeyCount += sstable.estimatedKeys() * fractionInShard(ShardManager.coveringRange(sstable));
        return shardAdjustedKeyCount;
    }

    default void applyTokenSpaceCoverage(SSTableWriter writer)
    {
        if (writer.getFirst() != null)
            writer.setTokenSpaceCoverage(rangeSpanned(writer.getFirst(), writer.getLast()));
    }
}
