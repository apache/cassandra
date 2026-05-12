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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;

/**
 * Factory and container for creating multiple {@link TrackedImportTransfer} instances from a collection
 * of SSTables, partitioned by {@link MutationTrackingService.KeyspaceShards}, which are aligned to replica ownership
 * ranges. Each shard receives its own CoordinatedTransfer instance, which can be executed independently.
 */
public class TrackedImportTransfers implements Iterable<TrackedImportTransfer>
{
    private final Collection<TrackedImportTransfer> transfers;

    private TrackedImportTransfers(Collection<TrackedImportTransfer> transfers)
    {
        this.transfers = transfers;
    }

    static TrackedImportTransfers create(String keyspace, MutationTrackingService.KeyspaceShards shards, Collection<SSTableReader> sstables, ConsistencyLevel cl)
    {
        // Clean up incoming SSTables to remove any existing untrusted CoordinatorLogOffsets
        for (SSTableReader sstable : sstables)
        {
            try
            {
                sstable.mutateCoordinatorLogOffsetsAndReload(ImmutableCoordinatorLogOffsets.NONE);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        SSTableIntervalTree intervals = SSTableIntervalTree.buildSSTableIntervalTree(sstables);
        List<TrackedImportTransfer> transfers = new ArrayList<>();

        shards.forEachShard(shard -> {
            Range<Token> range = shard.tokenRange();
            Collection<SSTableReader> sstablesForRange = intervals.search(Interval.create(range.left.minKeyBound(), range.right.maxKeyBound()));
            List<Range<Token>> ranges = Collections.singletonList(range);
            Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionForSSTables = new HashMap<>();
            sstablesForRange.removeIf(sstable -> {
                List<SSTableReader.PartitionPositionBounds> position = sstable.getPositionsForRanges(ranges);
                if (!position.isEmpty())
                    positionForSSTables.put(sstable, position);
                return position.isEmpty();
            });

            if (sstablesForRange.isEmpty())
                return;

            TrackedImportTransfer transfer = new TrackedImportTransfer(keyspace, range, shard.participants, sstablesForRange, positionForSSTables, cl, shard::nextId);
            transfers.add(transfer);
        });
        return new TrackedImportTransfers(transfers);
    }

    @Override
    public Iterator<TrackedImportTransfer> iterator()
    {
        return transfers.iterator();
    }

    @Override
    public String toString()
    {
        return "CoordinatedTransfers{transfers=" + transfers + '}';
    }
}
