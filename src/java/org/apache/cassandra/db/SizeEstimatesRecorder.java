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
package org.apache.cassandra.db;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * A very simplistic/crude partition count/size estimator.
 *
 * Exposing per-primary-range estimated partitions count and size in CQL form,
 * as a direct CQL alternative to Thrift's describe_splits_ex().
 *
 * Estimates (per primary range) are calculated and dumped into a system table (system.size_estimates) every 5 minutes.
 *
 * See CASSANDRA-7688.
 */
public class SizeEstimatesRecorder extends MigrationListener implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SizeEstimatesRecorder.class);

    public static final SizeEstimatesRecorder instance = new SizeEstimatesRecorder();

    private SizeEstimatesRecorder()
    {
        MigrationManager.instance.register(this);
    }

    public void run()
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap();
        if (!metadata.isMember(FBUtilities.getBroadcastAddress()))
        {
            logger.debug("Node is not part of the ring; not recording size estimates");
            return;
        }

        logger.trace("Recording size estimates");

        // find primary token ranges for the local node.
        Collection<Token> localTokens = StorageService.instance.getLocalTokens();
        Collection<Range<Token>> localRanges = metadata.getPrimaryRangesFor(localTokens);

        for (Keyspace keyspace : Keyspace.nonSystem())
        {
            for (ColumnFamilyStore table : keyspace.getColumnFamilyStores())
            {
                long start = System.nanoTime();
                recordSizeEstimates(table, localRanges);
                long passed = System.nanoTime() - start;
                logger.trace("Spent {} milliseconds on estimating {}.{} size",
                             TimeUnit.NANOSECONDS.toMillis(passed),
                             table.metadata.ksName,
                             table.metadata.cfName);
            }
        }
    }

    @SuppressWarnings("resource")
    private void recordSizeEstimates(ColumnFamilyStore table, Collection<Range<Token>> localRanges)
    {
        List<Range<Token>> unwrappedRanges = Range.normalize(localRanges);
        // for each local primary range, estimate (crudely) mean partition size and partitions count.
        Map<Range<Token>, Pair<Long, Long>> estimates = new HashMap<>(localRanges.size());
        for (Range<Token> range : unwrappedRanges)
        {
            // filter sstables that have partitions in this range.
            Refs<SSTableReader> refs = null;
            long partitionsCount, meanPartitionSize;

            try
            {
                while (refs == null)
                {
                    ColumnFamilyStore.ViewFragment view = table.select(View.select(SSTableSet.CANONICAL, Range.makeRowRange(range)));
                    refs = Refs.tryRef(view.sstables);
                }

                // calculate the estimates.
                partitionsCount = estimatePartitionsCount(refs, range);
                meanPartitionSize = estimateMeanPartitionSize(refs);
            }
            finally
            {
                if (refs != null)
                    refs.release();
            }

            estimates.put(range, Pair.create(partitionsCount, meanPartitionSize));
        }

        // atomically update the estimates.
        SystemKeyspace.updateSizeEstimates(table.metadata.ksName, table.metadata.cfName, estimates);
    }

    private long estimatePartitionsCount(Collection<SSTableReader> sstables, Range<Token> range)
    {
        long count = 0;
        for (SSTableReader sstable : sstables)
            count += sstable.estimatedKeysForRanges(Collections.singleton(range));
        return count;
    }

    private long estimateMeanPartitionSize(Collection<SSTableReader> sstables)
    {
        long sum = 0, count = 0;
        for (SSTableReader sstable : sstables)
        {
            long n = sstable.getEstimatedPartitionSize().count();
            sum += sstable.getEstimatedPartitionSize().mean() * n;
            count += n;
        }
        return count > 0 ? sum / count : 0;
    }

    @Override
    public void onDropColumnFamily(String keyspace, String table)
    {
        SystemKeyspace.clearSizeEstimates(keyspace, table);
    }
}
