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

import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A very simplistic/crude partition count/size estimator.
 *
 * Exposing per-primary-range estimated partitions count and size in CQL form.
 *
 * Estimates (per primary range) are calculated and dumped into a system table (system.size_estimates) every 5 minutes.
 *
 * See CASSANDRA-7688.
 */
public class SizeEstimatesRecorder implements SchemaChangeListener, Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SizeEstimatesRecorder.class);

    public static final SizeEstimatesRecorder instance = new SizeEstimatesRecorder();

    private SizeEstimatesRecorder()
    {
        Schema.instance.registerListener(this);
    }

    public void run()
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap();
        if (!metadata.isMember(FBUtilities.getBroadcastAddressAndPort()))
        {
            logger.debug("Node is not part of the ring; not recording size estimates");
            return;
        }

        logger.trace("Recording size estimates");

        for (Keyspace keyspace : Keyspace.nonLocalStrategy())
        {
            // In tools the call to describe_splits_ex() used to be coupled with the call to describe_local_ring() so
            // most access was for the local primary range; after creating the size_estimates table this was changed
            // to be the primary range.
            // In a multi-dc setup its not uncommon for the local ring to be offset by 1 for the next DC; example:
            // DC1: [0, 10, 20, 30]
            // DC2: [1, 11, 21, 31]
            // DC3: [2, 12, 22, 32]
            // When working with the primary ring we have:
            // [0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32]
            // this then leads to primrary ranges with one token in it, which cause the estimates to be less useful.
            // Since only one range was published some tools make this assumption; for this reason we can't publish
            // all ranges (including the replica ranges) nor can we keep backwards compatability and publish primary
            // range.  If we publish multiple ranges downstream integrations may start to see duplicate data.
            // See CASSANDRA-15637
            Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRanges(keyspace.getName());
            Collection<Range<Token>> localPrimaryRanges = StorageService.instance.getLocalPrimaryRange();
            boolean rangesAreEqual = primaryRanges.equals(localPrimaryRanges);
            for (ColumnFamilyStore table : keyspace.getColumnFamilyStores())
            {
                long start = nanoTime();

                // compute estimates for primary ranges for backwards compatability
                Map<Range<Token>, Pair<Long, Long>> estimates = computeSizeEstimates(table, primaryRanges);
                SystemKeyspace.updateSizeEstimates(table.metadata.keyspace, table.metadata.name, estimates);
                SystemKeyspace.updateTableEstimates(table.metadata.keyspace, table.metadata.name, SystemKeyspace.TABLE_ESTIMATES_TYPE_PRIMARY, estimates);

                if (!rangesAreEqual)
                {
                    // compute estimate for local primary range
                    estimates = computeSizeEstimates(table, localPrimaryRanges);
                }
                SystemKeyspace.updateTableEstimates(table.metadata.keyspace, table.metadata.name, SystemKeyspace.TABLE_ESTIMATES_TYPE_LOCAL_PRIMARY, estimates);

                long passed = nanoTime() - start;
                if (logger.isTraceEnabled())
                    logger.trace("Spent {} milliseconds on estimating {}.{} size",
                                 TimeUnit.NANOSECONDS.toMillis(passed),
                                 table.metadata.keyspace,
                                 table.metadata.name);
            }
        }
    }

    private static Map<Range<Token>, Pair<Long, Long>> computeSizeEstimates(ColumnFamilyStore table, Collection<Range<Token>> ranges)
    {
        // for each local primary range, estimate (crudely) mean partition size and partitions count.
        Map<Range<Token>, Pair<Long, Long>> estimates = new HashMap<>(ranges.size());
        for (Range<Token> localRange : ranges)
        {
            for (Range<Token> unwrappedRange : localRange.unwrap())
            {
                // filter sstables that have partitions in this range.
                Refs<SSTableReader> refs = null;
                long partitionsCount, meanPartitionSize;

                try
                {
                    while (refs == null)
                    {
                        Iterable<SSTableReader> sstables = table.getTracker().getView().select(SSTableSet.CANONICAL);
                        SSTableIntervalTree tree = SSTableIntervalTree.build(sstables);
                        Range<PartitionPosition> r = Range.makeRowRange(unwrappedRange);
                        Iterable<SSTableReader> canonicalSSTables = View.sstablesInBounds(r.left, r.right, tree);
                        refs = Refs.tryRef(canonicalSSTables);
                    }

                    // calculate the estimates.
                    partitionsCount = estimatePartitionsCount(refs, unwrappedRange);
                    meanPartitionSize = estimateMeanPartitionSize(refs);
                }
                finally
                {
                    if (refs != null)
                        refs.release();
                }

                estimates.put(unwrappedRange, Pair.create(partitionsCount, meanPartitionSize));
            }
        }

        return estimates;
    }

    private static long estimatePartitionsCount(Collection<SSTableReader> sstables, Range<Token> range)
    {
        long count = 0;
        for (SSTableReader sstable : sstables)
            count += sstable.estimatedKeysForRanges(Collections.singleton(range));
        return count;
    }

    private static long estimateMeanPartitionSize(Collection<SSTableReader> sstables)
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
    public void onDropTable(TableMetadata table, boolean dropData)
    {
        SystemKeyspace.clearEstimates(table.keyspace, table.name);
    }
}
