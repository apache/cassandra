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

package org.apache.cassandra.service.reads.range;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class RangeCommands
{
    private static final Logger logger = LoggerFactory.getLogger(RangeCommandIterator.class);

    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    /**
     * Introduce a maximum number of sub-ranges that the coordinator can request in parallel for range queries. Previously
     * we would request up to the maximum number of ranges but this causes problems if the number of vnodes is large.
     * By default we pick 10 requests per core, assuming all replicas have the same number of cores. The idea is that we
     * don't want a burst of range requests that will back up, hurting all other queries. At the same time,
     * we want to give range queries a chance to run if resources are available.
     */
    private static final int MAX_CONCURRENT_RANGE_REQUESTS = Math.max(1, CassandraRelevantProperties.MAX_CONCURRENT_RANGE_REQUESTS.getInt(FBUtilities.getAvailableProcessors() * 10));

    public static PartitionIterator partitions(PartitionRangeReadCommand command,
                                               ConsistencyLevel consistencyLevel,
                                               long queryStartNanoTime)
    {
        // Note that in general, a RangeCommandIterator will honor the command limit for each range, but will not enforce it globally.
        RangeCommandIterator rangeCommands = rangeCommandIterator(command, consistencyLevel, queryStartNanoTime);
        return command.limits().filter(command.postReconciliationProcessing(rangeCommands),
                                       command.nowInSec(),
                                       command.selectsFullPartition(),
                                       command.metadata().enforceStrictLiveness());
    }

    @VisibleForTesting
    static RangeCommandIterator rangeCommandIterator(PartitionRangeReadCommand command,
                                                     ConsistencyLevel consistencyLevel,
                                                     long queryStartNanoTime)
    {
        Tracing.trace("Computing ranges to query");

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        ReplicaPlanIterator replicaPlans = new ReplicaPlanIterator(command.dataRange().keyRange(),
                                                                   command.indexQueryPlan(),
                                                                   keyspace,
                                                                   consistencyLevel);

        if (command.isTopK())
            return new ScanAllRangesCommandIterator(keyspace, replicaPlans, command, replicaPlans.size(), queryStartNanoTime);

        int maxConcurrencyFactor = Math.min(replicaPlans.size(), MAX_CONCURRENT_RANGE_REQUESTS);
        int concurrencyFactor = maxConcurrencyFactor;
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (queryPlan == null || queryPlan.shouldEstimateInitialConcurrency())
        {
            // our estimate of how many result rows there will be per-range
            float resultsPerRange = estimateResultsPerRange(command, keyspace);
            // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
            // fetch enough rows in the first round
            resultsPerRange -= resultsPerRange * CONCURRENT_SUBREQUESTS_MARGIN;
            concurrencyFactor = resultsPerRange == 0.0
                                ? 1
                                : Math.max(1, Math.min(maxConcurrencyFactor, (int) Math.ceil(command.limits().count() / resultsPerRange)));
            logger.trace("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                         resultsPerRange, command.limits().count(), replicaPlans.size(), concurrencyFactor);
            Tracing.trace("Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)",
                          replicaPlans.size(), concurrencyFactor, resultsPerRange);
        }
        else
        {
            logger.trace("Max concurrent range requests: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                         MAX_CONCURRENT_RANGE_REQUESTS, command.limits().count(), replicaPlans.size(), concurrencyFactor);
            Tracing.trace("Submitting range requests on {} ranges with a concurrency of {}", replicaPlans.size(), concurrencyFactor);
        }

        ReplicaPlanMerger mergedReplicaPlans = new ReplicaPlanMerger(replicaPlans, keyspace, consistencyLevel);
        return new RangeCommandIterator(mergedReplicaPlans,
                                        command,
                                        concurrencyFactor,
                                        maxConcurrencyFactor,
                                        replicaPlans.size(),
                                        queryStartNanoTime);
    }

    /**
     * Estimate the number of result rows per range in the ring based on our local data.
     * <p>
     * This assumes that ranges are uniformly distributed across the cluster and
     * that the queried data is also uniformly distributed.
     */
    @VisibleForTesting
    static float estimateResultsPerRange(PartitionRangeReadCommand command, Keyspace keyspace)
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        Index.QueryPlan index = command.indexQueryPlan();
        float maxExpectedResults = index == null
                                   ? command.limits().estimateTotalResults(cfs)
                                   : index.getEstimatedResultRows();

        // adjust maxExpectedResults by the number of tokens this node has and the replication factor for this ks
        return (maxExpectedResults / DatabaseDescriptor.getNumTokens())
               / keyspace.getReplicationStrategy().getReplicationFactor().allReplicas;
    }

    /**
     * Added specifically to check for sufficient nodes live to serve partition denylist queries
     */
    public static boolean sufficientLiveNodesForSelectStar(TableMetadata metadata, ConsistencyLevel consistency)
    {
        try
        {
            Keyspace keyspace = Keyspace.open(metadata.keyspace);
            ReplicaPlanIterator rangeIterator = new ReplicaPlanIterator(DataRange.allData(metadata.partitioner).keyRange(),
                                                                        null,
                                                                        keyspace,
                                                                        consistency);

            // Called for the side effect of running assureSufficientLiveReplicasForRead.
            // Deliberately called with an invalid vnode count in case it is used elsewhere in the future..
            rangeIterator.forEachRemaining(r ->  ReplicaPlans.forRangeRead(keyspace, null, consistency, r.range(), -1));
            return true;
        }
        catch (UnavailableException e)
        {
            return false;
        }
    }
}
