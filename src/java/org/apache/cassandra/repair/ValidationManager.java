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

package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;

public class ValidationManager
{
    private static final Logger logger = LoggerFactory.getLogger(ValidationManager.class);

    public static final ValidationManager instance = new ValidationManager();

    private ValidationManager() {}

    private static MerkleTrees createMerkleTrees(ValidationPartitionIterator validationIterator, Collection<Range<Token>> ranges, ColumnFamilyStore cfs)
    {
        MerkleTrees tree = new MerkleTrees(cfs.getPartitioner());
        long allPartitions = validationIterator.estimatedPartitions();
        Map<Range<Token>, Long> rangePartitionCounts = validationIterator.getRangePartitionCounts();

        // The repair coordinator must hold RF trees in memory at once, so a given validation compaction can only
        // use 1 / RF of the allowed space.
        long availableBytes = (DatabaseDescriptor.getRepairSessionSpaceInMegabytes() * 1048576) /
                              cfs.keyspace.getReplicationStrategy().getReplicationFactor().allReplicas;

        for (Range<Token> range : ranges)
        {
            long numPartitions = rangePartitionCounts.get(range);
            double rangeOwningRatio = allPartitions > 0 ? (double)numPartitions / allPartitions : 0;
            // determine max tree depth proportional to range size to avoid blowing up memory with multiple tress,
            // capping at a depth that does not exceed our memory budget (CASSANDRA-11390, CASSANDRA-14096)
            int rangeAvailableBytes = Math.max(1, (int) (rangeOwningRatio * availableBytes));
            // Try to estimate max tree depth that fits the space budget assuming hashes of 256 bits = 32 bytes
            // note that estimatedMaxDepthForBytes cannot return a number lower than 1
            int estimatedMaxDepth = MerkleTree.estimatedMaxDepthForBytes(cfs.getPartitioner(), rangeAvailableBytes, 32);
            int maxDepth = rangeOwningRatio > 0
                           ? Math.min(estimatedMaxDepth, DatabaseDescriptor.getRepairSessionMaxTreeDepth())
                           : 0;
            // determine tree depth from number of partitions, capping at max tree depth (CASSANDRA-5263)
            int depth = numPartitions > 0 ? (int) Math.min(Math.ceil(Math.log(numPartitions) / Math.log(2)), maxDepth) : 0;
            tree.addMerkleTree((int) Math.pow(2, depth), range);
        }
        if (logger.isDebugEnabled())
        {
            // MT serialize may take time
            logger.debug("Created {} merkle trees with merkle trees size {}, {} partitions, {} bytes", tree.ranges().size(), tree.size(), allPartitions, MerkleTrees.serializer.serializedSize(tree, 0));
        }

        return tree;
    }

    private static ValidationPartitionIterator getValidationIterator(TableRepairManager repairManager, Validator validator) throws IOException
    {
        RepairJobDesc desc = validator.desc;
        return repairManager.getValidationIterator(desc.ranges, desc.parentSessionId, desc.sessionId, validator.isIncremental, validator.nowInSec);
    }

    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    @SuppressWarnings("resource")
    private void doValidation(ColumnFamilyStore cfs, Validator validator) throws IOException
    {
        // this isn't meant to be race-proof, because it's not -- it won't cause bugs for a CFS to be dropped
        // mid-validation, or to attempt to validate a droped CFS.  this is just a best effort to avoid useless work,
        // particularly in the scenario where a validation is submitted before the drop, and there are compactions
        // started prior to the drop keeping some sstables alive.  Since validationCompaction can run
        // concurrently with other compactions, it would otherwise go ahead and scan those again.
        if (!cfs.isValid())
            return;

        // Create Merkle trees suitable to hold estimated partitions for the given ranges.
        // We blindly assume that a partition is evenly distributed on all sstables for now.
        long start = System.nanoTime();
        long partitionCount = 0;
        long estimatedTotalBytes = 0;
        try (ValidationPartitionIterator vi = getValidationIterator(cfs.getRepairManager(), validator))
        {
            MerkleTrees tree = createMerkleTrees(vi, validator.desc.ranges, cfs);
            try
            {
                // validate the CF as we iterate over it
                validator.prepare(cfs, tree);
                while (vi.hasNext())
                {
                    try (UnfilteredRowIterator partition = vi.next())
                    {
                        validator.add(partition);
                        partitionCount++;
                    }
                }
                validator.complete();
            }
            finally
            {
                estimatedTotalBytes = vi.getEstimatedBytes();
                partitionCount = vi.estimatedPartitions();
            }
        }
        finally
        {
            cfs.metric.bytesValidated.update(estimatedTotalBytes);
            cfs.metric.partitionsValidated.update(partitionCount);
        }
        if (logger.isDebugEnabled())
        {
            long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.debug("Validation of {} partitions (~{}) finished in {} msec, for {}",
                         partitionCount,
                         FBUtilities.prettyPrintMemory(estimatedTotalBytes),
                         duration,
                         validator.desc);
        }
    }

    /**
     * Does not mutate data, so is not scheduled.
     */
    public Future<?> submitValidation(ColumnFamilyStore cfs, Validator validator)
    {
        Callable<Object> validation = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                try (TableMetrics.TableTimer.Context c = cfs.metric.validationTime.time())
                {
                    doValidation(cfs, validator);
                }
                catch (Throwable e)
                {
                    // we need to inform the remote end of our failure, otherwise it will hang on repair forever
                    validator.fail();
                    throw e;
                }
                return this;
            }
        };

        return cfs.getRepairManager().submitValidation(validation);
    }
}
