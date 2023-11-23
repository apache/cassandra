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

package org.apache.cassandra.service.reads;


import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.locator.Replica;

/**
 * We have a potential short read if the result from a given node contains the requested number of rows
 * (i.e. it has stopped returning results due to the limit), but some of them haven't
 * made it into the final post-reconciliation result due to other nodes' row, range, and/or partition tombstones.
 *
 * If that is the case, then that node may have more rows that we should fetch, as otherwise we could
 * ultimately return fewer rows than required. Also, those additional rows may contain tombstones which
 * which we also need to fetch as they may shadow rows or partitions from other replicas' results, which we would
 * otherwise return incorrectly.
 */
public class ShortReadProtection
{
    public static UnfilteredPartitionIterator extend(Replica source,
                                                     Runnable preFetchCallback,
                                                     UnfilteredPartitionIterator partitions,
                                                     ReadCommand command,
                                                     DataLimits.Counter mergedResultCounter,
                                                     long queryStartNanoTime,
                                                     boolean enforceStrictLiveness)
    {
        DataLimits.Counter singleResultCounter = command.limits().newCounter(command.nowInSec(),
                                                                             false,
                                                                             command.selectsFullPartition(),
                                                                             enforceStrictLiveness).onlyCount();

        ShortReadPartitionsProtection protection = new ShortReadPartitionsProtection(command,
                                                                                     source,
                                                                                     preFetchCallback,
                                                                                     singleResultCounter,
                                                                                     mergedResultCounter,
                                                                                     queryStartNanoTime);

        /*
         * The order of extention and transformations is important here. Extending with more partitions has to happen
         * first due to the way BaseIterator.hasMoreContents() works: only transformations applied after extension will
         * be called on the first partition of the extended iterator.
         *
         * Additionally, we want singleResultCounter to be applied after SRPP, so that its applyToPartition() method will
         * be called last, after the extension done by SRRP.applyToPartition() call. That way we preserve the same order
         * when it comes to calling SRRP.moreContents() and applyToRow() callbacks.
         *
         * See ShortReadPartitionsProtection.applyToPartition() for more details.
         */

        // extend with moreContents() only if it's a range read command with no partition key specified
        if (!command.isLimitedToOnePartition())
            partitions = MorePartitions.extend(partitions, protection);     // register SRPP.moreContents()

        partitions = Transformation.apply(partitions, protection);          // register SRPP.applyToPartition()
        partitions = Transformation.apply(partitions, singleResultCounter); // register the per-source counter

        return partitions;
    }
}
