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

package org.apache.cassandra.cache;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;

/**
 * Cache that loads blacklisted partitions from system_distributed.blacklisted_partitions table
 * This does not intentionally use AutoSavingCache since this is orthogonal to how AutoSavingCache works, i.e.
 * BlacklistedPartitionCache does not perdiodically save its state to disk, rather, it builds its state from disk.
 */
public class BlacklistedPartitionCache
{
    public final static BlacklistedPartitionCache instance = new BlacklistedPartitionCache();
    private static final Logger logger = LoggerFactory.getLogger(BlacklistedPartitionCache.class);
    private volatile Set<BlacklistedPartition> blacklistedPartitions;

    private BlacklistedPartitionCache()
    {
        // setup a periodic refresh
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(this::refreshCache,
                                                                DatabaseDescriptor.getBlacklistedPartitionsCacheRefreshInSec(),
                                                                DatabaseDescriptor.getBlacklistedPartitionsCacheRefreshInSec(),
                                                                TimeUnit.SECONDS);
    }

    /**
     * Loads blacklisted partitions from system_distributed.blacklisted partitions table.
     * Stops reading partitions upon exceeding the cache size limit by logging a warning.
     */
    public void refreshCache()
    {
        UntypedResultSet results = SystemDistributedKeyspace.getBlacklistedPartitions();
        Set<BlacklistedPartition> partitions = new HashSet<>();

        if (results != null)
        {
            int cacheSizeBytes = 0;
            for (UntypedResultSet.Row row : results)
            {
                try
                {
                    BlacklistedPartition blacklistedPartition = new BlacklistedPartition(row.getString("keyspace_name"), row.getString("columnfamily_name"), row.getString("partition_key"));

                    // check if adding this blacklisted partition would increase cache size beyond the set limit.
                    cacheSizeBytes += blacklistedPartition.unsharedHeapSize();
                    if (cacheSizeBytes / (1024 * 1024) > DatabaseDescriptor.getBlackListedPartitionsCacheSizeLimitInMB())
                    {
                        logger.warn("BlacklistedPartitions cache size limit of {} MB reached. Unable to load more blacklisted partitions. BlacklistedPartitions cache working in degraded mode.", DatabaseDescriptor.getBlackListedPartitionsCacheSizeLimitInMB());
                        break;
                    }
                    partitions.add(blacklistedPartition);
                }
                catch (IllegalArgumentException ex)
                {
                    // exception could arise incase of invalid keyspace/table. We shall continue to try reading other
                    // blacklisted partitions
                    logger.warn("Exception parsing blacklisted partition. {}", ex.getMessage());
                }
            }
        }
        this.blacklistedPartitions = partitions;
    }

    /**
     * indicates if a pair of tableId and key exist in the cache
     *
     * @param tableId TableId - the unique identifier of a table
     * @param key     DecoratedKey
     * @return true if <tableId, key> exist in cache
     */
    public boolean contains(TableId tableId, DecoratedKey key)
    {
        return null != blacklistedPartitions && blacklistedPartitions.contains(new BlacklistedPartition(tableId, key));
    }

    /**
     * count of blacklisted partitions
     *
     * @return
     */
    public int size()
    {
        return null == blacklistedPartitions ? 0 : blacklistedPartitions.size();
    }
}
