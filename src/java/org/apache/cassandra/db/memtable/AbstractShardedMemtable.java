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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;

public abstract class AbstractShardedMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractShardedMemtable.class);

    public static final String SHARDS_OPTION = "shards";
    public static final String SHARDED_MEMTABLE_CONFIG_OBJECT_NAME = "org.apache.cassandra.db:type=ShardedMemtableConfig";
    static
    {
        MBeanWrapper.instance.registerMBean(new ShardedMemtableConfig(), SHARDED_MEMTABLE_CONFIG_OBJECT_NAME, MBeanWrapper.OnException.LOG);
    }

    // default shard count, used when a specific number of shards is not specified in the options
    private static volatile int defaultShardCount = MEMTABLE_SHARD_COUNT.getInt(FBUtilities.getAvailableProcessors());

    // The boundaries for the keyspace as they were calculated when the memtable is created.
    // The boundaries will be NONE for system keyspaces or if StorageService is not yet initialized.
    // The fact this is fixed for the duration of the memtable lifetime, guarantees we'll always pick the same shard
    // for a given key, even if we race with the StorageService initialization or with topology changes.
    @Unmetered
    protected final ShardBoundaries boundaries;

    AbstractShardedMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                            TableMetadataRef metadataRef,
                            Owner owner,
                            Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner);
        int shardCount = shardCountOption != null ? shardCountOption : defaultShardCount;
        this.boundaries = owner.localRangeSplits(shardCount);
    }

    private static class ShardedMemtableConfig implements ShardedMemtableConfigMXBean
    {
        @Override
        public void setDefaultShardCount(String shardCount)
        {
            if ("auto".equalsIgnoreCase(shardCount))
            {
                defaultShardCount = FBUtilities.getAvailableProcessors();
            }
            else
            {
                try
                {
                    defaultShardCount = Integer.parseInt(shardCount);
                }
                catch (NumberFormatException ex)
                {
                    logger.warn("Unable to parse {} as valid value for shard count", shardCount);
                    return;
                }
            }
            logger.info("Requested setting shard count to {}; set to: {}", shardCount, defaultShardCount);
        }

        @Override
        public String getDefaultShardCount()
        {
            return Integer.toString(defaultShardCount);
        }
    }

    public static int getDefaultShardCount()
    {
        return defaultShardCount;
    }
}
