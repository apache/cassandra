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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Utility class for checking write threshold warnings on replicas.
 * Used by both regular mutation writes and CAS/LWT writes.
 */
public class WriteThresholds
{
    private static final Logger logger = LoggerFactory.getLogger(WriteThresholds.class);

    /**
     * Check write thresholds for a single partition update.
     * This method looks up the partition in TopPartitionTracker and adds
     * warning params to MessageParams if thresholds are exceeded.
     *
     * @param update the partition update being written
     * @param key the partition key being written
     */
    public static void checkWriteThresholds(PartitionUpdate update, org.apache.cassandra.db.DecoratedKey key)
    {
        if (!DatabaseDescriptor.isDaemonInitialized() || !DatabaseDescriptor.getWriteThresholdsEnabled())
            return;

        DataStorageSpec.LongBytesBound sizeWarnThreshold = DatabaseDescriptor.getWriteSizeWarnThreshold();
        int tombstoneWarnThreshold = DatabaseDescriptor.getWriteTombstoneWarnThreshold();

        if (sizeWarnThreshold == null && tombstoneWarnThreshold == 0)
            return;

        long sizeWarnBytes = sizeWarnThreshold != null ? sizeWarnThreshold.toBytes() : -1;

        TableId tableId = update.metadata().id;
        org.apache.cassandra.db.ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);

        if (cfs == null || cfs.topPartitions == null)
            return;

        long estimatedSize = cfs.topPartitions.topSizes().getEstimate(key);
        long estimatedTombstones = cfs.topPartitions.topTombstones().getEstimate(key);

        if (sizeWarnBytes > 0 && estimatedSize > sizeWarnBytes)
        {
            Long currentSize = org.apache.cassandra.db.MessageParams.get(ParamType.WRITE_SIZE_WARN);
            if (currentSize == null || currentSize < estimatedSize)
            {
                org.apache.cassandra.db.MessageParams.add(ParamType.WRITE_SIZE_WARN, estimatedSize);

                TableMetadata meta = update.metadata();
                String pk = meta.partitionKeyType.getString(key.getKey());
                logger.warn("Write to {}.{} partition {} triggered size warning; " +
                            "estimated size is {} bytes, threshold is {} bytes (see write_size_warn_threshold)",
                            meta.keyspace, meta.name, pk, estimatedSize, sizeWarnBytes);
            }
        }

        if (tombstoneWarnThreshold > 0 && estimatedTombstones > tombstoneWarnThreshold)
        {
            Integer currentTombstones = org.apache.cassandra.db.MessageParams.get(ParamType.WRITE_TOMBSTONE_WARN);
            if (currentTombstones == null || currentTombstones < estimatedTombstones)
            {
                org.apache.cassandra.db.MessageParams.add(ParamType.WRITE_TOMBSTONE_WARN, (int) estimatedTombstones);

                TableMetadata meta = update.metadata();
                String pk = meta.partitionKeyType.getString(key.getKey());
                logger.warn("Write to {}.{} partition {} triggered tombstone warning; " +
                            "estimated tombstone count is {}, threshold is {} (see write_tombstone_warn_threshold)",
                            meta.keyspace, meta.name, pk, estimatedTombstones, tombstoneWarnThreshold);
            }
        }
    }

    /**
     * Check write thresholds for all partition updates in a mutation.
     * This method iterates through all partition updates in the mutation.
     *
     * @param mutation the mutation containing one or more partition updates
     */
    public static void checkWriteThresholds(org.apache.cassandra.db.Mutation mutation)
    {
        if (!DatabaseDescriptor.isDaemonInitialized() || !DatabaseDescriptor.getWriteThresholdsEnabled())
            return;

        DataStorageSpec.LongBytesBound sizeWarnThreshold = DatabaseDescriptor.getWriteSizeWarnThreshold();
        int tombstoneWarnThreshold = DatabaseDescriptor.getWriteTombstoneWarnThreshold();

        if (sizeWarnThreshold == null && tombstoneWarnThreshold == 0)
            return;

        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            checkWriteThresholds(update, update.partitionKey());
        }
    }
}
