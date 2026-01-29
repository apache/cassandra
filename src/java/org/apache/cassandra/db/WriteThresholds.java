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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Utility class for checking write threshold warnings on replicas.
 * CASSANDRA-17258: paxos and accord do complex thread hand off and custom write logic which makes this patch complex, so was deferred
 */
public class WriteThresholds
{
    private static final Logger logger = LoggerFactory.getLogger(WriteThresholds.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    /**
     * Check write thresholds for all partition updates in a mutation.
     * This method iterates through all partition updates in the mutation.
     *
     * @param mutation the mutation containing one or more partition updates
     */
    public static void checkWriteThresholds(Mutation mutation)
    {
        if (!DatabaseDescriptor.isDaemonInitialized() || !DatabaseDescriptor.getWriteThresholdsEnabled())
            return;

        DataStorageSpec.LongBytesBound sizeWarnThreshold = DatabaseDescriptor.getWriteSizeWarnThreshold();
        int tombstoneWarnThreshold = DatabaseDescriptor.getWriteTombstoneWarnThreshold();

        if (sizeWarnThreshold == null && tombstoneWarnThreshold == -1)
            return;

        long sizeWarnBytes = sizeWarnThreshold != null ? sizeWarnThreshold.toBytes() : -1;

        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            checkWriteThresholdsInternal(update, update.partitionKey(), sizeWarnBytes, tombstoneWarnThreshold);
        }
    }

    /**
     * Internal method to check write thresholds for a single partition update.
     * This method looks up the partition in TopPartitionTracker and adds
     * warning params to MessageParams if thresholds are exceeded.
     *
     * @param update                 the partition update being written
     * @param key                    the partition key being written
     * @param sizeWarnBytes          size threshold in bytes, or -1 if disabled
     * @param tombstoneWarnThreshold tombstone count threshold, or -1 if disabled
     */
    private static void checkWriteThresholdsInternal(PartitionUpdate update, DecoratedKey key,
                                                     long sizeWarnBytes, int tombstoneWarnThreshold)
    {
        TableId tableId = update.metadata().id;
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);

        if (cfs == null || cfs.topPartitions == null)
            return;

        long estimatedSize = cfs.topPartitions.topSizes().getEstimate(key);
        long estimatedTombstones = cfs.topPartitions.topTombstones().getEstimate(key);

        TableMetadata meta = update.metadata();

        if (sizeWarnBytes != -1 && estimatedSize > sizeWarnBytes)
        {
            Number currentValue = MessageParams.get(ParamType.WRITE_SIZE_WARN);
            long currentLong = currentValue != null ? currentValue.longValue() : -1;

            if (currentLong < estimatedSize)
            {
                MessageParams.add(ParamType.WRITE_SIZE_WARN, estimatedSize);
                noSpamLogger.warn("Write to {} partition {} triggered size warning; " +
                                  "estimated size is {} bytes, threshold is {} bytes (see write_size_warn_threshold)",
                                  meta, meta.partitionKeyType.toCQLString(key.getKey()), estimatedSize, sizeWarnBytes);
            }
        }

        if (tombstoneWarnThreshold != -1 && estimatedTombstones > tombstoneWarnThreshold)
        {
            Number currentValue = MessageParams.get(ParamType.WRITE_TOMBSTONE_WARN);
            long currentLong = currentValue != null ? currentValue.longValue() : -1;

            if (currentLong < estimatedTombstones)
            {
                MessageParams.add(ParamType.WRITE_TOMBSTONE_WARN, (int) estimatedTombstones);
                noSpamLogger.warn("Write to {} partition {} triggered tombstone warning; " +
                                  "estimated tombstone count is {}, threshold is {} (see write_tombstone_warn_threshold)",
                                  meta, meta.partitionKeyType.toCQLString(key.getKey()), estimatedTombstones, tombstoneWarnThreshold);
            }
        }
    }
}
