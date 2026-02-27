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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
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
        DecoratedKey key = mutation.key();

        Map<TableId, Long> sizeWarnings = new HashMap<>();
        Map<TableId, Long> tombstoneWarnings = new HashMap<>();

        for (TableId tableId : mutation.getTableIds())
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
            if (cfs == null || cfs.topPartitions == null)
                continue;

            TableMetadata metadata = cfs.metadata();
            if (sizeWarnBytes != -1)
            {
                long estimatedSize = cfs.topPartitions.topSizes().getEstimate(key);
                if (estimatedSize > sizeWarnBytes)
                {
                    sizeWarnings.put(tableId, estimatedSize);
                    noSpamLogger.warn("Write to {} partition {} triggered size warning; " +
                                      "estimated size is {} bytes, threshold is {} bytes (see write_size_warn_threshold)",
                                      metadata, metadata.partitionKeyType.toCQLString(key.getKey()), estimatedSize, sizeWarnBytes);
                }
            }

            if (tombstoneWarnThreshold != -1)
            {
                long estimatedTombstones = cfs.topPartitions.topTombstones().getEstimate(key);
                if (estimatedTombstones > tombstoneWarnThreshold)
                {
                    tombstoneWarnings.put(tableId, estimatedTombstones);
                    noSpamLogger.warn("Write to {} partition {} triggered tombstone warning; " +
                                      "estimated tombstone count is {}, threshold is {} (see write_tombstone_warn_threshold)",
                                      metadata, metadata.partitionKeyType.toCQLString(key.getKey()), estimatedTombstones, tombstoneWarnThreshold);
                }
            }
        }

        if (!sizeWarnings.isEmpty())
            MessageParams.add(ParamType.WRITE_SIZE_WARN, sizeWarnings);
        if (!tombstoneWarnings.isEmpty())
            MessageParams.add(ParamType.WRITE_TOMBSTONE_WARN, tombstoneWarnings);
    }
}
