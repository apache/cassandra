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
package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.*;

import org.apache.cassandra.cache.*;

import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;

public class StatusLogger
{
    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);


    public static void log()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        // everything from o.a.c.concurrent
        logger.info(String.format("%-25s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All Time Blocked"));

        for (Map.Entry<String, String> tpool : ThreadPoolMetrics.getJmxThreadPools(server).entries())
        {
            logger.info(String.format("%-25s%10s%10s%15s%10s%18s%n",
                                      tpool.getValue(),
                                      ThreadPoolMetrics.getJmxMetric(server, tpool.getKey(), tpool.getValue(), "ActiveTasks"),
                                      ThreadPoolMetrics.getJmxMetric(server, tpool.getKey(), tpool.getValue(), "PendingTasks"),
                                      ThreadPoolMetrics.getJmxMetric(server, tpool.getKey(), tpool.getValue(), "CompletedTasks"),
                                      ThreadPoolMetrics.getJmxMetric(server, tpool.getKey(), tpool.getValue(), "CurrentlyBlockedTasks"),
                                      ThreadPoolMetrics.getJmxMetric(server, tpool.getKey(), tpool.getValue(), "TotalBlockedTasks")));
        }

        // one offs
        logger.info(String.format("%-25s%10s%10s",
                                  "CompactionManager", CompactionManager.instance.getActiveCompactions(), CompactionManager.instance.getPendingTasks()));
        int pendingLargeMessages = 0;
        for (int n : MessagingService.instance().getLargeMessagePendingTasks().values())
        {
            pendingLargeMessages += n;
        }
        int pendingSmallMessages = 0;
        for (int n : MessagingService.instance().getSmallMessagePendingTasks().values())
        {
            pendingSmallMessages += n;
        }
        logger.info(String.format("%-25s%10s%10s",
                                  "MessagingService", "n/a", pendingLargeMessages + "/" + pendingSmallMessages));

        // Global key/row cache information
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = CacheService.instance.rowCache;

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "Cache Type", "Size", "Capacity", "KeysToSave"));
        logger.info(String.format("%-25s%10s%25s%25s",
                                  "KeyCache",
                                  keyCache.weightedSize(),
                                  keyCache.getCapacity(),
                                  keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave));

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "RowCache",
                                  rowCache.weightedSize(),
                                  rowCache.getCapacity(),
                                  rowCacheKeysToSave == Integer.MAX_VALUE ? "all" : rowCacheKeysToSave));

        // per-CF stats
        logger.info(String.format("%-25s%20s", "Table", "Memtable ops,data"));
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            logger.info(String.format("%-25s%20s",
                                      cfs.keyspace.getName() + "." + cfs.name,
                                      cfs.metric.memtableColumnsCount.getValue() + "," + cfs.metric.memtableLiveDataSize.getValue()));
        }
    }
}
