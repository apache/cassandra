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

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;

public class StatusLogger
{
    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);
    private static final ReentrantLock busyMonitor = new ReentrantLock();

    public static void log()
    {
        // avoid logging more than once at the same time. throw away any attempts to log concurrently, as it would be
        // confusing and noisy for operators - and don't bother logging again, immediately as it'll just be the same data
        if (busyMonitor.tryLock())
        {
            try
            {
                logStatus();
            }
            finally
            {
                busyMonitor.unlock();
            }
        }
        else
        {
            logger.trace("StatusLogger is busy");
        }
    }

    private static void logStatus()
    {
        // everything from o.a.c.concurrent
        logger.info(String.format("%-28s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All Time Blocked"));

        for (ThreadPoolMetrics tpool : CassandraMetricsRegistry.Metrics.allThreadPoolMetrics())
        {
            logger.info(String.format("%-28s%10s%10s%15s%10s%18s",
                                      tpool.poolName,
                                      tpool.activeTasks.getValue(),
                                      tpool.pendingTasks.getValue(),
                                      tpool.completedTasks.getValue(),
                                      tpool.currentBlocked.getCount(),
                                      tpool.totalBlocked.getCount()));
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
        AutoSavingCache<KeyCacheKey, AbstractRowIndexEntry> keyCache = CacheService.instance.keyCache;
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
                                      cfs.getKeyspaceName() + "." + cfs.name,
                                      cfs.metric.memtableColumnsCount.getValue() + "," + cfs.metric.memtableLiveDataSize.getValue()));
        }
    }
}
