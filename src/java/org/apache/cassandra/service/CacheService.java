/**
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
package org.apache.cassandra.service;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";
    public static final int AVERAGE_KEY_CACHE_ROW_SIZE = 48;

    public static enum CacheType
    {
        KEY_CACHE("KeyCache"),
        ROW_CACHE("RowCache");

        private final String name;

        private CacheType(String typeName)
        {
            name = typeName;
        }

        public String toString()
        {
            return name;
        }
    }

    public final static CacheService instance = new CacheService();

    public final AutoSavingCache<KeyCacheKey, Long> keyCache;
    public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;

    private int rowCacheSavePeriod;
    private int keyCacheSavePeriod;

    private CacheService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        rowCacheSavePeriod = DatabaseDescriptor.getRowCacheSavePeriod();
        keyCacheSavePeriod = DatabaseDescriptor.getKeyCacheSavePeriod();

        keyCache = initKeyCache();
        rowCache = initRowCache();
    }

    /**
     * We can use Weighers.singleton() because Long can't be leaking memory
     * @return auto saving cache object
     */
    private AutoSavingCache<KeyCacheKey, Long> initKeyCache()
    {
        logger.info("Initializing key cache with capacity of {} MBs.", DatabaseDescriptor.getKeyCacheSizeInMB());

        long keyCacheInMemoryCapacity = DatabaseDescriptor.getKeyCacheSizeInMB() * 1024 * 1024;

        // as values are constant size we can use singleton weigher
        // where 48 = 40 bytes (average size of the key) + 8 bytes (size of value)
        ICache<KeyCacheKey, Long> kc = ConcurrentLinkedHashCache.create(keyCacheInMemoryCapacity / AVERAGE_KEY_CACHE_ROW_SIZE);
        AutoSavingCache<KeyCacheKey, Long> keyCache = new AutoSavingCache<KeyCacheKey, Long>(kc, CacheType.KEY_CACHE);

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();

        logger.info("Scheduling key cache save to each {} seconds (going to save {} keys).",
                    keyCacheSavePeriod,
                    keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave);

        keyCache.scheduleSaving(keyCacheSavePeriod, keyCacheKeysToSave);

        return keyCache;
    }

    /**
     * @return initialized row cache
     */
    private AutoSavingCache<RowCacheKey, IRowCacheEntry> initRowCache()
    {
        logger.info("Initializing row cache with capacity of {} MBs and provider {}",
                    DatabaseDescriptor.getRowCacheSizeInMB(),
                    DatabaseDescriptor.getRowCacheProvider().getClass().getName());

        long rowCacheInMemoryCapacity = DatabaseDescriptor.getRowCacheSizeInMB() * 1024 * 1024;

        // cache object
        ICache<RowCacheKey, IRowCacheEntry> rc = DatabaseDescriptor.getRowCacheProvider().create(rowCacheInMemoryCapacity, true);
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache<RowCacheKey, IRowCacheEntry>(rc, CacheType.ROW_CACHE);

        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        logger.info("Scheduling row cache save to each {} seconds (going to save {} keys).",
                    rowCacheSavePeriod,
                    rowCacheKeysToSave == Integer.MAX_VALUE ? "all" : rowCacheKeysToSave);

        rowCache.scheduleSaving(rowCacheSavePeriod, rowCacheKeysToSave);

        return rowCache;
    }

    public long getKeyCacheHits()
    {
        return keyCache.getHits();
    }

    public long getRowCacheHits()
    {
        return rowCache.getHits();
    }

    public long getKeyCacheRequests()
    {
        return keyCache.getRequests();
    }

    public long getRowCacheRequests()
    {
        return rowCache.getRequests();
    }

    public double getKeyCacheRecentHitRate()
    {
        return keyCache.getRecentHitRate();
    }

    public double getRowCacheRecentHitRate()
    {
        return rowCache.getRecentHitRate();
    }

    public int getRowCacheSavePeriodInSeconds()
    {
        return rowCacheSavePeriod;
    }

    public void setRowCacheSavePeriodInSeconds(int rcspis)
    {
        if (rcspis < 0)
            throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");

        rowCacheSavePeriod = rcspis;
        rowCache.scheduleSaving(rowCacheSavePeriod, DatabaseDescriptor.getRowCacheKeysToSave());
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return keyCacheSavePeriod;
    }

    public void setKeyCacheSavePeriodInSeconds(int kcspis)
    {
        if (kcspis < 0)
            throw new RuntimeException("KeyCacheSavePeriodInSeconds must be non-negative.");

        keyCacheSavePeriod = kcspis;
        keyCache.scheduleSaving(keyCacheSavePeriod, DatabaseDescriptor.getKeyCacheKeysToSave());
    }

    public void invalidateKeyCache()
    {
        keyCache.clear();
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public long getRowCacheCapacityInBytes()
    {
        return rowCache.getCapacity();
    }

    public long getRowCacheCapacityInMB()
    {
        return getRowCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setRowCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        rowCache.setCapacity(capacity * 1024 * 1024);
    }

    public long getKeyCacheCapacityInBytes()
    {
        return keyCache.getCapacity() * AVERAGE_KEY_CACHE_ROW_SIZE;
    }

    public long getKeyCacheCapacityInMB()
    {
        return getKeyCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setKeyCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        keyCache.setCapacity(capacity * 1024 * 1024 / 48);
    }

    public long getRowCacheSize()
    {
        return rowCache.weightedSize();
    }

    public long getKeyCacheSize()
    {
        return keyCache.weightedSize() * AVERAGE_KEY_CACHE_ROW_SIZE;
    }

    public void reduceCacheSizes()
    {
        reduceRowCacheSize();
        reduceKeyCacheSize();
    }

    public void reduceRowCacheSize()
    {
        rowCache.reduceCacheSize();
    }

    public void reduceKeyCacheSize()
    {
        keyCache.reduceCacheSize();
    }

    public void saveCaches() throws ExecutionException, InterruptedException
    {
        List<Future<?>> futures = new ArrayList<Future<?>>(2);
        logger.debug("submitting cache saves");

        futures.add(keyCache.submitWrite(DatabaseDescriptor.getKeyCacheKeysToSave()));
        futures.add(rowCache.submitWrite(DatabaseDescriptor.getRowCacheKeysToSave()));

        FBUtilities.waitOnFutures(futures);
        logger.debug("cache saves completed");
    }
}
