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
package org.apache.cassandra.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.cache.AutoSavingCache.CacheSerializer;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public static enum CacheType
    {
        KEY_CACHE("KeyCache"),
        ROW_CACHE("RowCache"),
        COUNTER_CACHE("CounterCache");

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

    public final AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache;
    public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;
    public final AutoSavingCache<CounterCacheKey, ClockAndCount> counterCache;

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

        keyCache = initKeyCache();
        rowCache = initRowCache();
        counterCache = initCounterCache();
    }

    /**
     * @return auto saving cache object
     */
    private AutoSavingCache<KeyCacheKey, RowIndexEntry> initKeyCache()
    {
        logger.info("Initializing key cache with capacity of {} MBs.", DatabaseDescriptor.getKeyCacheSizeInMB());

        long keyCacheInMemoryCapacity = DatabaseDescriptor.getKeyCacheSizeInMB() * 1024 * 1024;

        // as values are constant size we can use singleton weigher
        // where 48 = 40 bytes (average size of the key) + 8 bytes (size of value)
        ICache<KeyCacheKey, RowIndexEntry> kc;
        kc = ConcurrentLinkedHashCache.create(keyCacheInMemoryCapacity);
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = new AutoSavingCache<>(kc, CacheType.KEY_CACHE, new KeyCacheSerializer());

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();

        keyCache.scheduleSaving(DatabaseDescriptor.getKeyCacheSavePeriod(), keyCacheKeysToSave);

        return keyCache;
    }

    /**
     * @return initialized row cache
     */
    private AutoSavingCache<RowCacheKey, IRowCacheEntry> initRowCache()
    {
        logger.info("Initializing row cache with capacity of {} MBs", DatabaseDescriptor.getRowCacheSizeInMB());

        CacheProvider<RowCacheKey, IRowCacheEntry> cacheProvider;
        String cacheProviderClassName = DatabaseDescriptor.getRowCacheSizeInMB() > 0
                                        ? DatabaseDescriptor.getRowCacheClassName() : "org.apache.cassandra.cache.NopCacheProvider";
        try
        {
            Class<CacheProvider<RowCacheKey, IRowCacheEntry>> cacheProviderClass =
                (Class<CacheProvider<RowCacheKey, IRowCacheEntry>>) Class.forName(cacheProviderClassName);
            cacheProvider = cacheProviderClass.newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot find configured row cache provider class " + DatabaseDescriptor.getRowCacheClassName());
        }

        // cache object
        ICache<RowCacheKey, IRowCacheEntry> rc = cacheProvider.create();
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache<>(rc, CacheType.ROW_CACHE, new RowCacheSerializer());

        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        rowCache.scheduleSaving(DatabaseDescriptor.getRowCacheSavePeriod(), rowCacheKeysToSave);

        return rowCache;
    }

    private AutoSavingCache<CounterCacheKey, ClockAndCount> initCounterCache()
    {
        logger.info("Initializing counter cache with capacity of {} MBs", DatabaseDescriptor.getCounterCacheSizeInMB());

        long capacity = DatabaseDescriptor.getCounterCacheSizeInMB() * 1024 * 1024;

        AutoSavingCache<CounterCacheKey, ClockAndCount> cache =
            new AutoSavingCache<>(ConcurrentLinkedHashCache.<CounterCacheKey, ClockAndCount>create(capacity),
                                  CacheType.COUNTER_CACHE,
                                  new CounterCacheSerializer());

        int keysToSave = DatabaseDescriptor.getCounterCacheKeysToSave();

        logger.info("Scheduling counter cache save to every {} seconds (going to save {} keys).",
                    DatabaseDescriptor.getCounterCacheSavePeriod(),
                    keysToSave == Integer.MAX_VALUE ? "all" : keysToSave);

        cache.scheduleSaving(DatabaseDescriptor.getCounterCacheSavePeriod(), keysToSave);

        return cache;
    }


    public int getRowCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getRowCacheSavePeriod();
    }

    public void setRowCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setRowCacheSavePeriod(seconds);
        rowCache.scheduleSaving(seconds, DatabaseDescriptor.getRowCacheKeysToSave());
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getKeyCacheSavePeriod();
    }

    public void setKeyCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("KeyCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setKeyCacheSavePeriod(seconds);
        keyCache.scheduleSaving(seconds, DatabaseDescriptor.getKeyCacheKeysToSave());
    }

    public int getCounterCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getCounterCacheSavePeriod();
    }

    public void setCounterCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("CounterCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setCounterCacheSavePeriod(seconds);
        counterCache.scheduleSaving(seconds, DatabaseDescriptor.getCounterCacheKeysToSave());
    }

    public int getRowCacheKeysToSave()
    {
        return DatabaseDescriptor.getRowCacheKeysToSave();
    }

    public void setRowCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("RowCacheKeysToSave must be non-negative.");
        DatabaseDescriptor.setRowCacheKeysToSave(count);
        rowCache.scheduleSaving(getRowCacheSavePeriodInSeconds(), count);
    }

    public int getKeyCacheKeysToSave()
    {
        return DatabaseDescriptor.getKeyCacheKeysToSave();
    }

    public void setKeyCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("KeyCacheKeysToSave must be non-negative.");
        DatabaseDescriptor.setKeyCacheKeysToSave(count);
        keyCache.scheduleSaving(getKeyCacheSavePeriodInSeconds(), count);
    }

    public int getCounterCacheKeysToSave()
    {
        return DatabaseDescriptor.getCounterCacheKeysToSave();
    }

    public void setCounterCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("CounterCacheKeysToSave must be non-negative.");
        DatabaseDescriptor.setCounterCacheKeysToSave(count);
        counterCache.scheduleSaving(getCounterCacheSavePeriodInSeconds(), count);
    }

    public void invalidateKeyCache()
    {
        keyCache.clear();
    }

    public void invalidateKeyCacheForCf(UUID cfId)
    {
        Iterator<KeyCacheKey> keyCacheIterator = keyCache.keyIterator();
        while (keyCacheIterator.hasNext())
        {
            KeyCacheKey key = keyCacheIterator.next();
            if (key.cfId.equals(cfId))
                keyCacheIterator.remove();
        }
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public void invalidateRowCacheForCf(UUID cfId)
    {
        Iterator<RowCacheKey> rowCacheIterator = rowCache.keyIterator();
        while (rowCacheIterator.hasNext())
        {
            RowCacheKey rowCacheKey = rowCacheIterator.next();
            if (rowCacheKey.cfId.equals(cfId))
                rowCacheIterator.remove();
        }
    }

    public void invalidateCounterCacheForCf(UUID cfId)
    {
        Iterator<CounterCacheKey> counterCacheIterator = counterCache.keyIterator();
        while (counterCacheIterator.hasNext())
        {
            CounterCacheKey counterCacheKey = counterCacheIterator.next();
            if (counterCacheKey.cfId.equals(cfId))
                counterCacheIterator.remove();
        }
    }

    public void invalidateCounterCache()
    {
        counterCache.clear();
    }




    public void setRowCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        rowCache.setCapacity(capacity * 1024 * 1024);
    }


    public void setKeyCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        keyCache.setCapacity(capacity * 1024 * 1024);
    }

    public void setCounterCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        counterCache.setCapacity(capacity * 1024 * 1024);
    }

    public void saveCaches() throws ExecutionException, InterruptedException
    {
        List<Future<?>> futures = new ArrayList<>(3);
        logger.debug("submitting cache saves");

        futures.add(keyCache.submitWrite(DatabaseDescriptor.getKeyCacheKeysToSave()));
        futures.add(rowCache.submitWrite(DatabaseDescriptor.getRowCacheKeysToSave()));
        futures.add(counterCache.submitWrite(DatabaseDescriptor.getCounterCacheKeysToSave()));

        FBUtilities.waitOnFutures(futures);
        logger.debug("cache saves completed");
    }

    public static class CounterCacheSerializer implements CacheSerializer<CounterCacheKey, ClockAndCount>
    {
        public void serialize(CounterCacheKey key, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithLength(key.partitionKey, out);
            ByteBufferUtil.writeWithLength(key.cellName, out);
        }

        public Future<Pair<CounterCacheKey, ClockAndCount>> deserialize(DataInputStream in, final ColumnFamilyStore cfs) throws IOException
        {
            final ByteBuffer partitionKey = ByteBufferUtil.readWithLength(in);
            final CellName cellName = cfs.metadata.comparator.cellFromByteBuffer(ByteBufferUtil.readWithLength(in));
            return StageManager.getStage(Stage.READ).submit(new Callable<Pair<CounterCacheKey, ClockAndCount>>()
            {
                public Pair<CounterCacheKey, ClockAndCount> call() throws Exception
                {
                    DecoratedKey key = cfs.partitioner.decorateKey(partitionKey);
                    QueryFilter filter = QueryFilter.getNamesFilter(key,
                                                                    cfs.metadata.cfName,
                                                                    FBUtilities.singleton(cellName, cfs.metadata.comparator),
                                                                    Long.MIN_VALUE);
                    ColumnFamily cf = cfs.getTopLevelColumns(filter, Integer.MIN_VALUE);
                    if (cf == null)
                        return null;
                    Cell cell = cf.getColumn(cellName);
                    if (cell == null || !cell.isLive(Long.MIN_VALUE))
                        return null;
                    ClockAndCount clockAndCount = CounterContext.instance().getLocalClockAndCount(cell.value());
                    return Pair.create(CounterCacheKey.create(cfs.metadata.cfId, partitionKey, cellName), clockAndCount);
                }
            });
        }
    }

    public static class RowCacheSerializer implements CacheSerializer<RowCacheKey, IRowCacheEntry>
    {
        public void serialize(RowCacheKey key, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithLength(key.key, out);
        }

        public Future<Pair<RowCacheKey, IRowCacheEntry>> deserialize(DataInputStream in, final ColumnFamilyStore cfs) throws IOException
        {
            final ByteBuffer buffer = ByteBufferUtil.readWithLength(in);
            return StageManager.getStage(Stage.READ).submit(new Callable<Pair<RowCacheKey, IRowCacheEntry>>()
            {
                public Pair<RowCacheKey, IRowCacheEntry> call() throws Exception
                {
                    DecoratedKey key = cfs.partitioner.decorateKey(buffer);
                    QueryFilter cacheFilter = new QueryFilter(key, cfs.getColumnFamilyName(), cfs.readFilterForCache(), Integer.MIN_VALUE);
                    ColumnFamily data = cfs.getTopLevelColumns(cacheFilter, Integer.MIN_VALUE);
                    return Pair.create(new RowCacheKey(cfs.metadata.cfId, key), (IRowCacheEntry) data);
                }
            });
        }
    }

    public static class KeyCacheSerializer implements CacheSerializer<KeyCacheKey, RowIndexEntry>
    {
        public void serialize(KeyCacheKey key, DataOutputPlus out) throws IOException
        {
            RowIndexEntry entry = CacheService.instance.keyCache.get(key);
            if (entry == null)
                return;

            CFMetaData cfm = Schema.instance.getCFMetaData(key.cfId);
            if (cfm == null)
                return; // the table no longer exists.

            ByteBufferUtil.writeWithLength(key.key, out);
            out.writeInt(key.desc.generation);
            out.writeBoolean(true);
            key.desc.getFormat().getIndexSerializer(cfm).serialize(entry, out);
        }

        public Future<Pair<KeyCacheKey, RowIndexEntry>> deserialize(DataInputStream input, ColumnFamilyStore cfs) throws IOException
        {
            int keyLength = input.readInt();
            if (keyLength > FBUtilities.MAX_UNSIGNED_SHORT)
            {
                throw new IOException(String.format("Corrupted key cache. Key length of %d is longer than maximum of %d",
                                                    keyLength, FBUtilities.MAX_UNSIGNED_SHORT));
            }
            ByteBuffer key = ByteBufferUtil.read(input, keyLength);
            int generation = input.readInt();
            SSTableReader reader = findDesc(generation, cfs.getSSTables());
            input.readBoolean(); // backwards compatibility for "promoted indexes" boolean
            if (reader == null)
            {
                RowIndexEntry.Serializer.skipPromotedIndex(input);
                return null;
            }
            RowIndexEntry entry = reader.descriptor.getFormat().getIndexSerializer(reader.metadata).deserialize(input, reader.descriptor.version);
            return Futures.immediateFuture(Pair.create(new KeyCacheKey(cfs.metadata.cfId, reader.descriptor, key), entry));
        }

        private SSTableReader findDesc(int generation, Collection<SSTableReader> collection)
        {
            for (SSTableReader sstable : collection)
            {
                if (sstable.descriptor.generation == generation)
                    return sstable;
            }
            return null;
        }
    }
}
