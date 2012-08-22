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
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.cache.AutoSavingCache.CacheSerializer;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableReader.Operator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.github.jamm.MemoryMeter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.EntryWeigher;

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

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

    public final AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache;
    public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;

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
    }

    /**
     * We can use Weighers.singleton() because Long can't be leaking memory
     * @return auto saving cache object
     */
    private AutoSavingCache<KeyCacheKey, RowIndexEntry> initKeyCache()
    {
        logger.info("Initializing key cache with capacity of {} MBs.", DatabaseDescriptor.getKeyCacheSizeInMB());

        long keyCacheInMemoryCapacity = DatabaseDescriptor.getKeyCacheSizeInMB() * 1024 * 1024;

        // as values are constant size we can use singleton weigher
        // where 48 = 40 bytes (average size of the key) + 8 bytes (size of value)
        ICache<KeyCacheKey, RowIndexEntry> kc;
        if (MemoryMeter.isInitialized())
        {
            kc = ConcurrentLinkedHashCache.create(keyCacheInMemoryCapacity);
        }
        else
        {
            logger.warn("MemoryMeter uninitialized (jamm not specified as java agent); KeyCache size in JVM Heap will not be calculated accurately. " +
            		"Usually this means cassandra-env.sh disabled jamm because you are using a buggy JRE; upgrade to the Sun JRE instead");
            /* We don't know the overhead size because memory meter is not enabled. */
            EntryWeigher<KeyCacheKey, RowIndexEntry> weigher = new EntryWeigher<KeyCacheKey, RowIndexEntry>()
            {
                public int weightOf(KeyCacheKey key, RowIndexEntry entry)
                {
                    return key.key.length + entry.serializedSize();
                }
            };
            kc = ConcurrentLinkedHashCache.create(keyCacheInMemoryCapacity, weigher);
        }
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = new AutoSavingCache<KeyCacheKey, RowIndexEntry>(kc, CacheType.KEY_CACHE, new KeyCacheSerializer());

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();

        logger.info("Scheduling key cache save to each {} seconds (going to save {} keys).",
                DatabaseDescriptor.getKeyCacheSavePeriod(),
                    keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave);

        keyCache.scheduleSaving(DatabaseDescriptor.getKeyCacheSavePeriod(), keyCacheKeysToSave);

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
        ICache<RowCacheKey, IRowCacheEntry> rc = DatabaseDescriptor.getRowCacheProvider().create(rowCacheInMemoryCapacity);
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache<RowCacheKey, IRowCacheEntry>(rc, CacheType.ROW_CACHE, new RowCacheSerializer());

        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        logger.info("Scheduling row cache save to each {} seconds (going to save {} keys).",
                DatabaseDescriptor.getRowCacheSavePeriod(),
                    rowCacheKeysToSave == Integer.MAX_VALUE ? "all" : rowCacheKeysToSave);

        rowCache.scheduleSaving(DatabaseDescriptor.getRowCacheSavePeriod(), rowCacheKeysToSave);

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
        return DatabaseDescriptor.getRowCacheSavePeriod();
    }

    public void setRowCacheSavePeriodInSeconds(int rcspis)
    {
        if (rcspis < 0)
            throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setRowCacheSavePeriod(rcspis);
        rowCache.scheduleSaving(rcspis, DatabaseDescriptor.getRowCacheKeysToSave());
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getKeyCacheSavePeriod();
    }

    public void setKeyCacheSavePeriodInSeconds(int kcspis)
    {
        if (kcspis < 0)
            throw new RuntimeException("KeyCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setKeyCacheSavePeriod(kcspis);
        keyCache.scheduleSaving(kcspis, DatabaseDescriptor.getKeyCacheKeysToSave());
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
        return keyCache.getCapacity();
    }

    public long getKeyCacheCapacityInMB()
    {
        return getKeyCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setKeyCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        long weightedCapacity = capacity * 1024 * 1024;
        keyCache.setCapacity(MemoryMeter.isInitialized() ? weightedCapacity : (weightedCapacity / 48));
    }

    public long getRowCacheSize()
    {
        return rowCache.weightedSize();
    }

    public long getKeyCacheSize()
    {
        return keyCache.weightedSize();
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

    public class RowCacheSerializer implements CacheSerializer<RowCacheKey, IRowCacheEntry>
    {
        public void serialize(RowCacheKey key, DataOutput out) throws IOException
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
                    ColumnFamily data = cfs.getTopLevelColumns(QueryFilter.getIdentityFilter(key, new QueryPath(cfs.columnFamily)), Integer.MIN_VALUE, true);
                    return new Pair<RowCacheKey, IRowCacheEntry>(new RowCacheKey(cfs.metadata.cfId, key), data);
                }
            });
        }

        public void load(Set<ByteBuffer> buffers, ColumnFamilyStore cfs)
        {
            for (ByteBuffer key : buffers)
            {
                DecoratedKey dk = cfs.partitioner.decorateKey(key);
                ColumnFamily data = cfs.getTopLevelColumns(QueryFilter.getIdentityFilter(dk, new QueryPath(cfs.columnFamily)), Integer.MIN_VALUE, true);
                rowCache.put(new RowCacheKey(cfs.metadata.cfId, dk), data);
            }
        }
    }

    public class KeyCacheSerializer implements CacheSerializer<KeyCacheKey, RowIndexEntry>
    {
        public void serialize(KeyCacheKey key, DataOutput out) throws IOException
        {
            RowIndexEntry entry = CacheService.instance.keyCache.get(key);
            if (entry == null)
                return;
            ByteBufferUtil.writeWithLength(key.key, out);
            Descriptor desc = key.desc;
            out.writeInt(desc.generation);
            out.writeBoolean(desc.version.hasPromotedIndexes);
            if (!desc.version.hasPromotedIndexes)
                return;
            RowIndexEntry.serializer.serialize(entry, out);
        }

        public Future<Pair<KeyCacheKey, RowIndexEntry>> deserialize(DataInputStream input, ColumnFamilyStore cfs) throws IOException
        {
            ByteBuffer key = ByteBufferUtil.readWithLength(input);
            int generation = input.readInt();
            SSTableReader reader = findDesc(generation, cfs.getSSTables());
            if (reader == null)
            {
                RowIndexEntry.serializer.skipPromotedIndex(input);
                return null;
            }
            RowIndexEntry entry;
            if (input.readBoolean())
                entry = RowIndexEntry.serializer.deserialize(input, reader.descriptor.version);
            else
                entry = reader.getPosition(reader.partitioner.decorateKey(key), Operator.EQ);
            return Futures.immediateFuture(Pair.create(new KeyCacheKey(reader.descriptor, key), entry));
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

        public void load(Set<ByteBuffer> buffers, ColumnFamilyStore cfs)
        {
            for (ByteBuffer key : buffers)
            {
                DecoratedKey dk = cfs.partitioner.decorateKey(key);

                for (SSTableReader sstable : cfs.getSSTables())
                {
                    RowIndexEntry entry = sstable.getPosition(dk, Operator.EQ);
                    if (entry != null)
                        keyCache.put(new KeyCacheKey(sstable.descriptor, key), entry);
                }
            }
        }
    }
}
