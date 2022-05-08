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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.cache.AutoSavingCache.CacheSerializer;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public enum CacheType
    {
        KEY_CACHE("KeyCache"),
        ROW_CACHE("RowCache"),
        COUNTER_CACHE("CounterCache");

        private final String name;

        CacheType(String typeName)
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
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        keyCache = initKeyCache();
        rowCache = initRowCache();
        counterCache = initCounterCache();
    }

    /**
     * @return auto saving cache object
     */
    private AutoSavingCache<KeyCacheKey, RowIndexEntry> initKeyCache()
    {
        logger.info("Initializing key cache with capacity of {} MiBs.", DatabaseDescriptor.getKeyCacheSizeInMiB());

        long keyCacheInMemoryCapacity = DatabaseDescriptor.getKeyCacheSizeInMiB() * 1024 * 1024;

        // as values are constant size we can use singleton weigher
        // where 48 = 40 bytes (average size of the key) + 8 bytes (size of value)
        ICache<KeyCacheKey, RowIndexEntry> kc;
        kc = CaffeineCache.create(keyCacheInMemoryCapacity);
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
        logger.info("Initializing row cache with capacity of {} MiBs", DatabaseDescriptor.getRowCacheSizeInMiB());

        CacheProvider<RowCacheKey, IRowCacheEntry> cacheProvider;
        String cacheProviderClassName = DatabaseDescriptor.getRowCacheSizeInMiB() > 0
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
        logger.info("Initializing counter cache with capacity of {} MiBs", DatabaseDescriptor.getCounterCacheSizeInMiB());

        long capacity = DatabaseDescriptor.getCounterCacheSizeInMiB() * 1024 * 1024;

        AutoSavingCache<CounterCacheKey, ClockAndCount> cache =
            new AutoSavingCache<>(CaffeineCache.create(capacity),
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

    public void invalidateKeyCacheForCf(TableMetadata tableMetadata)
    {
        Iterator<KeyCacheKey> keyCacheIterator = keyCache.keyIterator();
        while (keyCacheIterator.hasNext())
        {
            KeyCacheKey key = keyCacheIterator.next();
            if (key.sameTable(tableMetadata))
                keyCacheIterator.remove();
        }
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public void invalidateRowCacheForCf(TableMetadata tableMetadata)
    {
        Iterator<RowCacheKey> rowCacheIterator = rowCache.keyIterator();
        while (rowCacheIterator.hasNext())
        {
            RowCacheKey key = rowCacheIterator.next();
            if (key.sameTable(tableMetadata))
                rowCacheIterator.remove();
        }
    }

    public void invalidateCounterCacheForCf(TableMetadata tableMetadata)
    {
        Iterator<CounterCacheKey> counterCacheIterator = counterCache.keyIterator();
        while (counterCacheIterator.hasNext())
        {
            CounterCacheKey key = counterCacheIterator.next();
            if (key.sameTable(tableMetadata))
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
        public void serialize(CounterCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException
        {
            assert(cfs.metadata().isCounter());
            TableMetadata tableMetadata = cfs.metadata();
            tableMetadata.id.serialize(out);
            out.writeUTF(tableMetadata.indexName().orElse(""));
            key.write(out);
        }

        public Future<Pair<CounterCacheKey, ClockAndCount>> deserialize(DataInputPlus in, final ColumnFamilyStore cfs) throws IOException
        {
            //Keyspace and CF name are deserialized by AutoSaving cache and used to fetch the CFS provided as a
            //parameter so they aren't deserialized here, even though they are serialized by this serializer
            if (cfs == null)
                return null;
            final CounterCacheKey cacheKey = CounterCacheKey.read(cfs.metadata(), in);
            if (!cfs.metadata().isCounter() || !cfs.isCounterCacheEnabled())
                return null;

            return Stage.READ.submit(new Callable<Pair<CounterCacheKey, ClockAndCount>>()
            {
                public Pair<CounterCacheKey, ClockAndCount> call() throws Exception
                {
                    ByteBuffer value = cacheKey.readCounterValue(cfs);
                    return value == null
                         ? null
                         : Pair.create(cacheKey, CounterContext.instance().getLocalClockAndCount(value));
                }
            });
        }
    }

    public static class RowCacheSerializer implements CacheSerializer<RowCacheKey, IRowCacheEntry>
    {
        public void serialize(RowCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException
        {
            assert(!cfs.isIndex());//Shouldn't have row cache entries for indexes
            TableMetadata tableMetadata = cfs.metadata();
            tableMetadata.id.serialize(out);
            out.writeUTF(tableMetadata.indexName().orElse(""));
            ByteArrayUtil.writeWithLength(key.key, out);
        }

        public Future<Pair<RowCacheKey, IRowCacheEntry>> deserialize(DataInputPlus in, final ColumnFamilyStore cfs) throws IOException
        {
            //Keyspace and CF name are deserialized by AutoSaving cache and used to fetch the CFS provided as a
            //parameter so they aren't deserialized here, even though they are serialized by this serializer
            final ByteBuffer buffer = ByteBufferUtil.readWithLength(in);
            if (cfs == null  || !cfs.isRowCacheEnabled())
                return null;
            final int rowsToCache = cfs.metadata().params.caching.rowsPerPartitionToCache();
            assert(!cfs.isIndex());//Shouldn't have row cache entries for indexes

            return Stage.READ.submit(new Callable<Pair<RowCacheKey, IRowCacheEntry>>()
            {
                public Pair<RowCacheKey, IRowCacheEntry> call() throws Exception
                {
                    DecoratedKey key = cfs.decorateKey(buffer);
                    int nowInSec = FBUtilities.nowInSeconds();
                    SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, key);
                    try (ReadExecutionController controller = cmd.executionController(); UnfilteredRowIterator iter = cmd.queryMemtableAndDisk(cfs, controller))
                    {
                        CachedPartition toCache = CachedBTreePartition.create(DataLimits.cqlLimits(rowsToCache).filter(iter, nowInSec, true), nowInSec);
                        return Pair.create(new RowCacheKey(cfs.metadata(), key), toCache);
                    }
                }
            });
        }
    }

    public static class KeyCacheSerializer implements CacheSerializer<KeyCacheKey, RowIndexEntry>
    {
        // For column families with many SSTables the linear nature of getSSTables slowed down KeyCache loading
        // by orders of magnitude. So we cache the sstables once and rely on cleanupAfterDeserialize to cleanup any
        // cached state we may have accumulated during the load.
        Map<Pair<String, String>, Map<SSTableId, SSTableReader>> cachedSSTableReaders = new ConcurrentHashMap<>();

        public void serialize(KeyCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException
        {
            RowIndexEntry entry = CacheService.instance.keyCache.getInternal(key);
            if (entry == null)
                return;

            TableMetadata tableMetadata = cfs.metadata();
            tableMetadata.id.serialize(out);
            out.writeUTF(tableMetadata.indexName().orElse(""));
            ByteArrayUtil.writeWithLength(key.key, out);
            if (key.desc.id instanceof SequenceBasedSSTableId)
            {
                out.writeInt(((SequenceBasedSSTableId) key.desc.id).generation);
            }
            else
            {
                out.writeInt(Integer.MIN_VALUE); // backwards compatibility for "int based generation only"
                ByteBufferUtil.writeWithShortLength(key.desc.id.asBytes(), out);
            }
            out.writeBoolean(true);

            SerializationHeader header = new SerializationHeader(false, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            key.desc.getFormat().getIndexSerializer(cfs.metadata(), key.desc.version, header).serializeForCache(entry, out);
        }

        public Future<Pair<KeyCacheKey, RowIndexEntry>> deserialize(DataInputPlus input, ColumnFamilyStore cfs) throws IOException
        {
            boolean skipEntry = cfs == null || !cfs.isKeyCacheEnabled();

            //Keyspace and CF name are deserialized by AutoSaving cache and used to fetch the CFS provided as a
            //parameter so they aren't deserialized here, even though they are serialized by this serializer
            int keyLength = input.readInt();
            if (keyLength > FBUtilities.MAX_UNSIGNED_SHORT)
            {
                throw new IOException(String.format("Corrupted key cache. Key length of %d is longer than maximum of %d",
                                                    keyLength, FBUtilities.MAX_UNSIGNED_SHORT));
            }
            ByteBuffer key = ByteBufferUtil.read(input, keyLength);
            int generation = input.readInt();
            SSTableId generationId = generation == Integer.MIN_VALUE
                                                   ? SSTableIdFactory.instance.fromBytes(ByteBufferUtil.readWithShortLength(input))
                                                   : new SequenceBasedSSTableId(generation); // Backwards compatibility for "int based generation sstables"
            input.readBoolean(); // backwards compatibility for "promoted indexes" boolean
            SSTableReader reader = null;
            if (!skipEntry)
            {
                Pair<String, String> qualifiedName = Pair.create(cfs.metadata.keyspace, cfs.metadata.name);
                Map<SSTableId, SSTableReader> generationToSSTableReader = cachedSSTableReaders.get(qualifiedName);
                if (generationToSSTableReader == null)
                {
                    generationToSSTableReader = new HashMap<>(cfs.getLiveSSTables().size());
                    for (SSTableReader ssTableReader : cfs.getSSTables(SSTableSet.CANONICAL))
                    {
                        generationToSSTableReader.put(ssTableReader.descriptor.id, ssTableReader);
                    }

                    cachedSSTableReaders.putIfAbsent(qualifiedName, generationToSSTableReader);
                }
                reader = generationToSSTableReader.get(generationId);
            }

            if (skipEntry || reader == null)
            {
                // The sstable doesn't exist anymore, so we can't be sure of the exact version and assume its the current version. The only case where we'll be
                // wrong is during upgrade, in which case we fail at deserialization. This is not a huge deal however since 1) this is unlikely enough that
                // this won't affect many users (if any) and only once, 2) this doesn't prevent the node from starting and 3) CASSANDRA-10219 shows that this
                // part of the code has been broken for a while without anyone noticing (it is, btw, still broken until CASSANDRA-10219 is fixed).
                RowIndexEntry.Serializer.skipForCache(input);
                return null;
            }

            RowIndexEntry.IndexSerializer<?> indexSerializer = reader.descriptor.getFormat().getIndexSerializer(reader.metadata(),
                                                                                                                reader.descriptor.version,
                                                                                                                reader.header);
            RowIndexEntry<?> entry = indexSerializer.deserializeForCache(input);
            return ImmediateFuture.success(Pair.create(new KeyCacheKey(cfs.metadata(), reader.descriptor, key), entry));
        }

        public void cleanupAfterDeserialize()
        {
            cachedSSTableReaders.clear();
        }
    }
}
