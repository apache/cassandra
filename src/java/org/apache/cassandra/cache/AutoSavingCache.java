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
package org.apache.cassandra.cache;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class AutoSavingCache<K extends CacheKey, V> extends InstrumentingCache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(AutoSavingCache.class);

    /** True if a cache flush is currently executing: only one may execute at a time. */
    public static final Set<CacheService.CacheType> flushInProgress = new NonBlockingHashSet<CacheService.CacheType>();

    protected volatile ScheduledFuture<?> saveTask;
    protected final CacheService.CacheType cacheType;

    private CacheSerializer<K, V> cacheLoader;
    private static final String CURRENT_VERSION = "b";

    public AutoSavingCache(ICache<K, V> cache, CacheService.CacheType cacheType, CacheSerializer<K, V> cacheloader)
    {
        super(cacheType.toString(), cache);
        this.cacheType = cacheType;
        this.cacheLoader = cacheloader;
    }

    public File getCachePath(String ksName, String cfName, String version)
    {
        return DatabaseDescriptor.getSerializedCachePath(ksName, cfName, cacheType, version);
    }

    public Writer getWriter(int keysToSave)
    {
        return new Writer(keysToSave);
    }

    public void scheduleSaving(int savePeriodInSeconds, final int keysToSave)
    {
        if (saveTask != null)
        {
            saveTask.cancel(false); // Do not interrupt an in-progress save
            saveTask = null;
        }
        if (savePeriodInSeconds > 0)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    submitWrite(keysToSave);
                }
            };
            saveTask = StorageService.optionalTasks.scheduleWithFixedDelay(runnable,
                                                                           savePeriodInSeconds,
                                                                           savePeriodInSeconds,
                                                                           TimeUnit.SECONDS);
        }
    }

    public int loadSaved(ColumnFamilyStore cfs)
    {
        int count = 0;
        long start = System.currentTimeMillis();

        // old cache format that only saves keys
        File path = getCachePath(cfs.table.name, cfs.columnFamily, null);
        if (path.exists())
        {
            DataInputStream in = null;
            try
            {
                logger.info(String.format("reading saved cache %s", path));
                in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
                Set<ByteBuffer> keys = new HashSet<ByteBuffer>();
                while (in.available() > 0)
                {
                    keys.add(ByteBufferUtil.readWithLength(in));
                    count++;
                }
                cacheLoader.load(keys, cfs);
            }
            catch (Exception e)
            {
                logger.warn(String.format("error reading saved cache %s, keys loaded so far: %d", path.getAbsolutePath(), count), e);
                return count;
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }

        // modern format, allows both key and value (so key cache load can be purely sequential)
        path = getCachePath(cfs.table.name, cfs.columnFamily, CURRENT_VERSION);
        if (path.exists())
        {
            DataInputStream in = null;
            try
            {
                logger.info(String.format("reading saved cache %s", path));
                in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
                List<Future<Pair<K, V>>> futures = new ArrayList<Future<Pair<K, V>>>();
                while (in.available() > 0)
                {
                    futures.add(cacheLoader.deserialize(in, cfs));
                    count++;
                }

                for (Future<Pair<K, V>> future : futures)
                {
                    Pair<K, V> entry = future.get();
                    // Key cache entry can return null, if the SSTable doesn't exist.
                    if (entry == null)
                        continue;
                    put(entry.left, entry.right);
                }
            }
            catch (Exception e)
            {
                logger.warn(String.format("error reading saved cache %s", path.getAbsolutePath()), e);
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }
        if (logger.isDebugEnabled())
            logger.debug(String.format("completed reading (%d ms; %d keys) saved cache %s",
                    System.currentTimeMillis() - start, count, path));
        return count;
    }

    public Future<?> submitWrite(int keysToSave)
    {
        return CompactionManager.instance.submitCacheWrite(getWriter(keysToSave));
    }

    public void reduceCacheSize()
    {
        if (getCapacity() > 0)
        {
            int newCapacity = (int) (DatabaseDescriptor.getReduceCacheCapacityTo() * weightedSize());

            logger.warn(String.format("Reducing %s capacity from %d to %s to reduce memory pressure",
                                      cacheType, getCapacity(), newCapacity));

            setCapacity(newCapacity);
        }
    }

    public class Writer extends CompactionInfo.Holder
    {
        private final Set<K> keys;
        private final CompactionInfo info;
        private long keysWritten;

        protected Writer(int keysToSave)
        {
            if (keysToSave >= getKeySet().size())
                keys = getKeySet();
            else
                keys = hotKeySet(keysToSave);

            OperationType type;
            if (cacheType == CacheService.CacheType.KEY_CACHE)
                type = OperationType.KEY_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.ROW_CACHE)
                type = OperationType.ROW_CACHE_SAVE;
            else
                type = OperationType.UNKNOWN;

            info = new CompactionInfo(new CFMetaData(Table.SYSTEM_KS, cacheType.toString(), null, null, null),
                                      type,
                                      0,
                                      keys.size(),
                                      "keys");
        }

        public CacheService.CacheType cacheType()
        {
            return cacheType;
        }

        public CompactionInfo getCompactionInfo()
        {
            // keyset can change in size, thus total can too
            return info.forProgress(keysWritten, Math.max(keysWritten, keys.size()));
        }

        public void saveCache()
        {
            logger.debug("Deleting old {} files.", cacheType);
            deleteOldCacheFiles();

            if (keys.size() == 0 || keys.size() == 0)
            {
                logger.debug("Skipping {} save, cache is empty.", cacheType);
                return;
            }

            long start = System.currentTimeMillis();

            HashMap<Pair<String, String>, SequentialWriter> writers = new HashMap<Pair<String, String>, SequentialWriter>();

            try
            {
                for (K key : keys)
                {
                    Pair<String, String> path = key.getPathInfo();
                    SequentialWriter writer = writers.get(path);

                    if (writer == null)
                    {
                        writer = tempCacheFile(path);
                        writers.put(path, writer);
                    }

                    try
                    {
                        cacheLoader.serialize(key, writer.stream);
                    }
                    catch (IOException e)
                    {
                        throw new FSWriteError(e, writer.getPath());
                    }

                    keysWritten++;
                }
            }
            finally
            {
                for (SequentialWriter writer : writers.values())
                    FileUtils.closeQuietly(writer);
            }

            for (Map.Entry<Pair<String, String>, SequentialWriter> info : writers.entrySet())
            {
                Pair<String, String> path = info.getKey();
                SequentialWriter writer = info.getValue();

                File tmpFile = new File(writer.getPath());
                File cacheFile = getCachePath(path.left, path.right, CURRENT_VERSION);

                cacheFile.delete(); // ignore error if it didn't exist
                if (!tmpFile.renameTo(cacheFile))
                    logger.error("Unable to rename " + tmpFile + " to " + cacheFile);
            }

            logger.info(String.format("Saved %s (%d items) in %d ms", cacheType, keys.size(), System.currentTimeMillis() - start));
        }

        private SequentialWriter tempCacheFile(Pair<String, String> pathInfo)
        {
            File path = getCachePath(pathInfo.left, pathInfo.right, CURRENT_VERSION);
            File tmpFile = FileUtils.createTempFile(path.getName(), null, path.getParentFile());
            return SequentialWriter.open(tmpFile, true);
        }

        private void deleteOldCacheFiles()
        {
            File savedCachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());

            if (savedCachesDir.exists() && savedCachesDir.isDirectory())
            {
                for (File file : savedCachesDir.listFiles())
                {
                    if (file.isFile() && file.getName().endsWith(cacheType.toString()))
                    {
                        if (!file.delete())
                            logger.warn("Failed to delete {}", file.getAbsolutePath());
                    }

                    if (file.isFile() && file.getName().endsWith(CURRENT_VERSION + ".db"))
                    {
                        if (!file.delete())
                            logger.warn("Failed to delete {}", file.getAbsolutePath());
                    }
                }
            }
        }
    }

    public interface CacheSerializer<K extends CacheKey, V>
    {
        void serialize(K key, DataOutput out) throws IOException;

        Future<Pair<K, V>> deserialize(DataInputStream in, ColumnFamilyStore cfs) throws IOException;

        @Deprecated
        void load(Set<ByteBuffer> buffer, ColumnFamilyStore cfs);
    }
}
