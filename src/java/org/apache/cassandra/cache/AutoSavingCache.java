/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cache;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.Pair;

public class AutoSavingCache<K extends CacheKey, V> extends InstrumentingCache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(AutoSavingCache.class);

    /** True if a cache flush is currently executing: only one may execute at a time. */
    public static final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    protected volatile ScheduledFuture<?> saveTask;
    protected final CacheService.CacheType cacheType;

    public AutoSavingCache(ICache<K, V> cache, CacheService.CacheType cacheType)
    {
        super(cache);
        this.cacheType = cacheType;
    }

    public File getCachePath(String ksName, String cfName)
    {
        return DatabaseDescriptor.getSerializedCachePath(ksName, cfName, cacheType);
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
            Runnable runnable = new WrappedRunnable()
            {
                public void runMayThrow()
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

    public Set<DecoratedKey> readSaved(String ksName, String cfName, IPartitioner partitioner)
    {
        File path = getCachePath(ksName, cfName);
        Set<DecoratedKey> keys = new TreeSet<DecoratedKey>();
        if (path.exists())
        {
            DataInputStream in = null;
            try
            {
                long start = System.currentTimeMillis();

                logger.info(String.format("reading saved cache %s", path));
                in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
                while (in.available() > 0)
                {
                    int size = in.readInt();
                    byte[] bytes = new byte[size];
                    in.readFully(bytes);
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    DecoratedKey key;
                    try
                    {
                        key = partitioner.decorateKey(buffer);
                    }
                    catch (Exception e)
                    {
                        logger.info(String.format("unable to read entry #%s from saved cache %s; skipping remaining entries",
                                keys.size(), path.getAbsolutePath()), e);
                        break;
                    }
                    keys.add(key);
                }
                if (logger.isDebugEnabled())
                    logger.debug(String.format("completed reading (%d ms; %d keys) saved cache %s",
                            System.currentTimeMillis() - start, keys.size(), path));
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
        return keys;
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

    public int estimateSizeToSave(Set<K> keys)
    {
        int bytes = 0;

        for (K key : keys)
            bytes += key.serializedSize();

        return bytes;
    }

    public class Writer extends CompactionInfo.Holder
    {
        private final Set<K> keys;
        private final CompactionInfo info;
        private final long estimatedTotalBytes;
        private long bytesWritten;

        protected Writer(int keysToSave)
        {
            if (keysToSave >= getKeySet().size())
                keys = getKeySet();
            else
                keys = hotKeySet(keysToSave);

            // an approximation -- the keyset can change while saving
            estimatedTotalBytes = estimateSizeToSave(keys);

            OperationType type;
            if (cacheType == CacheService.CacheType.KEY_CACHE)
                type = OperationType.KEY_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.ROW_CACHE)
                type = OperationType.ROW_CACHE_SAVE;
            else
                type = OperationType.UNKNOWN;

            info = new CompactionInfo(new CFMetaData("system", cacheType.toString(), null, null, null),
                                      type,
                                      0,
                                      estimatedTotalBytes);
        }

        public CompactionInfo getCompactionInfo()
        {
            long bytesWritten = this.bytesWritten;
            // keyset can change in size, thus totalBytes can too
            return info.forProgress(bytesWritten,
                                    Math.max(bytesWritten, estimatedTotalBytes));
        }

        public void saveCache() throws IOException
        {
            logger.debug("Deleting old {} files.", cacheType);
            deleteOldCacheFiles();

            if (keys.size() == 0 || estimatedTotalBytes == 0)
            {
                logger.debug("Skipping {} save, cache is empty.", cacheType);
                return;
            }

            long start = System.currentTimeMillis();

            HashMap<Pair<String, String>, SequentialWriter> writers = new HashMap<Pair<String, String>, SequentialWriter>();

            try
            {
                for (CacheKey key : keys)
                {
                    Pair<String, String> path = key.getPathInfo();
                    SequentialWriter writer = writers.get(path);

                    if (writer == null)
                    {
                        writer = tempCacheFile(path);
                        writers.put(path, writer);
                    }

                    key.write(writer.stream);
                    bytesWritten += key.serializedSize();
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
                File cacheFile = getCachePath(path.left, path.right);

                cacheFile.delete(); // ignore error if it didn't exist
                if (!tmpFile.renameTo(cacheFile))
                    logger.error("Unable to rename " + tmpFile + " to " + cacheFile);
            }

            logger.info(String.format("Saved %s (%d items) in %d ms", cacheType, keys.size(), System.currentTimeMillis() - start));
        }

        private SequentialWriter tempCacheFile(Pair<String, String> pathInfo) throws IOException
        {
            File path = getCachePath(pathInfo.left, pathInfo.right);
            File tmpFile = File.createTempFile(path.getName(), null, path.getParentFile());

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
                }
            }
        }
    }
}
