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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.io.util.CorruptFileException;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

public class AutoSavingCache<K extends CacheKey, V> extends InstrumentingCache<K, V>
{
    public interface IStreamFactory
    {
        InputStream getInputStream(File dataPath, File crcPath) throws IOException;
        OutputStream getOutputStream(File dataPath, File crcPath) throws FileNotFoundException;
    }

    private static final Logger logger = LoggerFactory.getLogger(AutoSavingCache.class);

    /** True if a cache flush is currently executing: only one may execute at a time. */
    public static final Set<CacheService.CacheType> flushInProgress = new NonBlockingHashSet<CacheService.CacheType>();

    protected volatile ScheduledFuture<?> saveTask;
    protected final CacheService.CacheType cacheType;

    private final CacheSerializer<K, V> cacheLoader;

    /*
     * CASSANDRA-10155 required a format change to fix 2i indexes and caching.
     * 2.2 is already at version "c" and 3.0 is at "d".
     *
     * Since cache versions match exactly and there is no partial fallback just add
     * a minor version letter.
     *
     * Sticking with "d" is fine for 3.0 since it has never been released or used by another version
     *
     * "e" introduced with CASSANDRA-11206, omits IndexInfo from key-cache, stores offset into index-file
     */
    private static final String CURRENT_VERSION = "e";

    private static volatile IStreamFactory streamFactory = new IStreamFactory()
    {
        private final SequentialWriterOption writerOption = SequentialWriterOption.newBuilder()
                                                                    .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                    .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                    .finishOnClose(true).build();

        public InputStream getInputStream(File dataPath, File crcPath) throws IOException
        {
            return ChecksummedRandomAccessReader.open(dataPath, crcPath);
        }

        public OutputStream getOutputStream(File dataPath, File crcPath)
        {
            return new ChecksummedSequentialWriter(dataPath, crcPath, null, writerOption);
        }
    };

    // Unused, but exposed for a reason. See CASSANDRA-8096.
    public static void setStreamFactory(IStreamFactory streamFactory)
    {
        AutoSavingCache.streamFactory = streamFactory;
    }

    public AutoSavingCache(ICache<K, V> cache, CacheService.CacheType cacheType, CacheSerializer<K, V> cacheloader)
    {
        super(cacheType.toString(), cache);
        this.cacheType = cacheType;
        this.cacheLoader = cacheloader;
    }

    public File getCacheDataPath(String version)
    {
        return DatabaseDescriptor.getSerializedCachePath( cacheType, version, "db");
    }

    public File getCacheCrcPath(String version)
    {
        return DatabaseDescriptor.getSerializedCachePath( cacheType, version, "crc");
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
            saveTask = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(runnable,
                                                                               savePeriodInSeconds,
                                                                               savePeriodInSeconds,
                                                                               TimeUnit.SECONDS);
        }
    }

    public ListenableFuture<Integer> loadSavedAsync()
    {
        final ListeningExecutorService es = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        final long start = System.nanoTime();

        ListenableFuture<Integer> cacheLoad = es.submit(new Callable<Integer>()
        {
            @Override
            public Integer call()
            {
                return loadSaved();
            }
        });
        cacheLoad.addListener(new Runnable()
        {
            @Override
            public void run()
            {
                if (size() > 0)
                    logger.info("Completed loading ({} ms; {} keys) {} cache",
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
                            CacheService.instance.keyCache.size(),
                            cacheType);
                es.shutdown();
            }
        }, MoreExecutors.directExecutor());

        return cacheLoad;
    }

    public int loadSaved()
    {
        int count = 0;
        long start = System.nanoTime();

        // modern format, allows both key and value (so key cache load can be purely sequential)
        File dataPath = getCacheDataPath(CURRENT_VERSION);
        File crcPath = getCacheCrcPath(CURRENT_VERSION);
        if (dataPath.exists() && crcPath.exists())
        {
            DataInputStreamPlus in = null;
            try
            {
                logger.info("reading saved cache {}", dataPath);
                in = new DataInputStreamPlus(new LengthAvailableInputStream(new BufferedInputStream(streamFactory.getInputStream(dataPath, crcPath)), dataPath.length()));

                //Check the schema has not changed since CFs are looked up by name which is ambiguous
                UUID schemaVersion = new UUID(in.readLong(), in.readLong());
                if (!schemaVersion.equals(Schema.instance.getVersion()))
                    throw new RuntimeException("Cache schema version "
                                              + schemaVersion.toString()
                                              + " does not match current schema version "
                                              + Schema.instance.getVersion());

                ArrayDeque<Future<Pair<K, V>>> futures = new ArrayDeque<Future<Pair<K, V>>>();
                while (in.available() > 0)
                {
                    //ksname and cfname are serialized by the serializers in CacheService
                    //That is delegated there because there are serializer specific conditions
                    //where a cache key is skipped and not written
                    String ksname = in.readUTF();
                    String cfname = in.readUTF();

                    ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreIncludingIndexes(Pair.create(ksname, cfname));

                    Future<Pair<K, V>> entryFuture = cacheLoader.deserialize(in, cfs);
                    // Key cache entry can return null, if the SSTable doesn't exist.
                    if (entryFuture == null)
                        continue;

                    futures.offer(entryFuture);
                    count++;

                    /*
                     * Kind of unwise to accrue an unbounded number of pending futures
                     * So now there is this loop to keep a bounded number pending.
                     */
                    do
                    {
                        while (futures.peek() != null && futures.peek().isDone())
                        {
                            Future<Pair<K, V>> future = futures.poll();
                            Pair<K, V> entry = future.get();
                            if (entry != null && entry.right != null)
                                put(entry.left, entry.right);
                        }

                        if (futures.size() > 1000)
                            Thread.yield();
                    } while(futures.size() > 1000);
                }

                Future<Pair<K, V>> future = null;
                while ((future = futures.poll()) != null)
                {
                    Pair<K, V> entry = future.get();
                    if (entry != null && entry.right != null)
                        put(entry.left, entry.right);
                }
            }
            catch (CorruptFileException e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn(String.format("Non-fatal checksum error reading saved cache %s", dataPath.getAbsolutePath()), e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.info(String.format("Harmless error reading saved cache %s", dataPath.getAbsolutePath()), t);
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }
        if (logger.isTraceEnabled())
            logger.trace("completed reading ({} ms; {} keys) saved cache {}",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), count, dataPath);
        return count;
    }

    public Future<?> submitWrite(int keysToSave)
    {
        return CompactionManager.instance.submitCacheWrite(getWriter(keysToSave));
    }

    public class Writer extends CompactionInfo.Holder
    {
        private final Iterator<K> keyIterator;
        private final CompactionInfo info;
        private long keysWritten;
        private final long keysEstimate;

        protected Writer(int keysToSave)
        {
            int size = size();
            if (keysToSave >= size || keysToSave == 0)
            {
                keyIterator = keyIterator();
                keysEstimate = size;
            }
            else
            {
                keyIterator = hotKeyIterator(keysToSave);
                keysEstimate = keysToSave;
            }

            OperationType type;
            if (cacheType == CacheService.CacheType.KEY_CACHE)
                type = OperationType.KEY_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.ROW_CACHE)
                type = OperationType.ROW_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.COUNTER_CACHE)
                type = OperationType.COUNTER_CACHE_SAVE;
            else
                type = OperationType.UNKNOWN;

            info = new CompactionInfo(CFMetaData.createFake(SchemaConstants.SYSTEM_KEYSPACE_NAME, cacheType.toString()),
                                      type,
                                      0,
                                      keysEstimate,
                                      Unit.KEYS,
                                      UUIDGen.getTimeUUID());
        }

        public CacheService.CacheType cacheType()
        {
            return cacheType;
        }

        public CompactionInfo getCompactionInfo()
        {
            // keyset can change in size, thus total can too
            // TODO need to check for this one... was: info.forProgress(keysWritten, Math.max(keysWritten, keys.size()));
            return info.forProgress(keysWritten, Math.max(keysWritten, keysEstimate));
        }

        public void saveCache()
        {
            logger.trace("Deleting old {} files.", cacheType);
            deleteOldCacheFiles();

            if (!keyIterator.hasNext())
            {
                logger.trace("Skipping {} save, cache is empty.", cacheType);
                return;
            }

            long start = System.nanoTime();

            Pair<File, File> cacheFilePaths = tempCacheFiles();
            try (WrappedDataOutputStreamPlus writer = new WrappedDataOutputStreamPlus(streamFactory.getOutputStream(cacheFilePaths.left, cacheFilePaths.right)))
            {

                //Need to be able to check schema version because CF names are ambiguous
                UUID schemaVersion = Schema.instance.getVersion();
                if (schemaVersion == null)
                {
                    Schema.instance.updateVersion();
                    schemaVersion = Schema.instance.getVersion();
                }
                writer.writeLong(schemaVersion.getMostSignificantBits());
                writer.writeLong(schemaVersion.getLeastSignificantBits());

                while (keyIterator.hasNext())
                {
                    K key = keyIterator.next();

                    ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreIncludingIndexes(key.ksAndCFName);
                    if (cfs == null)
                        continue; // the table or 2i has been dropped.

                    cacheLoader.serialize(key, writer, cfs);

                    keysWritten++;
                    if (keysWritten >= keysEstimate)
                        break;
                }
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, cacheFilePaths.left);
            }

            File cacheFile = getCacheDataPath(CURRENT_VERSION);
            File crcFile = getCacheCrcPath(CURRENT_VERSION);

            cacheFile.delete(); // ignore error if it didn't exist
            crcFile.delete();

            if (!cacheFilePaths.left.renameTo(cacheFile))
                logger.error("Unable to rename {} to {}", cacheFilePaths.left, cacheFile);

            if (!cacheFilePaths.right.renameTo(crcFile))
                logger.error("Unable to rename {} to {}", cacheFilePaths.right, crcFile);

            logger.info("Saved {} ({} items) in {} ms", cacheType, keysWritten, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }

        private Pair<File, File> tempCacheFiles()
        {
            File dataPath = getCacheDataPath(CURRENT_VERSION);
            File crcPath = getCacheCrcPath(CURRENT_VERSION);
            return Pair.create(FileUtils.createTempFile(dataPath.getName(), null, dataPath.getParentFile()),
                               FileUtils.createTempFile(crcPath.getName(), null, crcPath.getParentFile()));
        }

        private void deleteOldCacheFiles()
        {
            File savedCachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());
            assert savedCachesDir.exists() && savedCachesDir.isDirectory();
            File[] files = savedCachesDir.listFiles();
            if (files != null)
            {
                String cacheNameFormat = String.format("%s-%s.db", cacheType.toString(), CURRENT_VERSION);
                for (File file : files)
                {
                    if (!file.isFile())
                        continue; // someone's been messing with our directory.  naughty!

                    if (file.getName().endsWith(cacheNameFormat)
                     || file.getName().endsWith(cacheType.toString()))
                    {
                        if (!file.delete())
                            logger.warn("Failed to delete {}", file.getAbsolutePath());
                    }
                }
            }
            else
            {
                logger.warn("Could not list files in {}", savedCachesDir);
            }
        }
    }

    public interface CacheSerializer<K extends CacheKey, V>
    {
        void serialize(K key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException;

        Future<Pair<K, V>> deserialize(DataInputPlus in, ColumnFamilyStore cfs) throws IOException;
    }
}
