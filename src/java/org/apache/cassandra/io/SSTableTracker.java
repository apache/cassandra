package org.apache.cassandra.io;
/*
 * 
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
 * 
 */


import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;

import org.apache.cassandra.cache.JMXInstrumentedCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.Pair;
import org.apache.log4j.Logger;

public class SSTableTracker implements Iterable<SSTableReader>
{
    private static final Logger logger = Logger.getLogger(SSTableTracker.class);

    private volatile Set<SSTableReader> sstables;
    private final AtomicLong liveSize = new AtomicLong();
    private final AtomicLong totalSize = new AtomicLong();

    private final String ksname;
    private final String cfname;

    private final JMXInstrumentedCache<Pair<String, DecoratedKey>, SSTable.PositionSize> keyCache;
    private final JMXInstrumentedCache<String, ColumnFamily> rowCache;

    public SSTableTracker(String ksname, String cfname)
    {
        this.ksname = ksname;
        this.cfname = cfname;
        sstables = Collections.emptySet();
        keyCache = new JMXInstrumentedCache<Pair<String, DecoratedKey>, SSTable.PositionSize>(ksname, cfname + "KeyCache", 0);
        rowCache = new JMXInstrumentedCache<String, ColumnFamily>(ksname, cfname + "RowCache", 0);
    }

    protected class CacheWriter<K, V>
    {
        public void saveCache(JMXInstrumentedCache<K, V> cache, File savedCachePath, Function<K, byte[]> converter) throws IOException
        {
            long start = System.currentTimeMillis();
            String msgSuffix = " " + savedCachePath.getName() + " for " + cfname + " of " + ksname;
            logger.debug("saving" + msgSuffix);
            int count = 0;
            File tmpFile = File.createTempFile(savedCachePath.getName(), null, savedCachePath.getParentFile());
            FileOutputStream fout = new FileOutputStream(tmpFile);
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fout));
            FileDescriptor fd = fout.getFD();
            for (K key : cache.getKeySet())
            {
                byte[] bytes = converter.apply(key);
                out.writeInt(bytes.length);
                out.write(bytes);
                ++count;
            }
            out.flush();
            fd.sync();
            out.close();
            if (!tmpFile.renameTo(savedCachePath))
                throw new IOException("Unable to rename cache to " + savedCachePath);
            if (logger.isDebugEnabled())
                logger.debug("saved " + count + " keys in " + (System.currentTimeMillis() - start) + " ms from" + msgSuffix);
        }
    }

    public void saveKeyCache() throws IOException
    {
        Function<Pair<String, DecoratedKey>, byte[]> function = new Function<Pair<String, DecoratedKey>, byte[]>()
        {
            public byte[] apply(Pair<String, DecoratedKey> key)
            {
                return key.right.key.getBytes(Charset.forName("UTF-8"));
            }
        };
        CacheWriter<Pair<String, DecoratedKey>, SSTable.PositionSize> writer = new CacheWriter<Pair<String, DecoratedKey>, SSTable.PositionSize>();
        writer.saveCache(keyCache, DatabaseDescriptor.getSerializedKeyCachePath(ksname, cfname), function);
    }

    public void saveRowCache() throws IOException
    {
        Function<String, byte[]> function = new Function<String, byte[]>()
        {
            public byte[] apply(String key)
            {
                return key.getBytes(Charset.forName("UTF-8"));
            }
        };
        CacheWriter<String, ColumnFamily> writer = new CacheWriter<String, ColumnFamily>();
        writer.saveCache(rowCache, DatabaseDescriptor.getSerializedRowCachePath(ksname, cfname), function);
    }

    public synchronized void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements) throws IOException
    {
        Set<SSTableReader> sstablesNew = new HashSet<SSTableReader>(sstables);

        for (SSTableReader sstable : replacements)
        {
            assert sstable.getIndexPositions() != null;
            sstablesNew.add(sstable);
            long size = sstable.bytesOnDisk();
            liveSize.addAndGet(size);
            totalSize.addAndGet(size);
            sstable.setTrackedBy(this);
        }

        for (SSTableReader sstable : oldSSTables)
        {
            boolean removed = sstablesNew.remove(sstable);
            assert removed;
            sstable.markCompacted();
            liveSize.addAndGet(-sstable.bytesOnDisk());
        }

        sstables = Collections.unmodifiableSet(sstablesNew);
        updateCacheSizes();
    }

    public synchronized void add(Iterable<SSTableReader> sstables)
    {
        assert sstables != null;
        try
        {
            replace(Collections.<SSTableReader>emptyList(), sstables);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public synchronized void markCompacted(Collection<SSTableReader> compacted) throws IOException
    {
        replace(compacted, Collections.<SSTableReader>emptyList());
    }

    /**
     * Resizes the key and row caches based on the current key estimate.
     */
    public synchronized void updateCacheSizes()
    {
        long keys = estimatedKeys();

        if (!keyCache.isCapacitySetManually())
        {
            int keyCacheSize = DatabaseDescriptor.getKeysCachedFor(ksname, cfname, keys);
            if (keyCacheSize != keyCache.getCapacity())
            {
                // update cache size for the new key volume
                if (logger.isDebugEnabled())
                    logger.debug("key cache capacity for " + cfname + " is " + keyCacheSize);
                keyCache.updateCapacity(keyCacheSize);
            }
        }

        if (!rowCache.isCapacitySetManually())
        {
            int rowCacheSize = DatabaseDescriptor.getRowsCachedFor(ksname, cfname, keys);
            if (rowCacheSize != rowCache.getCapacity())
            {
                if (logger.isDebugEnabled())
                    logger.debug("row cache capacity for " + cfname + " is " + rowCacheSize);
                rowCache.updateCapacity(rowCacheSize);
            }
        }
    }

    // the modifiers create new, unmodifiable objects each time; the volatile fences the assignment
    // so we don't need any further synchronization for the common case here
    public Set<SSTableReader> getSSTables()
    {
        return sstables;
    }

    public int size()
    {
        return sstables.size();
    }

    public Iterator<SSTableReader> iterator()
    {
        return sstables.iterator();
    }

    public synchronized void clearUnsafe()
    {
        sstables = Collections.emptySet();
    }

    public JMXInstrumentedCache<String, ColumnFamily> getRowCache()
    {
        return rowCache;
    }

    public long estimatedKeys()
    {
        long n = 0;
        for (SSTableReader sstable : this)
        {
            n += sstable.estimatedKeys();
        }
        return n;
    }

    public long getLiveSize()
    {
        return liveSize.get();
    }

    public long getTotalSize()
    {
        return totalSize.get();
    }

    public void spaceReclaimed(long size)
    {
        totalSize.addAndGet(-size);
    }

    public JMXInstrumentedCache<Pair<String, DecoratedKey>, SSTable.PositionSize> getKeyCache()
    {
        return keyCache;
    }
}

