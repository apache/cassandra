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

package org.apache.cassandra.io.sstable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.cache.JMXInstrumentedCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class SSTableTracker implements Iterable<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableTracker.class);

    private volatile Set<SSTableReader> sstables;
    private final AtomicLong liveSize = new AtomicLong();
    private final AtomicLong totalSize = new AtomicLong();

    private final String ksname;
    private final String cfname;

    private final JMXInstrumentedCache<Pair<Descriptor,DecoratedKey>,Long> keyCache;
    private final JMXInstrumentedCache<DecoratedKey, ColumnFamily> rowCache;

    public SSTableTracker(String ksname, String cfname)
    {
        this.ksname = ksname;
        this.cfname = cfname;
        sstables = Collections.emptySet();
        keyCache = new JMXInstrumentedCache<Pair<Descriptor,DecoratedKey>,Long>(ksname, cfname + "KeyCache", 0);
        rowCache = new JMXInstrumentedCache<DecoratedKey, ColumnFamily>(ksname, cfname + "RowCache", 3);
    }

    protected class CacheWriter<K, V>
    {
        public void saveCache(JMXInstrumentedCache<K, V> cache, File savedCachePath, Function<K, ByteBuffer> converter) throws IOException
        {
            long start = System.currentTimeMillis();
            String msgSuffix = savedCachePath.getName() + " for " + cfname + " of " + ksname;
            logger.info("saving " + msgSuffix);
            int count = 0;
            File tmpFile = File.createTempFile(savedCachePath.getName(), null, savedCachePath.getParentFile());
            FileOutputStream fout = new FileOutputStream(tmpFile);
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fout));
            FileDescriptor fd = fout.getFD();
            for (K key : cache.getKeySet())
            {
                ByteBuffer bytes = converter.apply(key);
                out.writeInt(bytes.remaining());
                out.write(bytes.array(),bytes.position()+bytes.arrayOffset(),bytes.remaining());
                ++count;
            }
            out.flush();
            fd.sync();
            out.close();
            if (!tmpFile.renameTo(savedCachePath))
                throw new IOException("Unable to rename cache to " + savedCachePath);
            if (logger.isDebugEnabled())
                logger.debug("saved " + count + " keys in " + (System.currentTimeMillis() - start) + " ms from " + msgSuffix);
        }
    }

    public void saveKeyCache() throws IOException
    {
        Function<Pair<Descriptor, DecoratedKey>, ByteBuffer> function = new Function<Pair<Descriptor, DecoratedKey>, ByteBuffer>()
        {
            public ByteBuffer apply(Pair<Descriptor, DecoratedKey> key)
            {
                return key.right.key;
            }
        };
        CacheWriter<Pair<Descriptor, DecoratedKey>, Long> writer = new CacheWriter<Pair<Descriptor, DecoratedKey>, Long>();
        writer.saveCache(keyCache, DatabaseDescriptor.getSerializedKeyCachePath(ksname, cfname), function);
    }

    public void saveRowCache() throws IOException
    {
        Function<DecoratedKey, ByteBuffer> function = new Function<DecoratedKey, ByteBuffer>()
        {
            public ByteBuffer apply(DecoratedKey key)
            {
                return key.key;
            }
        };
        CacheWriter<DecoratedKey, ColumnFamily> writer = new CacheWriter<DecoratedKey, ColumnFamily>();
        writer.saveCache(rowCache, DatabaseDescriptor.getSerializedRowCachePath(ksname, cfname), function);
    }

    public synchronized void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        Set<SSTableReader> sstablesNew = new HashSet<SSTableReader>(sstables);

        for (SSTableReader sstable : replacements)
        {
            assert sstable.getKeySamples() != null;
            sstablesNew.add(sstable);
            long size = sstable.bytesOnDisk();
            liveSize.addAndGet(size);
            totalSize.addAndGet(size);
            sstable.setTrackedBy(this);
        }

        long maxDataAge = -1;
        for (SSTableReader sstable : oldSSTables)
        {
            boolean removed = sstablesNew.remove(sstable);
            assert removed;
            sstable.markCompacted();
            maxDataAge = Math.max(maxDataAge, sstable.maxDataAge);
            liveSize.addAndGet(-sstable.bytesOnDisk());
        }

        sstables = Collections.unmodifiableSet(sstablesNew);
        updateCacheSizes();
    }

    public synchronized void add(Iterable<SSTableReader> sstables)
    {
        assert sstables != null;
        replace(Collections.<SSTableReader>emptyList(), sstables);
    }

    public synchronized void markCompacted(Collection<SSTableReader> compacted)
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

    public JMXInstrumentedCache<DecoratedKey, ColumnFamily> getRowCache()
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

    public JMXInstrumentedCache<Pair<Descriptor, DecoratedKey>, Long> getKeyCache()
    {
        return keyCache;
    }
}

