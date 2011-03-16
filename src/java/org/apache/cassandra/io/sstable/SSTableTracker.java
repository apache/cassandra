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

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.JMXInstrumentedCache;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.Pair;

public class SSTableTracker implements Iterable<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableTracker.class);

    private volatile Set<SSTableReader> sstables;
    private final AtomicLong liveSize = new AtomicLong();
    private final AtomicLong totalSize = new AtomicLong();

    private final ColumnFamilyStore cfs;

    public SSTableTracker(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        sstables = Collections.emptySet();
    }

    public synchronized void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        Set<SSTableReader> sstablesNew = new HashSet<SSTableReader>(sstables);

        for (SSTableReader sstable : replacements)
        {
            assert sstable.getKeySamples() != null;
            if (logger.isDebugEnabled())
                logger.debug(String.format("adding %s to list of files tracked for %s.%s",
                                           sstable.descriptor, cfs.table.name, cfs.getColumnFamilyName()));
            sstablesNew.add(sstable);
            long size = sstable.bytesOnDisk();
            liveSize.addAndGet(size);
            totalSize.addAndGet(size);
            sstable.setTrackedBy(this);
        }

        long maxDataAge = -1;
        for (SSTableReader sstable : oldSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("removing %s from list of files tracked for %s.%s",
                                           sstable.descriptor, cfs.table.name, cfs.getColumnFamilyName()));
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
        cfs.keyCache.updateCacheSize(keys);
        cfs.rowCache.updateCacheSize(keys);
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

    public JMXInstrumentedCache<Pair<Descriptor, DecoratedKey>, Long> getKeyCache()
    {
        return cfs.keyCache;
    }

    public JMXInstrumentedCache<DecoratedKey, ColumnFamily> getRowCache()
    {
        return cfs.rowCache;
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
}

