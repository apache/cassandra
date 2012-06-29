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
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throttle;

/**
 * Manage compaction options.
 */
public class CompactionController
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);

    public final ColumnFamilyStore cfs;
    private final DataTracker.SSTableIntervalTree overlappingTree;

    public final int gcBefore;
    public final int mergeShardBefore;
    private final Throttle throttle = new Throttle("Cassandra_Throttle", new Throttle.ThroughputFunction()
    {
        /** @return Instantaneous throughput target in bytes per millisecond. */
        public int targetThroughput()
        {
            if (DatabaseDescriptor.getCompactionThroughputMbPerSec() < 1 || StorageService.instance.isBootstrapMode())
                // throttling disabled
                return 0;
            // total throughput
            int totalBytesPerMS = DatabaseDescriptor.getCompactionThroughputMbPerSec() * 1024 * 1024 / 1000;
            // per stream throughput (target bytes per MS)
            return totalBytesPerMS / Math.max(1, CompactionManager.instance.getActiveCompactions());
        }
    });

    public CompactionController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore, boolean forceDeserialize)
    {
        this(cfs,
             gcBefore,
             DataTracker.buildIntervalTree(cfs.getOverlappingSSTables(sstables)));
    }

    protected CompactionController(ColumnFamilyStore cfs,
                                   int gcBefore,
                                   DataTracker.SSTableIntervalTree overlappingTree)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        // If we merge an old NodeId id, we must make sure that no further increment for that id are in an active memtable.
        // For that, we must make sure that this id was renewed before the creation of the oldest unflushed memtable. We
        // add 5 minutes to be sure we're on the safe side in terms of thread safety (though we should be fine in our
        // current 'stop all write during memtable switch' situation).
        this.mergeShardBefore = (int) ((cfs.oldestUnflushedMemtable() + 5 * 3600) / 1000);
        this.overlappingTree = overlappingTree;
    }

    public String getKeyspace()
    {
        return cfs.table.name;
    }

    public String getColumnFamily()
    {
        return cfs.columnFamily;
    }

    /**
     * @return true if it's okay to drop tombstones for the given row, i.e., if we know all the verisons of the row
     * are included in the compaction set
     */
    public boolean shouldPurge(DecoratedKey key)
    {
        List<SSTableReader> filteredSSTables = overlappingTree.search(key);
        for (SSTableReader sstable : filteredSSTables)
        {
            if (sstable.getBloomFilter().isPresent(key.key))
                return false;
        }
        return true;
    }

    public void invalidateCachedRow(DecoratedKey key)
    {
        cfs.invalidateCachedRow(key);
    }

    /**
     * @return an AbstractCompactedRow implementation to write the merged rows in question.
     *
     * If there is a single source row, the data is from a current-version sstable, we don't
     * need to purge and we aren't forcing deserialization for scrub, write it unchanged.
     * Otherwise, we deserialize, purge tombstones, and reserialize in the latest version.
     */
    public AbstractCompactedRow getCompactedRow(List<SSTableIdentityIterator> rows)
    {
        long rowSize = 0;
        for (SSTableIdentityIterator row : rows)
            rowSize += row.dataSize;

        if (rowSize > DatabaseDescriptor.getInMemoryCompactionLimit())
        {
            String keyString = cfs.metadata.getKeyValidator().getString(rows.get(0).getKey().key);
            logger.info(String.format("Compacting large row %s/%s:%s (%d bytes) incrementally",
                                      cfs.table.name, cfs.columnFamily, keyString, rowSize));
            return new LazilyCompactedRow(this, rows);
        }
        return new PrecompactedRow(this, rows);
    }

    /** convenience method for single-sstable compactions */
    public AbstractCompactedRow getCompactedRow(SSTableIdentityIterator row)
    {
        return getCompactedRow(Collections.singletonList(row));
    }
    
    public void mayThrottle(long currentBytes)
    {
        throttle.throttle(currentBytes);
    }
}
