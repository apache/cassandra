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
 *
 */
package org.apache.cassandra.db.compaction;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EchoedRow;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Manage compaction options.
 */
public class CompactionController
{
    private static Logger logger = LoggerFactory.getLogger(CompactionController.class);

    private final ColumnFamilyStore cfs;
    private final Set<SSTableReader> sstables;
    private final boolean forceDeserialize;

    public final boolean isMajor;
    public final int gcBefore;
    private int throttleResolution;

    public CompactionController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore, boolean forceDeserialize)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.sstables = new HashSet<SSTableReader>(sstables);
        this.gcBefore = gcBefore;
        this.forceDeserialize = forceDeserialize;
        isMajor = cfs.isCompleteSSTables(this.sstables);
        // how many rows we expect to compact in 100ms
        long rowSize = cfs.getMeanRowSize();
        int rowsPerSecond = rowSize > 0
                          ? (int) (DatabaseDescriptor.getCompactionThroughputMbPerSec() * 1024 * 1024 / rowSize)
                          : 1000;
        throttleResolution = rowsPerSecond / 10;
        if (throttleResolution <= 0)
            throttleResolution = 1;
    }

    public int getThrottleResolution()
    {
        return throttleResolution;
    }

    public String getKeyspace()
    {
        return cfs.table.name;
    }

    public String getColumnFamily()
    {
        return cfs.columnFamily;
    }

    public boolean shouldPurge(DecoratedKey key)
    {
        return isMajor || !cfs.isKeyInRemainingSSTables(key, sstables);
    }

    public boolean needDeserialize()
    {
        if (forceDeserialize)
            return true;

        for (SSTableReader sstable : sstables)
            if (!sstable.descriptor.isLatestVersion)
                return true;

        return false;
    }

    public void invalidateCachedRow(DecoratedKey key)
    {
        cfs.invalidateCachedRow(key);
    }

    public void removeDeletedInCache(DecoratedKey key)
    {
        ColumnFamily cachedRow = cfs.getRawCachedRow(key);
        if (cachedRow != null)
            ColumnFamilyStore.removeDeleted(cachedRow, gcBefore);
    }

    public boolean isMajor()
    {
        return isMajor;
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
        if (rows.size() == 1 && !needDeserialize() && !shouldPurge(rows.get(0).getKey()))
            return new EchoedRow(this, rows.get(0));

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
}
