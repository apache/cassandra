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
package org.apache.cassandra.io;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTableReader;

/**
 * Manage compaction options.
 */
public class CompactionController
{
    private final ColumnFamilyStore cfs;
    private final Set<SSTableReader> sstables;
    private final boolean forceDeserialize;

    public final boolean isMajor;
    public final int gcBefore;

    private static final CompactionController basicController = new CompactionController(null, Collections.<SSTableReader>emptySet(), false, Integer.MAX_VALUE, false);
    private static final CompactionController basicDeserializingController = new CompactionController(null, Collections.<SSTableReader>emptySet(), false, Integer.MAX_VALUE, true);

    public CompactionController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, boolean isMajor, int gcBefore, boolean forceDeserialize)
    {
        this.cfs = cfs;
        this.isMajor = isMajor;
        this.sstables = new HashSet<SSTableReader>(sstables);
        this.gcBefore = gcBefore;
        this.forceDeserialize = forceDeserialize;
    }

    /**
     * Returns a controller that never purge
     */
    public static CompactionController getBasicController(boolean forceDeserialize)
    {
        return forceDeserialize ? basicDeserializingController : basicController;
    }

    public boolean shouldPurge(DecoratedKey key)
    {
        return isMajor || (cfs != null && !cfs.isKeyInRemainingSSTables(key, sstables));
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
        if (cfs != null)
            cfs.invalidateCachedRow(key);
    }

    public void removeDeletedInCache(DecoratedKey key)
    {
        if (cfs != null)
        {
            ColumnFamily cachedRow = cfs.getRawCachedRow(key);
            if (cachedRow != null)
                ColumnFamilyStore.removeDeleted(cachedRow, gcBefore);
        }
    }

}
