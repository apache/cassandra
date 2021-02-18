/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * Maintain a atomic view for read requests, so that requests can read all data during concurrent compactions.
 *
 * All per-column {@link SSTableIndex} updates should be proxied by {@link StorageAttachedIndexGroup} to make
 * sure per-sstable {@link SSTableContext} are in-sync.
 */
public class IndexViewManager
{
    private static final Logger logger = LoggerFactory.getLogger(IndexViewManager.class);
    
    private final ColumnContext context;
    private final AtomicReference<View> view = new AtomicReference<>();

    public IndexViewManager(ColumnContext context)
    {
        this(context, Collections.emptySet());
    }

    @VisibleForTesting
    IndexViewManager(ColumnContext context, Collection<SSTableIndex> indices)
    {
        this.context = context;
        this.view.set(new View(context, indices));
    }

    public View getView()
    {
        return view.get();
    }

    /**
     * Replaces old SSTables with new by creating new immutable view.
     *
     * @param oldSSTables A set of SSTables to remove.
     * @param newSSTableContexts A set of SSTableContexts to add to tracker.
     * @param validate if true, per-column index files' header and footer will be validated.
     * @param rename if true check whether the per-column index components need renaming
     *
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Set<SSTableContext> update(Collection<SSTableReader> oldSSTables, Collection<SSTableContext> newSSTableContexts, boolean validate, boolean rename)
    {
        // Valid indexes on the left and invalid SSTable contexts on the right...
        Pair<Set<SSTableIndex>, Set<SSTableContext>> indexes = context.getBuiltIndexes(newSSTableContexts, validate, rename);

        View currentView, newView;
        Collection<SSTableIndex> newViewIndexes = new HashSet<>();
        Collection<SSTableIndex> releasableIndexes = new ArrayList<>();
        Collection<SSTableReader> toRemove = new HashSet<>(oldSSTables);
        
        do
        {
            currentView = view.get();
            newViewIndexes.clear();
            releasableIndexes.clear();

            for (SSTableIndex sstableIndex : currentView)
            {
                // When aborting early open transaction, toRemove may have the same sstable files as newSSTableContexts,
                // but different SSTableReader java objects with different start positions. So we need to release them
                // from existing view.  see DSP-19677
                SSTableReader sstable = sstableIndex.getSSTable();
                if (toRemove.contains(sstable) || newViewIndexes.contains(sstableIndex))
                    releasableIndexes.add(sstableIndex);
                else
                    newViewIndexes.add(sstableIndex);
            }

            for (SSTableIndex sstableIndex : indexes.left)
            {
                if (newViewIndexes.contains(sstableIndex))
                    releasableIndexes.add(sstableIndex);
                else
                    newViewIndexes.add(sstableIndex);
            }

            newView = new View(context, newViewIndexes);
        }
        while (!view.compareAndSet(currentView, newView));

        releasableIndexes.forEach(SSTableIndex::release);

        if (logger.isTraceEnabled())
            logger.trace(context.logMessage("There are now {} active SSTable indexes."), view.get().getIndexes().size());

        return indexes.right;
    }

    public void drop(Collection<SSTableReader> sstablesToRebuild)
    {
        View currentView = view.get();

        Set<SSTableReader> toRemove = new HashSet<>(sstablesToRebuild);
        for (SSTableIndex index : currentView)
        {
            SSTableReader sstable = index.getSSTable();
            if (!toRemove.contains(sstable))
                continue;

            index.markObsolete();
        }

        update(toRemove, Collections.emptyList(), false, false);
    }

    public void invalidate()
    {
        View currentView = view.get();

        for (SSTableIndex index : currentView)
        {
            index.markObsolete();
        }

        view.set(new View(context, Collections.emptyList()));
    }
}
