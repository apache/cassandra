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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexValidation;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * Maintain an atomic view for read requests, so that requests can read all data during concurrent compactions.
 * <p>
 * All per-column {@link SSTableIndex} updates should be proxied by {@link StorageAttachedIndexGroup} to make
 * sure per-sstable {@link SSTableContext} are in-sync.
 */
public class IndexViewManager
{
    private static final Logger logger = LoggerFactory.getLogger(IndexViewManager.class);
    
    private final StorageAttachedIndex index;
    private final AtomicReference<View> view = new AtomicReference<>();

    public IndexViewManager(StorageAttachedIndex index)
    {
        this.index = index;
        this.view.set(new View(index.termType(), Collections.emptySet()));
    }

    public View view()
    {
        return view.get();
    }

    /**
     * Replaces old SSTables with new by creating new immutable view.
     *
     * @param oldSSTables A set of SSTables to remove.
     * @param newSSTableContexts A set of SSTableContexts to add to tracker.
     * @param validation Controls how indexes should be validated
     *
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Collection<SSTableContext> update(Collection<SSTableReader> oldSSTables, Collection<SSTableContext> newSSTableContexts, IndexValidation validation)
    {
        // Valid indexes on the left and invalid SSTable contexts on the right...
        Pair<Collection<SSTableIndex>, Collection<SSTableContext>> indexes = getBuiltIndexes(newSSTableContexts, validation);

        View currentView, newView;
        Collection<SSTableIndex> newViewIndexes = new HashSet<>();
        Collection<SSTableIndex> releasableIndexes = new ArrayList<>();

        do
        {
            currentView = view.get();
            newViewIndexes.clear();
            releasableIndexes.clear();

            for (SSTableIndex sstableIndex : currentView)
            {
                // When aborting early open transaction, toRemove may have the same sstable files as newSSTableContexts,
                // but different SSTableReader java objects with different start positions. So we need to release them
                // from existing view.
                SSTableReader sstable = sstableIndex.getSSTable();
                if (oldSSTables.contains(sstable) || newViewIndexes.contains(sstableIndex))
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

            newView = new View(index.termType(), newViewIndexes);
        }
        while (!view.compareAndSet(currentView, newView));

        releasableIndexes.forEach(SSTableIndex::release);

        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("There are now {} active SSTable indexes."), view.get().getIndexes().size());

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

        update(toRemove, Collections.emptyList(), IndexValidation.NONE);
    }

    /**
     * Called when index is dropped. Mark all {@link SSTableIndex} as released and per-column index files
     * will be removed when in-flight queries are completed.
     */
    public void invalidate()
    {
        View previousView = view.getAndSet(new View(index.termType(), Collections.emptyList()));

        for (SSTableIndex index : previousView)
        {
            index.markObsolete();
        }
    }

    /**
     * @return the indexes that are built on the given SSTables on the left and corrupted indexes'
     * corresponding contexts on the right
     */
    private Pair<Collection<SSTableIndex>, Collection<SSTableContext>> getBuiltIndexes(Collection<SSTableContext> sstableContexts, IndexValidation validation)
    {
        Set<SSTableIndex> valid = new HashSet<>(sstableContexts.size());
        Set<SSTableContext> invalid = new HashSet<>();

        for (SSTableContext sstableContext : sstableContexts)
        {
            if (sstableContext.sstable.isMarkedCompacted())
                continue;

            if (!sstableContext.indexDescriptor.isPerColumnIndexBuildComplete(index.identifier()))
            {
                logger.debug(index.identifier().logMessage("An on-disk index build for SSTable {} has not completed."), sstableContext.descriptor());
                continue;
            }

            if (sstableContext.indexDescriptor.isIndexEmpty(index.termType(), index.identifier()))
            {
                logger.debug(index.identifier().logMessage("No on-disk index was built for SSTable {} because the SSTable " +
                                                           "had no indexable rows for the index."), sstableContext.descriptor());
                continue;
            }

            try
            {
                if (validation != IndexValidation.NONE)
                {
                    if (!sstableContext.indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), validation))
                    {
                        invalid.add(sstableContext);
                        continue;
                    }
                }

                SSTableIndex ssTableIndex = sstableContext.newSSTableIndex(index);
                logger.debug(index.identifier().logMessage("Successfully created index for SSTable {}."), sstableContext.descriptor());

                // Try to add new index to the set, if set already has such index, we'll simply release and move on.
                // This covers situation when SSTable collection has the same SSTable multiple
                // times because we don't know what kind of collection it actually is.
                if (!valid.add(ssTableIndex))
                {
                    ssTableIndex.release();
                }
            }
            catch (Throwable e)
            {
                logger.warn(index.identifier().logMessage("Failed to update per-column components for SSTable {}"), sstableContext.descriptor(), e);
                invalid.add(sstableContext);
            }
        }

        return Pair.create(valid, invalid);
    }
}
