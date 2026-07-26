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
package org.apache.cassandra.index.sai;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Manage per-sstable {@link SSTableContext} for {@link StorageAttachedIndexGroup}
 */
@ThreadSafe
public class SSTableContextManager
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Even though `SSTableContext` happens to point to its corresponding `IndexDescriptor` for convenience, we track
    // the latter separately because we need to track descriptors before it is safe to build a context (we create
    // a descriptor as soon as we start indexing a sstable to start tracking the added components, but can only create
    // its context when the per-sstable components are complete).
    private final ConcurrentHashMap<SSTableReader, IndexDescriptor> sstableDescriptors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SSTableReader, SSTableContext> sstableContexts = new ConcurrentHashMap<>();

    private final Tracker tracker;

    SSTableContextManager(Tracker tracker)
    {
        this.tracker = tracker;
    }

    /**
     * Initialize {@link SSTableContext}s if they are not already initialized.
     *
     * @param removed SSTables being removed
     * @param added SSTables being added
     * @param validate if true, header and footer will be validated.
     *
     * @return if all the added (and still "live" at the time of this call) sstable with complete index build have
     * valid per-sstable components, then an optional with the context for all those sstables. Otherwise, if any sstable
     * has invalid/missing components, then an empty optional is returned (and all invalid sstable will have had their
     * context removed, after a call to onInvalid).
     */
    @SuppressWarnings("resource")
    public Optional<Set<SSTableContext>> update(Collection<SSTableReader> removed, Iterable<SSTableReader> added, boolean validate, Set<StorageAttachedIndex> indices)
    {
        release(removed);

        Set<SSTableContext> contexts = new HashSet<>();
        boolean hasInvalid = false;

        for (SSTableReader sstable : added)
        {
            if (sstable.isMarkedCompacted())
            {
                logger.debug("Skipped tracking sstable {} because it's marked compacted", sstable);
                continue;
            }

            IndexDescriptor indexDescriptor = getOrLoadIndexDescriptor(sstable, indices);
            var perSSTableComponents = indexDescriptor.perSSTableComponents();
            if (!perSSTableComponents.isComplete())
            {
                // This usually means no index has been built for that sstable yet (the alternative would be that we
                // lost the completion marker when index build failed). Not point in running
                // validation (it would fail), and we also don't want to add it to the returned contexts, since it's
                // not ready yet. We know a future call of this method will be triggered for that sstable once the
                // index finishes building.
                logger.debug("Skipped tracking sstable {} because per sstable components are not complete (components={})", sstable, perSSTableComponents.all());
                continue;
            }

            try
            {
                // Only validate on restart or newly refreshed SSTable. Newly built files are unlikely to be corrupted.
                if (validate && !sstableContexts.containsKey(sstable) && !perSSTableComponents.validateComponents(sstable, tracker, false))
                {
                    // Note that the validation already log details on the problem if it fails, so no reason to log further
                    hasInvalid = true;
                    continue;
                }
                // ConcurrentHashMap#compute guarantees atomicity, so {@link SSTableContext#create(SSTableReader)}} is
                // called at most once per key and underlying components.
                contexts.add(sstableContexts.compute(sstable, (__, prevContext) -> computeUpdatedContext(sstable, prevContext, perSSTableComponents)));
            }
            catch (Throwable t)
            {
                logger.warn(indexDescriptor.logMessage("Unexpected error updating per-SSTable components for SSTable {}"), sstable.descriptor, t);
                // We haven't been able to correctly set the context, so the index shouldn't be used, and we invalidate
                // the components to ensure that's the case.
                perSSTableComponents.invalidate(sstable, tracker);
                hasInvalid = true;
                remove(sstable);
            }
        }

        return hasInvalid ? Optional.empty() : Optional.of(contexts);
    }

    private static SSTableContext computeUpdatedContext(SSTableReader reader, @Nullable SSTableContext previousContext, IndexComponents.ForRead perSSTableComponents)
    {
        // We can (and should) keep the previous context if both:
        // 1. it exists
        // 2. it uses a "complete" set of per-sstable components (not that we always initially create a `SSTableContext`
        //    from a complete set, so if it is not complete, it means the previous components have been corrupted, and
        //    we want to use the new one (a rebuild)).
        // 3. it uses "up-to-date" per-sstable components.
        if (previousContext != null && previousContext.usedPerSSTableComponents().isComplete() && previousContext.usedPerSSTableComponents().buildId().equals(perSSTableComponents.buildId()))
            return previousContext;

        // Now, if we create a new one, we should close the previous one if it exists.
        // Note that `SSTableIndex` references `SSTableContext` through a `#sharedCopy() so even if there is still
        // index referencing this context currently in use, this will not break ongoing queries.
        if (previousContext != null)
            previousContext.close();

        return SSTableContext.create(reader, perSSTableComponents);
    }

    private void release(Collection<SSTableReader> toRelease)
    {
        toRelease.forEach(this::remove);
    }

    Collection<SSTableContext> allContexts()
    {
        return sstableContexts.values();
    }

    @VisibleForTesting
    SSTableContext getContext(SSTableReader sstable)
    {
        return sstableContexts.get(sstable);
    }

    /**
     * @return total number of per-sstable open files for live sstables
     */
    int openFiles()
    {
        return sstableContexts.values().stream().mapToInt(SSTableContext::openFilesPerSSTable).sum();
    }

    /**
     * @return total disk usage of all per-sstable index files
     */
    long diskUsage()
    {
        return sstableContexts.values().stream()
                              .mapToLong(ssTableContext -> ssTableContext.usedPerSSTableComponents().liveSizeOnDiskInBytes())
                              .sum();
    }

    @VisibleForTesting
    public boolean contains(SSTableReader sstable)
    {
        return sstableContexts.containsKey(sstable);
    }

    @VisibleForTesting
    public int size()
    {
        return sstableContexts.size();
    }

    @VisibleForTesting
    public void clear()
    {
        sstableDescriptors.clear();
        sstableContexts.values().forEach(SSTableContext::close);
        sstableContexts.clear();
    }

    @SuppressWarnings("resource")
    private void remove(SSTableReader sstable)
    {
        sstableDescriptors.remove(sstable);
        SSTableContext context = sstableContexts.remove(sstable);
        if (context != null)
            context.close();
    }

    /**
     * Returns a descriptor for the given sstable and indexes, reusing a cached one when possible.
     * <p>
     * Note that during index initialization, index is added to {@link StorageAttachedIndexGroup} one by one but initialization
     * tasks are executed concurrently, each task may have a different view of registered indexes. The cache must handle
     * it and load indexes if they are not already loaded by the cached IndexDescriptor.
     */
    IndexDescriptor getOrLoadIndexDescriptor(SSTableReader sstable, Set<StorageAttachedIndex> indices)
    {
        Set<IndexContext> requested = contexts(indices);
        return sstableDescriptors.compute(sstable, (k, existing) -> {
            // load for the 1st time
            if (existing == null)
                return IndexDescriptor.load(sstable, requested);

            // all requested indexes are loaded
            if (existing.includedIndexes().containsAll(requested))
                return existing;

            logger.debug("Reloading existing IndexDescriptor for {} because included indexes {} do not include all indexes from {}", sstable.descriptor.id, existing.includedIndexes(), requested);
            // Load indexes that are not included. Not creating a new IndexDescriptor because existing IndexDescriptor
            // is already referenced in SSTableContext#perSSTableComponents
            existing = existing.loadIfAbsent(sstable, requested);
            return existing;
        });
    }

    @VisibleForTesting
    public int getDescriptorCount()
    {
        return sstableDescriptors.size();
    }

    private static Set<IndexContext> contexts(Set<StorageAttachedIndex> indices)
    {
        Set<IndexContext> contexts = Sets.newHashSetWithExpectedSize(indices.size());
        for (StorageAttachedIndex index : indices)
            contexts.add(index.getIndexContext());
        return contexts;
    }
}
