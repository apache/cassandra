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

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * Manages per-sstable {@link SSTableContext}s for {@link StorageAttachedIndexGroup}
 */
@ThreadSafe
public class SSTableContextManager
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableContextManager.class);

    private final ConcurrentHashMap<SSTableReader, SSTableContext> sstableContexts = new ConcurrentHashMap<>();

    /**
     * Live sstables with no per-sstable completion marker, and therefore left out of the index view entirely. Every
     * row in one of these answers no index predicate.
     * <p>
     * That is the expected state while an index's initial build has not reached the sstable yet, and a bug
     * afterwards -- so this is recorded rather than acted on, and the two cases are told apart by the caller (see
     * {@link StorageAttachedIndexGroup#onSSTableChanged}, which warns only when an index is already queryable).
     * Before this existed the skip was a bare {@code continue} and the resulting hole was undetectable: nothing
     * throws, nothing logs, and the index still reports itself healthy.
     * <p>
     * Same lifecycle as {@link #sstableContexts}: an entry is added when {@link #update} finds no marker, or when
     * {@link #releaseUnindexed} strips the components of an sstable that stays live, and dropped when the sstable
     * later turns up complete, is {@link #release}d, is found marked compacted, or the manager is {@link #clear()}ed.
     */
    private final Set<SSTableReader> incompleteSSTables = ConcurrentHashMap.newKeySet();

    /**
     * Initialize {@link SSTableContext}s if they are not already initialized.
     *
     * @param removed SSTables being removed
     * @param added SSTables being added
     * @param validation Controls how indexes should be validated
     *
     * @return a set of contexts for SSTables with valid per-SSTable components, and a set of
     * SSTables with invalid or missing components
     */
    public Pair<Set<SSTableContext>, Set<SSTableReader>> update(Collection<SSTableReader> removed, Iterable<SSTableReader> added, IndexValidation validation)
    {
        release(removed);

        Set<SSTableContext> contexts = new HashSet<>();
        Set<SSTableReader> invalid = new HashSet<>();

        incompleteSSTables.removeIf(SSTableReader::isMarkedCompacted);

        for (SSTableReader sstable : added)
        {
            if (sstable.isMarkedCompacted())
            {
                incompleteSSTables.remove(sstable);
                continue;
            }

            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);

            if (!indexDescriptor.isPerSSTableIndexBuildComplete())
            {
                // Don't even try to validate or add the context if the completion marker is missing. Recorded, so
                // that the hole is at least observable: this sstable is live and its rows match no index predicate.
                incompleteSSTables.add(sstable);
                continue;
            }

            incompleteSSTables.remove(sstable);

            try
            {
                // Only validate on restart or newly refreshed SSTable. Newly built files are unlikely to be corrupted.
                if (!sstableContexts.containsKey(sstable) && !indexDescriptor.validatePerSSTableComponents(validation, true, false))
                {
                    invalid.add(sstable);
                    removeInvalidSSTableContext(sstable);
                    continue;
                }
                // ConcurrentHashMap#computeIfAbsent guarantees atomicity, so {@link SSTableContext#create(SSTableReader)}}
                // is called at most once per key.
                contexts.add(sstableContexts.computeIfAbsent(sstable, SSTableContext::create));
            }
            catch (Throwable t)
            {
                logger.warn(indexDescriptor.logMessage("Failed to update per-SSTable components for SSTable {}"), sstable.descriptor, t);
                invalid.add(sstable);
                removeInvalidSSTableContext(sstable);
            }
        }

        return Pair.create(contexts, invalid);
    }

    /**
     * Closes and forgets the contexts of sstables that are leaving the live set. They stop being query-visible at the
     * same time, so they also stop counting as unindexed.
     */
    public void release(Collection<SSTableReader> toRelease)
    {
        toRelease.forEach(incompleteSSTables::remove);
        closeContexts(toRelease);
    }

    /**
     * Closes the contexts of sstables that stay live while their per-sstable components are deleted, ahead of a build
     * that will write them again.
     * <p>
     * Records them as unindexed instead of forgetting them: from here until that build completes -- minutes or hours
     * for a large sstable -- the sstable is live with no index components at all, and a build that dies leaves it that
     * way for good. Dropping the record here would make {@link #incompleteSSTables} read empty for exactly the window
     * in which query results are incomplete.
     */
    void releaseUnindexed(Collection<SSTableReader> toRelease)
    {
        closeContexts(toRelease);

        for (SSTableReader sstable : toRelease)
        {
            if (!sstable.isMarkedCompacted())
                incompleteSSTables.add(sstable);
        }
    }

    private void closeContexts(Collection<SSTableReader> toRelease)
    {
        toRelease.stream().map(sstableContexts::remove).filter(Objects::nonNull).forEach(SSTableContext::close);
    }

    /**
     * A snapshot of the sstables left out of the index view because they carry no per-sstable index components.
     * See {@link #incompleteSSTables}.
     * <p>
     * A copy on purpose: {@link #incompleteSSTables} is mutated by paths that hold no {@link StorageAttachedIndexGroup}
     * monitor, so a caller that tests the set and then reads from it -- to log an example descriptor, say -- would
     * otherwise race with removal and see an empty iterator.
     */
    Set<SSTableReader> incompleteSSTables()
    {
        return ImmutableSet.copyOf(incompleteSSTables);
    }

    /**
     * @return the number of sstables currently recorded in {@link #incompleteSSTables}, without copying the set
     */
    int incompleteSSTableCount()
    {
        return incompleteSSTables.size();
    }

    /**
     * @return total number of per-sstable open files for live sstables
     */
    int openFiles()
    {
        return sstableContexts.values().stream().mapToInt(SSTableContext::openFilesPerSSTable).sum();
    }

    /**
     * @return total disk usage (in bytes) of all per-sstable index files
     */
    long diskUsage()
    {
        return sstableContexts.values().stream().mapToLong(SSTableContext::diskUsage).sum();
    }

    Set<SSTableReader> sstables()
    {
        return sstableContexts.keySet();
    }

    @VisibleForTesting
    public int size()
    {
        return sstableContexts.size();
    }

    @VisibleForTesting
    public void clear()
    {
        sstableContexts.values().forEach(SSTableContext::close);
        sstableContexts.clear();
        // Same lifecycle as the contexts: these readers are no longer tracked here, so leaving their records behind
        // would over-report unindexed sstables and let a released sstable's descriptor turn up in the warning.
        incompleteSSTables.clear();
    }

    @SuppressWarnings("EmptyTryBlock")
    private void removeInvalidSSTableContext(SSTableReader sstable)
    {
        try (SSTableContext ignored = sstableContexts.remove(sstable))
        {
        }
    }
}
