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

        for (SSTableReader sstable : added)
        {
            if (sstable.isMarkedCompacted())
            {
                continue;
            }

            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);

            if (!indexDescriptor.isPerSSTableIndexBuildComplete())
            {
                // Don't even try to validate or add the context if the completion marker is missing.
                continue;
            }

            try
            {
                // Only validate on restart or newly refreshed SSTable. Newly built files are unlikely to be corrupted.
                if (!sstableContexts.containsKey(sstable) && !indexDescriptor.validatePerSSTableComponents(validation))
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

    public void release(Collection<SSTableReader> toRelease)
    {
        toRelease.stream().map(sstableContexts::remove).filter(Objects::nonNull).forEach(SSTableContext::close);
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
    }

    @SuppressWarnings("EmptyTryBlock")
    private void removeInvalidSSTableContext(SSTableReader sstable)
    {
        try (SSTableContext ignored = sstableContexts.remove(sstable))
        {
        }
    }
}
