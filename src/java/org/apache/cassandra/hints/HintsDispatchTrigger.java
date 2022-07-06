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
package org.apache.cassandra.hints;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
/**
 * A simple dispatch trigger that's being run every 10 seconds.
 *
 * Goes through all hint stores and schedules for dispatch all the hints for hosts that are:
 * 1. Not currently scheduled for dispatch, and
 * 2. Either have some hint files, or an active hint writer, and
 * 3. Are live
 *
 * What does triggering a hints store for dispatch mean?
 * - If there are existing hint files, it means submitting them for dispatch;
 * - If there is an active writer, closing it, for the next run to pick it up.
 */
final class HintsDispatchTrigger implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatchTrigger.class);
    private final HintsCatalog catalog;
    private final HintsWriteExecutor writeExecutor;
    private final HintsDispatchExecutor dispatchExecutor;
    private final AtomicBoolean isPaused;
    public int totalHintFiles;

    HintsDispatchTrigger(HintsCatalog catalog,
                         HintsWriteExecutor writeExecutor,
                         HintsDispatchExecutor dispatchExecutor,
                         AtomicBoolean isPaused)
    {
        this.catalog = catalog;
        this.writeExecutor = writeExecutor;
        this.dispatchExecutor = dispatchExecutor;
        this.isPaused = isPaused;
        this.totalHintFiles = 0;
    }

    public void run()
    {
        if (isPaused.get())
            return;

        catalog.stores()
               .filter(store -> !isScheduled(store))
               .filter(HintsStore::isLive)
               .filter(store -> store.isWriting() || store.hasFiles())
               .forEach(this::schedule);

        /**
         * The Cassandra does not clean up the orphan hint files. If a node N1's hint file
         * say f1.hints & f1.crc is present on node N2, and if the N1 node is no longer part of the Cassandra ring,
         * then f1.hints and f1.crc stay forever. There is no clean-up mechanism for such orphan files.
         * This functionality is by default disabled, but if enabled on N2, and if it finds such orphan files,
         * then it will clean up after those hint files are older than the configured number of days
         */
        // we also count the total hint files present on disk to emit a corresponding metrics
        totalHintFiles = 0;
        detectAndCleanupOrphanHintStores();
        StorageMetrics.totalHintFilesPresent.inc(totalHintFiles);
    }

    private void schedule(HintsStore store)
    {
        if (store.hasFiles())
            dispatchExecutor.dispatch(store);

        if (store.isWriting())
            writeExecutor.closeWriter(store);

        HintsService.instance.getHintsBufferPool().clearEarliestHintsForHostId(store.hostId);
    }

    private boolean isScheduled(HintsStore store)
    {
         return dispatchExecutor.isScheduled(store);
    }

    private void detectAndCleanupOrphanHintStores()
    {
        Set<UUID> allValidNodesCurrentlyInRing = StorageService.instance.getTokenMetadata().getAllEndpointsUUID();
        catalog.stores()
                .filter(store -> countHintFiles(store))
               .filter(store -> !isScheduled(store))
               .filter(store -> isOrphan(store, allValidNodesCurrentlyInRing))
               .forEach(this::purgeOrphanHintFiles);
    }

    public boolean countHintFiles(HintsStore store)
    {
        totalHintFiles += store.getDispatchQueueSize();
        return true;
    }

    @VisibleForTesting
    public boolean isOrphan(HintsStore store, Set<UUID> allValidNodesCurrentlyInRing)
    {
        Instant orphanWindowBoundary = Instant.now().minus(Duration.ofDays(DatabaseDescriptor.getOrphanNodeHintFilesAgeInDays()));
        Instant hintFileCreationTime = Instant.ofEpochMilli(store.getLastUsedTimestamp());
        boolean orphan = !allValidNodesCurrentlyInRing.contains(store.hostId) && hintFileCreationTime.isBefore(orphanWindowBoundary);
        if (orphan)
        {
            StorageMetrics.orphanHintStoresDetected.inc();
            logger.warn("Orphan hint store found. HostID: {}, timestamp: {}, orphanWindowBoundary: {}, hintFileCreationTime: {}, allValidNodesCurrentlyInRing: {}", store.hostId, store.getLastUsedTimestamp(),
                        orphanWindowBoundary,
                        hintFileCreationTime,
                        allValidNodesCurrentlyInRing);
        }
        return orphan;
    }

    private void purgeOrphanHintFiles(HintsStore store)
    {
        if (DatabaseDescriptor.isOrphanNodeHintFilesCleanupEnabled())
        {
            StorageMetrics.orphanHintStoresPurged.inc();
            logger.warn("Removing all the orphan hint store files. HostID: {}, timestamp: {}", store.hostId, store.getLastUsedTimestamp());
            store.deleteAllHints();
        }
    }
}
