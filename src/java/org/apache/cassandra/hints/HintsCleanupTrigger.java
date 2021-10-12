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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Delete the expired orphaned hints files.
 * An orphaned file is considered as no associating endpoint with its host ID.
 * An expired file is one that has lived longer than the largest gcgs of all tables.
 */
final class HintsCleanupTrigger implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsCleanupTrigger.class);
    private final HintsCatalog hintsCatalog;
    private final HintsDispatchExecutor dispatchExecutor;

    HintsCleanupTrigger(HintsCatalog catalog, HintsDispatchExecutor dispatchExecutor)
    {
        this.hintsCatalog = catalog;
        this.dispatchExecutor = dispatchExecutor;
    }

    public void run()
    {
        if (!DatabaseDescriptor.isAutoHintsCleanupEnabled())
            return;

        hintsCatalog.stores()
                    .filter(store -> StorageService.instance.getEndpointForHostId(store.hostId) == null)
                    .forEach(this::cleanup);
    }

    private void cleanup(HintsStore hintsStore)
    {
        logger.info("Found orphaned hints files for host: {}. Try to delete.", hintsStore.hostId);

        // The host ID has been replaced and the store is still writing hint for the old host
        if (hintsStore.isWriting())
            hintsStore.closeWriter();

        // Interrupt the dispatch if any. At this step, it is certain that the hintsStore is orphaned.
        dispatchExecutor.interruptDispatch(hintsStore.hostId);
        Runnable cleanup = () -> hintsStore.deleteExpiredHints(currentTimeMillis());
        ScheduledExecutors.optionalTasks.execute(cleanup);
    }
}
