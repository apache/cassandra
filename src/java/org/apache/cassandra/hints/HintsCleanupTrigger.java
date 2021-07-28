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

import org.apache.cassandra.service.StorageService;

/**
 * Delete the expired orphaned hints files.
 * An orphaned file is considered as no associating endpoint with its host ID.
 * An expired file is one that has lived longer than {@link Hint#maxHintTTL}.
 */
final class HintsCleanupTrigger implements Runnable
{
    private final HintsCatalog hintsCatalog;
    private final HintsCleanupExecutor cleanupExecutor;
    private final HintsDispatchExecutor dispatchExecutor;

    HintsCleanupTrigger(HintsCatalog catalog, HintsCleanupExecutor cleanupExecutor, HintsDispatchExecutor dispatchExecutor)
    {
        this.hintsCatalog = catalog;
        this.cleanupExecutor = cleanupExecutor;
        this.dispatchExecutor = dispatchExecutor;
    }

    public void run()
    {
        hintsCatalog.stores()
                    .filter(store -> StorageService.instance.getEndpointForHostId(store.hostId) == null)
                    .forEach(this::cleanup);
    }

    private void cleanup(HintsStore hintsStore)
    {
        // The host ID has been replaced and the store is still writing hint for the old host
        if (hintsStore.isWriting())
            hintsStore.closeWriter();

        // Interrupt the dispatch if any. The case should be very rare.
        dispatchExecutor.interruptDispatch(hintsStore.hostId);

        cleanupExecutor.cleanup(hintsStore);
    }
}
