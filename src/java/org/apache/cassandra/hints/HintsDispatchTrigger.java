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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;

import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddress;

/**
 * A simple dispatch trigger that's being run every 10 seconds.
 *
 * Goes through all hint stores and schedules for dispatch all the hints for hosts that are:
 * 1. Not currently scheduled for dispatch, and
 * 2. Either have some hint files, or an active hint writer, and
 * 3. Are live, and
 * 4. Have matching schema versions
 *
 * What does triggering a hints store for dispatch mean?
 * - If there are existing hint files, it means submitting them for dispatch;
 * - If there is an active writer, closing it, for the next run to pick it up.
 */
final class HintsDispatchTrigger implements Runnable
{
    private final HintsCatalog catalog;
    private final HintsWriteExecutor writeExecutor;
    private final HintsDispatchExecutor dispatchExecutor;
    private final AtomicBoolean isPaused;

    HintsDispatchTrigger(HintsCatalog catalog,
                         HintsWriteExecutor writeExecutor,
                         HintsDispatchExecutor dispatchExecutor,
                         AtomicBoolean isPaused)
    {
        this.catalog = catalog;
        this.writeExecutor = writeExecutor;
        this.dispatchExecutor = dispatchExecutor;
        this.isPaused = isPaused;
    }

    public void run()
    {
        if (isPaused.get())
            return;

        catalog.stores()
               .filter(store -> !isScheduled(store))
               .filter(HintsStore::isLive)
               .filter(store -> store.isWriting() || store.hasFiles())
               .filter(store -> Gossiper.instance.valuesEqual(getBroadcastAddress(), store.address(), ApplicationState.SCHEMA))
               .forEach(this::schedule);
    }

    private void schedule(HintsStore store)
    {
        if (store.hasFiles())
            dispatchExecutor.dispatch(store);

        if (store.isWriting())
            writeExecutor.closeWriter(store);
    }

    private boolean isScheduled(HintsStore store)
    {
        return dispatchExecutor.isScheduled(store);
    }
}
