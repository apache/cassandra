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

package org.apache.cassandra.service.accord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.DataStore;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.AccordDurableOnFlush.ReportDurable;

public class AccordDataStore implements DataStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDataStore.class);
    enum FlushListenerKey { KEY }

    /**
     * Ensures data for the intersecting ranges is flushed to sstable before calling back with reportOnSuccess.
     * This is used to gate journal cleanup, since we skip the CommitLog for applying to the data table.
     */
    @Override
    public void ensureDurable(CommandStore commandStore, Ranges ranges, RedundantBefore reportOnSuccess, int flags)
    {
        if (commandStore.node().isReplaying() || ranges.isEmpty())
            return;

        logger.debug("{} awaiting local data durability for {}", commandStore, ranges);
        ensureDurableInternal(commandStore, reportOnSuccess, flags);
    }

    @Override
    public void ensureDurable(CommandStore commandStore, RedundantBefore reportOnSuccess, int flags)
    {
        logger.debug("{} awaiting full local data durability", commandStore);
        ensureDurableInternal(commandStore, reportOnSuccess, flags);
    }

    private void ensureDurableInternal(CommandStore commandStore, RedundantBefore redundantBefore, int flags)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(((AccordCommandStore)commandStore).tableId());
        AccordDurableOnFlush.notifyOnDurable(cfs, commandStore, ReportDurable.of(redundantBefore, flags));
    }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback, FetchKind kind)
    {
        switch (kind)
        {
            default: throw new UnhandledEnum(kind);
            case Image:
            {
                AccordFetchCoordinator coordinator;
                try
                {
                    coordinator = new AccordFetchCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore());
                }
                catch (Throwable t)
                {
                    return new FetchResult.Failure(t);
                }

                coordinator.start();
                return coordinator.result();
            }
            case Sync:
            {
                throw new UnsupportedOperationException();
            }
        }
    }
}
