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
import accord.local.cfk.CommandsForKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

public class AccordDataStore implements DataStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDataStore.class);
    enum FlushListenerKey { KEY }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        AccordFetchCoordinator coordinator = new AccordFetchCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore());
        coordinator.start();
        return coordinator.result();
    }

    /**
     * Ensures data for the intersecting ranges is flushed to sstable before calling back with reportOnSuccess.
     * This is used to gate journal cleanup, since we skip the CommitLog for applying to the data table.
     */
    public void ensureDurable(CommandStore commandStore, Ranges ranges, RedundantBefore reportOnSuccess)
    {
        if (!CommandsForKey.reportLinearizabilityViolations())
            return;

        logger.debug("{} awaiting local data durability of {}", commandStore, ranges);
        ColumnFamilyStore prev = null;
        for (Range range : ranges)
        {
            ColumnFamilyStore cfs;
            if (prev != null && prev.metadata().id.equals(range.prefix())) cfs = prev;
            else cfs = Schema.instance.getColumnFamilyStoreInstance((TableId) range.prefix());
            if (cfs == null)
            {
                // TODO (expected): should we record this as durable?
                continue;
            }

            while (true)
            {
                Memtable memtable = cfs.getCurrentMemtable();
                // If RX came when after a quiet period or if it raced with a previous memtable flush
                if (memtable.isClean())
                {
                    AccordDurableOnFlush.notify(cfs.metadata(), commandStore, reportOnSuccess);
                    break;
                }

                AccordDurableOnFlush onFlush = memtable.ensureFlushListener(FlushListenerKey.KEY, AccordDurableOnFlush::new);
                if (onFlush != null && onFlush.add(commandStore.id(), reportOnSuccess))
                    break;

                if (cfs == prev)
                {
                    // we must already have a successful notify, so just propagate
                    AccordDurableOnFlush.notify(cfs.metadata(), commandStore, reportOnSuccess);
                    break;
                }
            }

            prev = cfs;
        }
    }
}
