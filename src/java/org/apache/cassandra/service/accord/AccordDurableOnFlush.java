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

import java.util.Map;
import java.util.function.BiConsumer;

import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.RedundantBefore;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

class AccordDurableOnFlush implements BiConsumer<Long, TableMetadata>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDurableOnFlush.class);

    private Int2ObjectHashMap<RedundantBefore> commandStores = new Int2ObjectHashMap<>();

    AccordDurableOnFlush()
    {
    }

    synchronized boolean add(int commandStoreId, RedundantBefore reportOnFlush)
    {
        if (commandStores == null)
            return false;
        commandStores.merge(commandStoreId, reportOnFlush, RedundantBefore::merge);
        return true;
    }

    @Override
    public void accept(Long memtableId, TableMetadata metadata)
    {
        Int2ObjectHashMap<RedundantBefore> notify;
        synchronized (this)
        {
            notify = commandStores;
            commandStores = null;
        }
        CommandStores commandStores = AccordService.unsafeInstance().node().commandStores();
        for (Map.Entry<Integer, RedundantBefore> e : notify.entrySet())
        {
            RedundantBefore durable = e.getValue();
            notifyInOrder(memtableId, metadata, commandStores.forId(e.getKey()), durable);
        }
    }

    public static void notifyOnDurable(ColumnFamilyStore cfs, CommandStore commandStore, RedundantBefore onDurable)
    {
        View view = cfs.getTracker().getView();
        for (int i = view.liveMemtables.size() - 1; i >= 0 ; --i)
        {
            Memtable candidate = view.liveMemtables.get(i);
            if (candidate.isClean())
                continue;

            AccordDurableOnFlush onFlush = candidate.ensureFlushListener(AccordDataStore.FlushListenerKey.KEY, AccordDurableOnFlush::new);
            if (onFlush != null && onFlush.add(commandStore.id(), onDurable))
                return;
        }

        for (int i = view.flushingMemtables.size() - 1; i >= 0 ; --i)
        {
            Memtable candidate = view.flushingMemtables.get(i);
            AccordDurableOnFlush onFlush = candidate.ensureFlushListener(AccordDataStore.FlushListenerKey.KEY, AccordDurableOnFlush::new);
            if (onFlush != null && onFlush.add(commandStore.id(), onDurable))
                return;
        }

        notifyNow(cfs.metadata(), commandStore, onDurable);
    }

    static void notifyInOrder(long memtableId, TableMetadata metadata, CommandStore commandStore, RedundantBefore report)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        View view = cfs.getTracker().getView();
        boolean notifyNow = true;
        for (Memtable memtable : view.liveMemtables)
            notifyNow &= memtable.getMemtableId() > memtableId;
        for (Memtable memtable : view.flushingMemtables)
            notifyNow &= memtable.getMemtableId() > memtableId;
        if (notifyNow) notifyNow(metadata, commandStore, report);
        else cfs.waitForPriorFlushes().addListener(() -> notifyNow(metadata, commandStore, report));
    }

    static void notifyNow(TableMetadata metadata, CommandStore commandStore, RedundantBefore report)
    {
        logger.debug("Reporting flush of {}/{}; reporting {} to {}", metadata.id, metadata, report, commandStore);
        commandStore.execute((AccordExecutor.Unstoppable) () -> "Report Durable", safeStore -> {
            safeStore.upsertRedundantBefore(report);
        }, commandStore.agent());
    }
}
