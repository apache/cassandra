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

    public static class ReportDurable
    {
        public static final int COMMAND_STORE_FLUSH = 1;
        public static final int DATA_STORE_FLUSH = 2;

        public final RedundantBefore redundantBefore;
        final int flags;

        private ReportDurable(RedundantBefore redundantBefore, int flags)
        {
            this.redundantBefore = redundantBefore;
            this.flags = flags;
        }

        public boolean isDataStoreFlush()
        {
            return isDataStoreFlush(flags);
        }

        public static boolean isDataStoreFlush(int flags)
        {
            return 0 != (flags & DATA_STORE_FLUSH);
        }

        public boolean isCommandStoreFlush()
        {
            return isCommandStoreFlush(flags);
        }

        public static boolean isCommandStoreFlush(int flags)
        {
            return 0 != (flags & COMMAND_STORE_FLUSH);
        }

        public static ReportDurable of(RedundantBefore redundantBefore)
        {
            return of(redundantBefore, 0);
        }

        public static ReportDurable of(RedundantBefore redundantBefore, int flags)
        {
            return new ReportDurable(redundantBefore, flags);
        }

        public static ReportDurable commandStoreFlush()
        {
            return new ReportDurable(RedundantBefore.EMPTY, COMMAND_STORE_FLUSH);
        }

        static ReportDurable merge(ReportDurable a, ReportDurable b)
        {
            return new ReportDurable(RedundantBefore.merge(a.redundantBefore, b.redundantBefore), a.flags | b.flags);
        }

        @Override
        public String toString()
        {
            return redundantBefore.toString();
        }
    }

    private Int2ObjectHashMap<ReportDurable> commandStores = new Int2ObjectHashMap<>();

    AccordDurableOnFlush()
    {
    }

    synchronized boolean add(int commandStoreId, ReportDurable reportOnFlush)
    {
        if (commandStores == null)
            return false;
        commandStores.merge(commandStoreId, reportOnFlush, ReportDurable::merge);
        return true;
    }

    @Override
    public void accept(Long memtableId, TableMetadata metadata)
    {
        Int2ObjectHashMap<ReportDurable> notify;
        synchronized (this)
        {
            notify = commandStores;
            commandStores = null;
        }
        CommandStores commandStores = AccordService.unsafeInstance().node().commandStores();
        for (Map.Entry<Integer, ReportDurable> e : notify.entrySet())
        {
            ReportDurable durable = e.getValue();
            notifyInOrder(memtableId, metadata, commandStores.forId(e.getKey()), durable);
        }
    }

    public static void notifyOnDurable(ColumnFamilyStore cfs, CommandStore commandStore, ReportDurable onDurable)
    {
        if (cfs == null)
        {
            // TODO (required): is this correct? Revisit when we improve DROP TABLE
            notifyNow(commandStore, onDurable);
            return;
        }
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

        notifyNow(commandStore, onDurable);
    }

    static void notifyInOrder(long memtableId, TableMetadata metadata, CommandStore commandStore, ReportDurable report)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        if (cfs == null)
        {
            notifyNow(commandStore, report);
            return;
        }
        View view = cfs.getTracker().getView();
        boolean notifyNow = true;
        for (Memtable memtable : view.liveMemtables)
            notifyNow &= memtable.getMemtableId() > memtableId;
        for (Memtable memtable : view.flushingMemtables)
            notifyNow &= memtable.getMemtableId() > memtableId;
        if (notifyNow) notifyNow(commandStore, report);
        else cfs.waitForPriorFlushes().addListener(() -> notifyNow(commandStore, report));
    }

    static void notifyNow(CommandStore commandStore, ReportDurable report)
    {
        logger.debug("{} reporting flush with {}", commandStore, report);
        commandStore.execute((AccordExecutor.Unstoppable) () -> "Report Durable", safeStore -> {
            safeStore.reportDurable(report.redundantBefore, report.flags);
        }, commandStore.agent());
    }
}
