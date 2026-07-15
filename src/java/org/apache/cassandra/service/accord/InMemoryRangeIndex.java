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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;

import javax.annotation.Nullable;

import accord.impl.cfr.IdEntry;
import accord.impl.cfr.InMemoryRangeSummaryIndex;
import accord.impl.cfr.LoadListener;
import accord.local.Command;
import accord.local.CommandSummaries.Summary;
import accord.local.CommandSummaries.SummaryLoader;
import accord.local.LoadKeysFor;
import accord.local.MaxDecidedRX;
import accord.local.RedundantBefore;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.async.Cancellable;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;

import static org.apache.cassandra.io.util.CompressedFrameDataInputPlus.readList;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.writeList;

public class InMemoryRangeIndex extends InMemoryRangeSummaryIndex implements RangeIndex
{
    public static class Loader extends RangeIndex.Loader
    {
        private final InMemoryRangeIndex owner;
        private Cancellable unregister;

        public Loader(InMemoryRangeIndex owner, RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId, LoadKeysFor loadKeysFor)
        {
            super(redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKinds, minTxnId, maxTxnId, loadKeysFor);
            this.owner = owner;
        }

        @Override
        public void loadExclusive(Map<Timestamp, Summary> into, AccordCommandStore.Caches caches)
        {
            if (loadKeysFor == LoadKeysFor.RECOVERY)
                unregister = owner.registerListener(new LoadListener(this, into));
        }

        public void load(Map<Timestamp, Summary> into, BooleanSupplier abort)
        {
            if (loadKeysFor != LoadKeysFor.RECOVERY)
                return;

            if (abort.getAsBoolean())
                throw new CancellationException();

            List<TxnId> load = new ArrayList<>();
            owner.search(this, null, load::add);

            if (abort.getAsBoolean())
                throw new CancellationException();

            ArrayDeque<TxnId> loadFromDisk = new ArrayDeque<>();
            try (AccordCommandStore.ExclusiveCaches caches = commandStore().lockCaches())
            {
                for (TxnId txnId : load)
                {
                    AccordCacheEntry<TxnId, Command, ?> entry = caches.commands().getUnsafe(txnId);
                    if (entry == null)
                    {
                        loadFromDisk.add(txnId);
                    }
                    else
                    {
                        Summary summary = ifRelevant(entry);
                        if (summary != null)
                            into.putIfAbsent(txnId, summary);
                    }
                }
            }

            for (TxnId txnId = loadFromDisk.poll(); txnId != null; txnId = loadFromDisk.poll())
            {
                if (abort.getAsBoolean())
                    throw new CancellationException();

                Summary summary = loadFromDisk(txnId);
                if (summary != null)
                    into.putIfAbsent(txnId, summary);
            }
        }

        public void finish(Map<Timestamp, Summary> into)
        {
            owner.search(this, into::put, null);
        }

        @Override
        public void cleanupExclusive(AccordCommandStore.Caches caches)
        {
            if (unregister != null)
            {
                unregister.cancel();
                unregister = null;
            }
        }

        @Override
        protected AccordCommandStore commandStore()
        {
            return owner.commandStore;
        }
    }

    private final AccordCommandStore commandStore;

    public InMemoryRangeIndex(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
    }

    public RangeIndex.Loader loader(TxnId primaryTxnId, Timestamp primaryExecuteAt, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges)
    {
        RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
        MaxDecidedRX maxDecidedRX = commandStore.unsafeGetMaxDecidedRX();
        return SummaryLoader.loader(redundantBefore, maxDecidedRX, primaryTxnId, primaryExecuteAt, loadKeysFor, keysOrRanges, this::newLoader);
    }

    @Override
    public void postReplay()
    {
        prune(commandStore);
    }

    private RangeIndex.Loader newLoader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, @Nullable TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, LoadKeysFor loadKeysFor)
    {
        return new Loader(this, redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKind, minTxnId, maxTxnId, loadKeysFor);
    }

    @Override
    public void save(File file) throws IOException
    {
        writeList(file, snapshot(), CommandStoreSerializers.rangeIndexIdEntry);
    }

    @Override
    public List<IdEntry> load(File file) throws IOException
    {
        return readList(file, CommandStoreSerializers.rangeIndexIdEntry);
    }

    @Override
    public void restore(Object loaded)
    {
        restore((List<IdEntry>)loaded);
    }
}
