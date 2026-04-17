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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import accord.api.RoutingKey;
import accord.local.CommandStore;
import accord.local.LoadKeys;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import static accord.api.Journal.FieldUpdates;
import static accord.local.CommandStores.RangesForEpoch;

public abstract class AbstractSafeCommandStore<C extends SafeCommand,
                                              CFK extends SafeCommandsForKey,
                                              Caches extends AbstractSafeCommandStore.CommandStoreCaches<C, CFK>>
extends SafeCommandStore
{
    protected final PreLoadContext context;

    private final CommandStore commandStore;
    private FieldUpdates fieldUpdates;

    protected AbstractSafeCommandStore(PreLoadContext context, CommandStore commandStore)
    {
        this.context = context;
        this.commandStore = commandStore;
    }

    @Override
    public CommandStore commandStore()
    {
        return commandStore;
    }

    public interface CommandStoreCaches<C, CFK> extends AutoCloseable
    {
        void close();

        C acquireIfLoaded(TxnId txnId);
        CFK acquireIfLoaded(RoutingKey key);
    }

    protected abstract Caches tryGetCaches();
    protected abstract C add(C safeCommand, Caches caches);
    protected abstract CFK add(CFK safeCfk, Caches caches);

    @Override
    public PreLoadContext canExecute(PreLoadContext with)
    {
        if (with.isEmpty()) return with;
        if (with.keys().domain() == Routable.Domain.Range)
            return with.isSubsetOf(this.context) ? with : null;

        LoadKeys require = with.loadKeys();
        if (require != LoadKeys.NONE)
        {
            PreLoadContext context = context();
            if (!context.loadKeys().satisfiesIfPresent(require))
                return null;

            if (with.loadKeysFor().compareTo(context.loadKeysFor()) > 0)
                return null;
        }

        try (Caches caches = tryGetCaches())
        {
            for (TxnId txnId : with.txnIds())
            {
                if (null != getInternal(txnId))
                    continue;

                if (caches == null)
                    return null;

                C safeCommand = caches.acquireIfLoaded(txnId);
                if (safeCommand == null)
                    return null;

                add(safeCommand, caches);
            }

            LoadKeys loadKeys = with.loadKeys();
            if (loadKeys == LoadKeys.NONE)
                return with;

            List<RoutingKey> unavailable = null;
            Unseekables<?> keys = with.keys();
            if (keys.isEmpty())
                return with;

            for (int i = 0 ; i < keys.size() ; ++i)
            {
                RoutingKey key = (RoutingKey) keys.get(i);
                if (null != getInternal(key))
                    continue; // already in working set

                if (caches != null)
                {
                    CFK safeCfk = caches.acquireIfLoaded(key);
                    if (safeCfk != null)
                    {
                        add(safeCfk, caches);
                        continue;
                    }
                }
                if (unavailable == null)
                    unavailable = new ArrayList<>();
                unavailable.add(key);
            }

            if (unavailable == null)
                return with;

            if (unavailable.size() == keys.size())
                return null;

            return PreLoadContext.contextFor(with.primaryTxnId(), with.additionalTxnId(), keys.without(RoutingKeys.ofSortedUnique(unavailable)), loadKeys, context.loadKeysFor(), context.reason());
        }
    }

    @Override
    public PreLoadContext context()
    {
        return context;
    }

    @Override
    protected C ifLoadedInternal(TxnId txnId)
    {
        try (Caches caches = tryGetCaches())
        {
            if (caches == null)
                return null;

            C command = caches.acquireIfLoaded(txnId);
            if (command == null)
                return null;

            return add(command, caches);
        }
    }

    @Override
    protected CFK ifLoadedInternal(RoutingKey txnId)
    {
        try (Caches caches = tryGetCaches())
        {
            if (caches == null)
                return null;

            CFK cfk = caches.acquireIfLoaded(txnId);
            if (cfk == null)
                return null;

            return add(cfk, caches);
        }
    }

    // TODO (expected): cleanup the integration hooks here; they're a bit byzantine. Also clearly document behaviour.
    public void postExecute()
    {
        flushFieldUpdates();
    }

    protected void persistFieldUpdates()
    {
        flushFieldUpdates();
    }

    protected void flushFieldUpdates()
    {
        if (fieldUpdates == null)
            return;

        if (fieldUpdates.newRedundantBefore != null)
            super.unsafeSetRedundantBefore(fieldUpdates.newRedundantBefore);

        if (fieldUpdates.newBootstrapBeganAt != null)
            super.setBootstrapBeganAt(fieldUpdates.newBootstrapBeganAt);

        if (fieldUpdates.newSafeToRead != null)
            super.setSafeToRead(fieldUpdates.newSafeToRead);

        if (fieldUpdates.newRangesForEpoch != null)
            super.setRangesForEpoch(fieldUpdates.newRangesForEpoch);

        fieldUpdates = null;
    }

    /**
     * Persistent field update logic
     */

    @Override
    public final void upsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        // TODO (required): this is potentially unsafe: if the update is not persisted for some reason (due to some later exception)
        //   we can continue with a stale redundantBefore
        // TODO (expected): fix RedundantBefore sorting issue and switch to upsert mode
        ensureFieldUpdates().newRedundantBefore = RedundantBefore.merge(redundantBefore(), addRedundantBefore);
        unsafeUpsertRedundantBefore(addRedundantBefore);
    }

    @Override
    public final void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        ensureFieldUpdates().newBootstrapBeganAt = newBootstrapBeganAt;
    }

    @Override
    public final void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        ensureFieldUpdates().newSafeToRead = newSafeToRead;
    }

    @Override
    public final void setPermanentlyUnsafeToRead(Ranges newPermanentlyUnsafeToRead)
    {
        ensureFieldUpdates().newPermanentlyUnsafeToRead = newPermanentlyUnsafeToRead;
    }

    @Override
    public void setRangesForEpoch(RangesForEpoch rangesForEpoch)
    {
        if (rangesForEpoch != null)
        {
            super.setRangesForEpoch(rangesForEpoch);
            ensureFieldUpdates().newRangesForEpoch = rangesForEpoch;
        }
    }

    @Override
    public RangesForEpoch ranges()
    {
        if (fieldUpdates != null && fieldUpdates.newRangesForEpoch != null)
            return fieldUpdates.newRangesForEpoch;

        return commandStore.unsafeGetRangesForEpoch();
    }

    @Override
    public NavigableMap<TxnId, Ranges> bootstrapBeganAt()
    {
        if (fieldUpdates != null && fieldUpdates.newBootstrapBeganAt != null)
            return fieldUpdates.newBootstrapBeganAt;

        return super.bootstrapBeganAt();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> safeToReadAt()
    {
        if (fieldUpdates != null && fieldUpdates.newSafeToRead != null)
            return fieldUpdates.newSafeToRead;

        return super.safeToReadAt();
    }

    @Override
    public RedundantBefore redundantBefore()
    {
        if (fieldUpdates != null && fieldUpdates.newRedundantBefore != null)
            return fieldUpdates.newRedundantBefore;

        return super.redundantBefore();
    }

    private FieldUpdates ensureFieldUpdates()
    {
        if (fieldUpdates == null) fieldUpdates = new FieldUpdates();
        return fieldUpdates;
    }

    public FieldUpdates fieldUpdates()
    {
        return fieldUpdates;
    }
}
