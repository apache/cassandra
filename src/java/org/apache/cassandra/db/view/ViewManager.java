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
package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages {@link View}'s for a single {@link ColumnFamilyStore}. All of the views for that table are created when this
 * manager is initialized.
 *
 * The main purposes of the manager are to provide a single location for updates to be vetted to see whether they update
 * any views {@link ViewManager#updatesAffectView(Collection, boolean)}, provide locks to prevent multiple
 * updates from creating incoherent updates in the view {@link ViewManager#acquireLockFor(ByteBuffer)}, and
 * to affect change on the view.
 */
public class ViewManager
{
    private static final Logger logger = LoggerFactory.getLogger(ViewManager.class);

    public class ForStore
    {
        private final ConcurrentNavigableMap<String, View> viewsByName;

        public ForStore()
        {
            this.viewsByName = new ConcurrentSkipListMap<>();
        }

        public Iterable<View> allViews()
        {
            return viewsByName.values();
        }

        public Iterable<ColumnFamilyStore> allViewsCfs()
        {
            List<ColumnFamilyStore> viewColumnFamilies = new ArrayList<>();
            for (View view : allViews())
                viewColumnFamilies.add(keyspace.getColumnFamilyStore(view.getDefinition().viewName));
            return viewColumnFamilies;
        }

        public void forceBlockingFlush()
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
                viewCfs.forceBlockingFlush();
        }

        public void dumpMemtables()
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
                viewCfs.dumpMemtable();
        }

        public void truncateBlocking(long truncatedAt)
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
            {
                ReplayPosition replayAfter = viewCfs.discardSSTables(truncatedAt);
                SystemKeyspace.saveTruncationRecord(viewCfs, truncatedAt, replayAfter);
            }
        }

        public void addView(View view)
        {
            viewsByName.put(view.name, view);
        }

        public void removeView(String name)
        {
            viewsByName.remove(name);
        }
    }

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentViewWriters() * 1024);

    private static final boolean enableCoordinatorBatchlog = Boolean.getBoolean("cassandra.mv_enable_coordinator_batchlog");

    private final ConcurrentNavigableMap<UUID, ForStore> viewManagersByStore;
    private final ConcurrentNavigableMap<String, View> viewsByName;
    private final Keyspace keyspace;

    public ViewManager(Keyspace keyspace)
    {
        this.viewManagersByStore = new ConcurrentSkipListMap<>();
        this.viewsByName = new ConcurrentSkipListMap<>();
        this.keyspace = keyspace;
    }

    /**
     * Calculates and pushes updates to the views replicas. The replicas are determined by
     * {@link ViewUtils#getViewNaturalEndpoint(String, Token, Token)}.
     */
    public void pushViewReplicaUpdates(PartitionUpdate update, boolean writeCommitLog, AtomicLong baseComplete)
    {
        List<Mutation> mutations = null;
        TemporalRow.Set temporalRows = null;
        for (Map.Entry<String, View> view : viewsByName.entrySet())
        {
            // Make sure that we only get mutations from views which are affected since the set includes all views for a
            // keyspace. This will prevent calling getTemporalRowSet for the wrong base table.
            if (view.getValue().updateAffectsView(update))
            {
                temporalRows = view.getValue().getTemporalRowSet(update, temporalRows, false);

                Collection<Mutation> viewMutations = view.getValue().createMutations(update, temporalRows, false);
                if (viewMutations != null && !viewMutations.isEmpty())
                {
                    if (mutations == null)
                        mutations = Lists.newLinkedList();
                    mutations.addAll(viewMutations);
                }
            }
        }

        if (mutations != null)
            StorageProxy.mutateMV(update.partitionKey().getKey(), mutations, writeCommitLog, baseComplete);
    }

    public boolean updatesAffectView(Collection<? extends IMutation> mutations, boolean coordinatorBatchlog)
    {
        if (coordinatorBatchlog && !enableCoordinatorBatchlog)
            return false;

        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate cf : mutation.getPartitionUpdates())
            {
                assert keyspace.getName().equals(cf.metadata().ksName);

                if (coordinatorBatchlog && keyspace.getReplicationStrategy().getReplicationFactor() == 1)
                    continue;

                for (View view : allViews())
                {
                    if (view.updateAffectsView(cf))
                        return true;
                }
            }
        }

        return false;
    }

    public Iterable<View> allViews()
    {
        return viewsByName.values();
    }

    public void update(String viewName)
    {
        View view = viewsByName.get(viewName);
        assert view != null : "When updating a view, it should already be in the ViewManager";
        view.build();

        // We provide the new definition from the base metadata
        Optional<ViewDefinition> viewDefinition = keyspace.getMetadata().views.get(viewName);
        assert viewDefinition.isPresent() : "When updating a view, it should still be in the Keyspaces views";
        view.updateDefinition(viewDefinition.get());
    }

    public void reload()
    {
        Map<String, ViewDefinition> newViewsByName = new HashMap<>();
        for (ViewDefinition definition : keyspace.getMetadata().views)
        {
            newViewsByName.put(definition.viewName, definition);
        }

        for (String viewName : viewsByName.keySet())
        {
            if (!newViewsByName.containsKey(viewName))
                removeView(viewName);
        }

        for (Map.Entry<String, ViewDefinition> entry : newViewsByName.entrySet())
        {
            if (!viewsByName.containsKey(entry.getKey()))
                addView(entry.getValue());
        }

        for (View view : allViews())
        {
            view.build();
            // We provide the new definition from the base metadata
            view.updateDefinition(newViewsByName.get(view.name));
        }
    }

    public void addView(ViewDefinition definition)
    {
        View view = new View(definition, keyspace.getColumnFamilyStore(definition.baseTableId));
        forTable(view.getDefinition().baseTableId).addView(view);
        viewsByName.put(definition.viewName, view);
    }

    public void removeView(String name)
    {
        View view = viewsByName.remove(name);

        if (view == null)
            return;

        forTable(view.getDefinition().baseTableId).removeView(name);
        SystemKeyspace.setViewRemoved(keyspace.getName(), view.name);
        SystemDistributedKeyspace.setViewRemoved(keyspace.getName(), view.name);
    }

    public void buildAllViews()
    {
        for (View view : allViews())
            view.build();
    }

    public ForStore forTable(UUID baseId)
    {
        ForStore forStore = viewManagersByStore.get(baseId);
        if (forStore == null)
        {
            forStore = new ForStore();
            ForStore previous = viewManagersByStore.put(baseId, forStore);
            if (previous != null)
                forStore = previous;
        }
        return forStore;
    }

    public static Lock acquireLockFor(int keyAndCfidHash)
    {
        Lock lock = LOCKS.get(keyAndCfidHash);

        if (lock.tryLock())
            return lock;

        return null;
    }
}
