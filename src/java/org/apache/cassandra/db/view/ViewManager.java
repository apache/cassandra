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

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.MV_ENABLE_COORDINATOR_BATCHLOG;

/**
 * Manages {@link View}'s for a single {@link ColumnFamilyStore}. All of the views for that table are created when this
 * manager is initialized.
 *
 * The main purposes of the manager are to provide a single location for updates to be vetted to see whether they update
 * any views {@link #updatesAffectView(Collection, boolean)}, provide locks to prevent multiple
 * updates from creating incoherent updates in the view {@link #acquireLockFor(int)}, and
 * to affect change on the view.
 *
 * TODO: I think we can get rid of that class. For addition/removal of view by names, we could move it Keyspace. And we
 * not sure it's even worth keeping viewsByName as none of the related operation are performance sensitive so we could
 * find the view by iterating over the CFStore.viewManager directly.
 * For the lock, it could move to Keyspace too, but I don't remmenber why it has to be at the keyspace level and if it
 * can be at the table level, maybe that's where it should be.
 */
public class ViewManager
{
    private static final Logger logger = LoggerFactory.getLogger(ViewManager.class);

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentViewWriters() * 1024);

    private static final boolean enableCoordinatorBatchlog = MV_ENABLE_COORDINATOR_BATCHLOG.getBoolean();

    private final ConcurrentMap<String, View> viewsByName = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableId, TableViews> viewsByBaseTable = new ConcurrentHashMap<>();
    private final Keyspace keyspace;

    public ViewManager(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    public boolean updatesAffectView(Collection<? extends IMutation> mutations, boolean coordinatorBatchlog)
    {
        if (!enableCoordinatorBatchlog && coordinatorBatchlog)
            return false;

        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                assert keyspace.getName().equals(update.metadata().keyspace);

                if (coordinatorBatchlog && keyspace.getReplicationStrategy().getReplicationFactor().allReplicas == 1)
                    continue;

                if (!forTable(update.metadata().id).updatedViews(update).isEmpty())
                    return true;
            }
        }

        return false;
    }

    private Iterable<View> allViews()
    {
        return viewsByName.values();
    }

    public void reload(boolean buildAllViews)
    {
        Views views = keyspace.getMetadata().views;
        Map<String, ViewMetadata> newViewsByName = Maps.newHashMapWithExpectedSize(views.size());
        for (ViewMetadata definition : views)
        {
            newViewsByName.put(definition.name(), definition);
        }

        for (Map.Entry<String, ViewMetadata> entry : newViewsByName.entrySet())
        {
            if (!viewsByName.containsKey(entry.getKey()))
                addView(entry.getValue());
        }

        if (!buildAllViews)
            return;

        // Building views involves updating view build status in the system_distributed
        // keyspace and therefore it requires ring information. This check prevents builds
        // being submitted when Keyspaces are initialized during CassandraDaemon::setup as
        // that happens before StorageService & gossip are initialized. After SS has been
        // init'd we schedule builds for *all* views anyway, so this doesn't have any effect
        // on startup. It does mean however, that builds will not be triggered if gossip is
        // disabled via JMX or nodetool as that sets SS to an uninitialized state.
        if (!StorageService.instance.isInitialized())
        {
            logger.info("Not submitting build tasks for views in keyspace {} as " +
                        "storage service is not initialized", keyspace.getName());
            return;
        }

        for (View view : allViews())
        {
            view.build();
            // We provide the new definition from the base metadata
            view.updateDefinition(newViewsByName.get(view.name));
        }
    }

    public void addView(ViewMetadata definition)
    {
        // Skip if the base table doesn't exist due to schema propagation issues, see CASSANDRA-13737
        if (!keyspace.hasColumnFamilyStore(definition.baseTableId))
        {
            logger.warn("Not adding view {} because the base table {} is unknown",
                        definition.name(),
                        definition.baseTableId);
            return;
        }

        View view = new View(definition, keyspace.getColumnFamilyStore(definition.baseTableId));
        forTable(view.getDefinition().baseTableId).add(view);
        viewsByName.put(definition.name(), view);
    }

    /**
     * Stops the building of the specified view, no-op if it isn't building.
     *
     * @param name the name of the view
     */
    public void dropView(String name)
    {
        View view = viewsByName.remove(name);

        if (view == null)
            return;

        view.stopBuild();
        forTable(view.getDefinition().baseTableId).removeByName(name);
        SystemKeyspace.setViewRemoved(keyspace.getName(), view.name);
        SystemDistributedKeyspace.setViewRemoved(keyspace.getName(), view.name);
    }

    public View getByName(String name)
    {
        return viewsByName.get(name);
    }

    public void buildAllViews()
    {
        for (View view : allViews())
            view.build();
    }

    public TableViews forTable(TableId id)
    {
        TableViews views = viewsByBaseTable.get(id);
        if (views == null)
        {
            views = new TableViews(id);
            TableViews previous = viewsByBaseTable.putIfAbsent(id, views);
            if (previous != null)
                views = previous;
        }
        return views;
    }

    public static Lock acquireLockFor(int keyAndCfidHash)
    {
        Lock lock = LOCKS.get(keyAndCfidHash);

        if (lock.tryLock())
            return lock;

        return null;
    }
}
