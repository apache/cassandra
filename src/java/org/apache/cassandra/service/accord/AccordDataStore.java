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

import java.util.ArrayList;
import java.util.List;

import accord.api.DataStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.Future;

public class AccordDataStore implements DataStore
{
    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        AccordFetchCoordinator coordinator = new AccordFetchCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore());
        coordinator.start();
        return coordinator.result();
    }

    @Override
    public void snapshot()
    {
        // TODO: maintain a list of Accord tables, perhaps in ClusterMetadata?
        ClusterMetadata metadata = ClusterMetadata.current();
        List<Future<?>> futures = new ArrayList<>();
        for (KeyspaceMetadata ks : metadata.schema.getKeyspaces())
        {
            for (TableMetadata table : ks.tables)
            {
                if (table.isAccordEnabled())
                    futures.add(Keyspace.open(ks.name).getColumnFamilyStore(table.id).forceFlush(ColumnFamilyStore.FlushReason.ACCORD));
            }
        }
        try
        {
            for (Future<?> future : futures)
                future.get();
        }
        catch (Throwable t)
        {
            throw new IllegalStateException("Could not snapshot table state.", t);
        }
    }
}
