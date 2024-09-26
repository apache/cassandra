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
import accord.primitives.TxnId;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

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
    public AsyncResult<Void> snapshot(Ranges ranges, TxnId before) // TODO: does this have to go to journal, too?
    {
        AsyncResults.SettableResult<Void> result = new AsyncResults.SettableResult<>();
        // TODO: maintain a list of Accord tables, perhaps in ClusterMetadata?
        ClusterMetadata metadata = ClusterMetadata.current();
        List<Future<?>> futures = new ArrayList<>();
        for (KeyspaceMetadata ks : metadata.schema.getKeyspaces())
        {
            // TODO: only flush intersecting ranges
            for (TableMetadata table : ks.tables)
            {
                if (table.isAccordEnabled())
                    futures.add(Keyspace.open(ks.name).getColumnFamilyStore(table.id).forceFlush(ColumnFamilyStore.FlushReason.ACCORD));
            }
        }

        FutureCombiner.allOf(futures).addCallback((objects, throwable) -> {
            if (throwable != null)
                result.setFailure(throwable);
            else
                result.setSuccess(null);
        });

        return result;
    }
}
