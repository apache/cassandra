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

package org.apache.cassandra.service.accord.repair;

import java.util.Collection;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableSet;

import accord.api.Result;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.ExecutePath;
import accord.coordinate.ExecuteSyncPoint;
import accord.local.Node;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;

/**
 * Sync point adapter used for accord-only repairs.
 *
 * Repair has the requirement that all client writes begun before the repair will be fully replicated once repair
 * has completed. In the case of accord, repairs that compare data on disk satisfy this requirement by running
 * a sync point as part of streaming if differences are found. For accord-only repairs, the barrier used by normal
 * repairs is not sufficient since it only requires a quorum of nodes to respond before completing. This sync point
 * adapter requires responses from all of the supplied endpoints before completing. Note that shards only block on the
 * intersection of the provided replicas and their own endpoints.
 */
public class RepairSyncPointAdapter<S extends Seekables<?, ?>> extends CoordinationAdapter.Adapters.AbstractSyncPointAdapter<S>
{
    private final ImmutableSet<Node.Id> requiredResponses;

    public RepairSyncPointAdapter(Collection<Node.Id> requiredResponses)
    {
        this.requiredResponses = ImmutableSet.copyOf(requiredResponses);
    }

    @Override
    public void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
    {
        RequiredResponseTracker tracker = new RequiredResponseTracker(requiredResponses, all);
        ExecuteSyncPoint.ExecuteBlocking<S> execute = new ExecuteSyncPoint.ExecuteBlocking<>(node, new SyncPoint<>(txnId, deps, (S) txn.keys(), route), tracker, executeAt);
        execute.addCallback(callback);
        execute.start();
    }

    @Override
    public void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super SyncPoint<S>, Throwable> callback)
    {
        throw new UnsupportedOperationException();
    }

    public static <S extends Seekables<?, ?>> CoordinationAdapter<SyncPoint<S>> create(Collection<Node.Id> requiredResponses)
    {
        return new RepairSyncPointAdapter<>(requiredResponses);
    }
}
