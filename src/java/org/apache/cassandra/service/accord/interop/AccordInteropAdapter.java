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

package org.apache.cassandra.service.accord.interop;

import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.api.Update;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.CoordinationAdapter.Adapters.TxnAdapter;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecutePath;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.Topologies.SelectNodeOwnership;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.accord.AccordEndpointMapper;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnRead;

import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.Kind.Minimal;

public class AccordInteropAdapter extends TxnAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteropAdapter.class);
    public static final class AccordInteropFactory extends DefaultFactory
    {
        final AccordInteropAdapter standard, recovery;

        public AccordInteropFactory(AccordEndpointMapper endpointMapper)
        {
            standard = new AccordInteropAdapter(endpointMapper, Minimal);
            recovery = new AccordInteropAdapter(endpointMapper, Maximal);
        }

        @Override
        public <R> CoordinationAdapter<R> get(TxnId txnId, Kind step)
        {
            if (txnId.isSyncPoint())
                return super.get(txnId, step);
            return (CoordinationAdapter<R>) (step == Kind.Recovery ? recovery : standard);
        }
    };

    private final AccordEndpointMapper endpointMapper;
    private final Apply.Kind applyKind;

    private AccordInteropAdapter(AccordEndpointMapper endpointMapper, Apply.Kind applyKind)
    {
        super(Minimal);
        this.endpointMapper = endpointMapper;
        this.applyKind = applyKind;
    }

    @Override
    public void execute(Node node, SequentialAsyncExecutor executor, Topologies any, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super Result, Throwable> callback)
    {
        if (!doInteropExecute(node, executor, route, ballot, txnId, txn, executeAt, stableDeps, callback))
            super.execute(node, executor, any, route, ballot, path, flags, txnId, txn, executeAt, stableDeps, sendDeps, callback);
    }

    @Override
    public void persist(Node node, SequentialAsyncExecutor executor, Topologies any, Route<?> require, Route<?> sendTo, SelectNodeOwnership selectSendTo, FullRoute<?> route, Ballot ballot, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, boolean informDurableOnDone, BiConsumer<? super Result, Throwable> callback)
    {
        if (applyKind == Minimal && doInteropPersist(node, executor, any, require, sendTo, selectSendTo, ballot, txnId, txn, executeAt, deps, writes, result, route, informDurableOnDone, callback))
            return;

        super.persist(node, executor, any, require, sendTo, selectSendTo, route, ballot, flags, txnId, txn, executeAt, deps, writes, result, informDurableOnDone, callback);
    }

    private boolean doInteropExecute(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        // Unrecoverable repair always needs to be run by AccordInteropExecution
        AccordUpdate.Kind updateKind = AccordUpdate.kind(txn.update());
        ConsistencyLevel consistencyLevel = txn.read() instanceof TxnRead ? ((TxnRead) txn.read()).cassandraConsistencyLevel() : null;
        if (updateKind != AccordUpdate.Kind.UNRECOVERABLE_REPAIR && (consistencyLevel == null || consistencyLevel == ConsistencyLevel.ONE || txn.read().keys().isEmpty()))
            return false;

        new AccordInteropExecution(node, txnId, txn, updateKind, route, ballot, executeAt, deps, callback, executor, consistencyLevel, endpointMapper)
            .start();
        return true;
    }

    private boolean doInteropPersist(Node node, SequentialAsyncExecutor executor, Topologies any, Route<?> require, Route<?> sendTo, SelectNodeOwnership selectSendTo, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, FullRoute<?> fullRoute, boolean informDurableOnDone, BiConsumer<? super Result, Throwable> callback)
    {
        Update update = txn.update();
        ConsistencyLevel consistencyLevel = update instanceof AccordUpdate ? ((AccordUpdate) update).cassandraCommitCL() : null;
        if (consistencyLevel == null || consistencyLevel == ConsistencyLevel.ANY || writes.isEmpty())
            return false;

        Topologies all = execution(node, any, sendTo, selectSendTo, fullRoute, txnId, executeAt);
        new AccordInteropPersist(node, executor, all, txnId, require, ballot, txn, executeAt, deps, writes, result, fullRoute, consistencyLevel, CoordinationFlags.none(), informDurableOnDone, callback)
            .start(Minimal, any, writes, result);
        return true;
    }
}
