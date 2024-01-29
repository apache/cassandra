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

import accord.api.Result;
import accord.api.Update;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.CoordinationAdapter.Adapters.AbstractTxnAdapter;
import accord.coordinate.ExecutePath;
import accord.coordinate.PersistTxn;
import accord.local.Node;
import accord.messages.Apply;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.accord.AccordEndpointMapper;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.interop.AccordInteropExecution.InteropExecutor;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnRead;

import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.Kind.Minimal;

public class AccordInteropAdapter extends AbstractTxnAdapter
{
    public static final class AccordInteropFactory implements CoordinationAdapter.Factory
    {
        final AccordInteropAdapter standard, recovery;

        public AccordInteropFactory(AccordAgent agent, AccordEndpointMapper endpointMapper)
        {
            final InteropExecutor executor = new InteropExecutor(agent);
            standard = new AccordInteropAdapter(executor, endpointMapper, Minimal);
            recovery = new AccordInteropAdapter(executor, endpointMapper, Maximal);
        }

        @Override
        public <R> CoordinationAdapter<R> get(TxnId txnId, Step step)
        {
            return (CoordinationAdapter<R>) (step == Step.InitiateRecovery ? recovery : standard);
        }
    };

    private final InteropExecutor executor;
    private final AccordEndpointMapper endpointMapper;
    private final Apply.Kind applyKind;

    private AccordInteropAdapter(InteropExecutor executor, AccordEndpointMapper endpointMapper, Apply.Kind applyKind)
    {
        this.executor = executor;
        this.endpointMapper = endpointMapper;
        this.applyKind = applyKind;
    }

    @Override
    public void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        if (!doInteropExecute(node, route, txnId, txn, executeAt, deps, callback))
            super.execute(node, all, route, path, txnId, txn, executeAt, deps, callback);
    }

    @Override
    public void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
    {
        if (applyKind == Minimal && doInteropPersist(node, all, route, txnId, txn, executeAt, deps, writes, result, callback))
            return;

        if (callback != null) callback.accept(result, null);
        new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
            .start(Apply.FACTORY, applyKind, all, writes, result);
    }

    private boolean doInteropExecute(Node node, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        // Unrecoverable repair always needs to be run by AccordInteropExecution
        AccordUpdate.Kind updateKind = AccordUpdate.kind(txn.update());
        ConsistencyLevel consistencyLevel = txn.read() instanceof TxnRead ? ((TxnRead) txn.read()).cassandraConsistencyLevel() : null;
        if (updateKind != AccordUpdate.Kind.UNRECOVERABLE_REPAIR && (consistencyLevel == null || consistencyLevel == ConsistencyLevel.ONE || txn.read().keys().isEmpty()))
            return false;

        new AccordInteropExecution(node, txnId, txn, updateKind, route, txn.read().keys().toParticipants(), executeAt, deps, callback, executor, consistencyLevel, endpointMapper)
            .start();
        return true;
    }

    private static boolean doInteropPersist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
    {
        Update update = txn.update();
        ConsistencyLevel consistencyLevel = update instanceof AccordUpdate ? ((AccordUpdate) update).cassandraCommitCL() : null;
        if (consistencyLevel == null || consistencyLevel == ConsistencyLevel.ANY || writes.isEmpty())
            return false;

        new AccordInteropPersist(node, all, txnId, route, txn, executeAt, deps, writes, result, consistencyLevel, callback)
            .start(AccordInteropApply.FACTORY, Minimal, all, writes, result);
        return true;
    }
}
