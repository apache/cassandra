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

import accord.api.ExclusiveAsyncExecutor;
import accord.api.Result;
import accord.coordinate.ExecuteFlag;
import accord.coordinate.Persist;
import accord.coordinate.tracking.AllTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
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
import accord.utils.Invariants;

import org.apache.cassandra.db.ConsistencyLevel;

import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

/**
 * Similar to Accord persist, but can wait on a configurable number of responses and sends AccordInteropApply messages
 * that only return a response when the Apply has actually occurred. Regular Apply messages only get the transaction
 * to PreApplied.
 */
public class AccordInteropPersist extends Persist
{
    public AccordInteropPersist(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, TxnId txnId, Route<?> sendTo, Ballot ballot, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, FullRoute<?> fullRoute, ConsistencyLevel consistencyLevel, ExecuteFlag.CoordinationFlags flags, boolean informDurableOnDone, Apply.Kind applyKind, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, topologies, txnId, ballot, sendTo, txn, executeAt, deps, writes, result.toPersistable(), fullRoute, flags, informDurableOnDone, AccordInteropApply.FACTORY, applyKind, consistencyLevel == ALL ? AllTracker::new : QuorumTracker::new, (ignore, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else callback.accept(result, null);
        });
        Invariants.requireArgument(consistencyLevel == QUORUM || consistencyLevel == ALL || consistencyLevel == SERIAL || consistencyLevel == ONE);
    }

    @Override
    public void start()
    {
        super.start();
    }
}
