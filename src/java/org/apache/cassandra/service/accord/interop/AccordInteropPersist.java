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
import accord.coordinate.ExecuteFlag;
import accord.coordinate.Persist;
import accord.coordinate.tracking.AllTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.coordinate.tracking.ResponseTracker;
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
import accord.utils.Invariants;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.Throwables;

/**
 * Similar to Accord persist, but can wait on a configurable number of responses and sends AccordInteropApply messages
 * that only return a response when the Apply has actually occurred. Regular Apply messages only get the transaction
 * to PreApplied.
 */
public class AccordInteropPersist extends Persist
{
    private static class CallbackHolder
    {
        boolean isDone = false;
        private final ResponseTracker tracker;
        private final Result result;
        private final BiConsumer<? super Result, Throwable> clientCallback;
        private Throwable failure = null;

        public CallbackHolder(ResponseTracker tracker, Result result, BiConsumer<? super Result, Throwable> clientCallback)
        {
            this.tracker = tracker;
            this.result = result;
            this.clientCallback = clientCallback;
        }

        private void handleStatus(RequestStatus status)
        {
            if (isDone)
                return;

            switch (status)
            {
                default: throw new IllegalStateException("Unhandled request status " + status);
                case Success:
                    isDone = true;
                    clientCallback.accept(result, null);
                    return;
                case Failed:
                    isDone = true;
                    clientCallback.accept(null, failure);
                    return;
                case NoChange:
                    // noop
            }
        }

        public void recordSuccess(Node.Id node)
        {
            handleStatus(tracker.recordSuccess(node));
        }

        public void recordFailure(Node.Id node, Throwable throwable)
        {
            failure = Throwables.merge(failure, throwable);
            handleStatus(tracker.recordFailure(node));
        }

        boolean recordCallbackFailure(Throwable throwable)
        {
            if (isDone)
                return false;
            isDone = true;
            failure = Throwables.merge(failure, throwable);
            clientCallback.accept(null, failure);
            return true;
        }
    }

    private final ConsistencyLevel consistencyLevel;
    private CallbackHolder callback;

    public AccordInteropPersist(Node node, SequentialAsyncExecutor executor, Topologies topologies, TxnId txnId, Route<?> sendTo, Ballot ballot, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, FullRoute<?> fullRoute, ConsistencyLevel consistencyLevel, ExecuteFlag.CoordinationFlags flags, boolean informDurableOnDone, BiConsumer<? super Result, Throwable> clientCallback)
    {
        super(node, executor, topologies, txnId, ballot, sendTo, txn, executeAt, deps, writes, result, fullRoute, flags, informDurableOnDone, AccordInteropApply.FACTORY);
        Invariants.requireArgument(consistencyLevel == ConsistencyLevel.QUORUM || consistencyLevel == ConsistencyLevel.ALL || consistencyLevel == ConsistencyLevel.SERIAL || consistencyLevel == ConsistencyLevel.ONE);
        this.consistencyLevel = consistencyLevel;
        registerClientCallback(result, clientCallback);
    }

    public void registerClientCallback(Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        Invariants.require(callback == null);
        switch (consistencyLevel)
        {
            case ONE: // Can safely upgrade ONE to QUORUM/SERIAL to get a synchronous commit
            case SERIAL:
            case QUORUM:
                callback = new CallbackHolder(new QuorumTracker(topologies), result, clientCallback);
                break;
            case ALL:
                callback = new CallbackHolder(new AllTracker(topologies), result, clientCallback);
                break;
            default:
                throw new IllegalArgumentException("Unhandled consistency level: " + consistencyLevel);
        }
    }

    @Override
    public void onSuccess(Node.Id from, Apply.ApplyReply reply)
    {
        super.onSuccess(from, reply);
        switch (reply)
        {
            case Redundant:
            case Applied:
                callback.recordSuccess(from);
                return;
            case Insufficient:
                // On insufficient Persist will send a commit with the missing information
                // which will allow a final response to be returned later that could be successful
                return;
            default: throw new IllegalArgumentException("Unhandled apply response " + reply);
        }
    }

    @Override
    public void onFailure(Node.Id from, Throwable failure)
    {
        callback.recordFailure(from, failure);
    }

    @Override
    public boolean onCallbackFailure(Node.Id from, Throwable failure)
    {
        return callback.recordCallbackFailure(failure);
    }
}
