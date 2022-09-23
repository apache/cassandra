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

import accord.local.Listener;
import accord.local.PartialCommand;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;

public class AccordPartialCommand implements PartialCommand
{
    interface RemovedListener {}
    private static class RemovedCommand implements RemovedListener
    {
        private final TxnId txnId;

        public RemovedCommand(TxnId txnId)
        {
            this.txnId = txnId;
        }
    }

    private static class RemovedCommandsForKey implements RemovedListener
    {
        private final PartitionKey key;

        public RemovedCommandsForKey(PartitionKey key)
        {
            this.key = key;
        }
    }

    private final TxnId txnId;
    private final Txn txn;
    private final Timestamp executeAt;
    private final Status status;
    private List<RemovedListener> removedListeners = null;

    public AccordPartialCommand(TxnId txnId, Txn txn, Timestamp executeAt, Status status)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.status = status;
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public Txn txn()
    {
        return txn;
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    @Override
    public Status status()
    {
        return status;
    }

    @Override
    public void removeListener(Listener listener)
    {
        if (listener.isTransient())
            return;

        if (removedListeners == null)
            removedListeners = new ArrayList<>();

        if (listener instanceof AccordCommand)
        {
            AccordCommand command = (AccordCommand) listener;
            removedListeners.add(new RemovedCommand(command.txnId()));
        }
        else if (listener instanceof AccordCommandsForKey)
        {
            AccordCommandsForKey cfk = (AccordCommandsForKey) listener;
            removedListeners.add(new RemovedCommandsForKey(cfk.key()));
        }
        else
        {
            throw new IllegalArgumentException("Unhandled listener type: " + listener.getClass());
        }
    }

    public static class WithDeps extends AccordPartialCommand implements PartialCommand.WithDeps
    {
        private final Deps deps;

        public WithDeps(TxnId txnId, Txn txn, Timestamp executeAt, Status status, Deps deps)
        {
            super(txnId, txn, executeAt, status);
            this.deps = deps;
        }

        @Override
        public Deps savedDeps()
        {
            return deps;
        }
    }
}