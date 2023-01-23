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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.serializers.CommitSerializers.CommitSerializer;

public class AccordInteropCommit extends Commit
{
    public static final IVersionedSerializer<AccordInteropCommit> serializer = new CommitSerializer<AccordInteropCommit, AccordInteropRead>(AccordInteropRead.class, AccordInteropRead.requestSerializer)
    {
        @Override
        protected AccordInteropCommit deserializeCommit(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Kind kind, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData read)
        {
            return new AccordInteropCommit(kind, txnId, scope, waitForEpoch, executeAt, partialTxn, partialDeps, fullRoute, read);
        }
    };

    public AccordInteropCommit(Kind kind, TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nonnull ReadData readData)
    {
        super(kind, txnId, scope, waitForEpoch, executeAt, partialTxn, partialDeps, fullRoute, readData);
    }

    public AccordInteropCommit(Kind kind, Node.Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, AccordInteropRead read)
    {
        super(kind, to, coordinateTopology, topologies, txnId, txn, route, executeAt, deps, (t, u, p) -> read);
    }

    @Override
    public MessageType type()
    {
        switch (kind)
        {
            case Minimal: return AccordMessageType.INTEROP_COMMIT_MINIMAL_REQ;
            case Maximal: return AccordMessageType.INTEROP_COMMIT_MAXIMAL_REQ;
            default: throw new IllegalStateException();
        }
    }
}
