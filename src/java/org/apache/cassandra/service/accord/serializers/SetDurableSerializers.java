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
package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.messages.SetGloballyDurable;
import accord.messages.SetShardDurable;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;

public class SetDurableSerializers
{
    public static final UnversionedSerializer<SetShardDurable> shardDurable = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(SetShardDurable msg, DataOutputPlus out) throws IOException
        {
            syncPoint.serialize(msg.exclusiveSyncPoint, out);
            CommandSerializers.outcomeDurability.serialize(msg.durability, out);
        }

        @Override
        public SetShardDurable deserialize(DataInputPlus in) throws IOException
        {
            return new SetShardDurable(syncPoint.deserialize(in),
                                       CommandSerializers.outcomeDurability.deserialize(in));
        }

        @Override
        public long serializedSize(SetShardDurable msg)
        {
            return syncPoint.serializedSize(msg.exclusiveSyncPoint)
                + CommandSerializers.outcomeDurability.serializedSize(msg.durability);
        }
    };

    public static final UnversionedSerializer<SetGloballyDurable> globallyDurable = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(SetGloballyDurable msg, DataOutputPlus out) throws IOException
        {
            CommandStoreSerializers.durableBefore.serialize(msg.durableBefore, out);
        }

        @Override
        public SetGloballyDurable deserialize(DataInputPlus in) throws IOException
        {
            return new SetGloballyDurable(CommandStoreSerializers.durableBefore.deserialize(in));
        }

        @Override
        public long serializedSize(SetGloballyDurable msg)
        {
            return CommandStoreSerializers.durableBefore.serializedSize(msg.durableBefore);
        }
    };

    public static final UnversionedSerializer<SyncPoint> syncPoint = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(SyncPoint sp, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(sp.syncId, out);
            ExecuteAtSerializer.serialize(sp.syncId, sp.executeAt, out);
            DepsSerializers.deps.serialize(sp.waitFor, out);
            KeySerializers.fullRoute.serialize(sp.route, out);
        }

        @Override
        public SyncPoint deserialize(DataInputPlus in) throws IOException
        {
            TxnId syncId = CommandSerializers.txnId.deserialize(in);
            Timestamp executeAt = ExecuteAtSerializer.deserialize(syncId, in);
            Deps waitFor = DepsSerializers.deps.deserialize(in);
            FullRoute<?> route = KeySerializers.fullRoute.deserialize(in);
            return SyncPoint.SerializationSupport.construct(syncId, executeAt, waitFor, route);
        }

        @Override
        public long serializedSize(SyncPoint sp)
        {
            return   CommandSerializers.txnId.serializedSize(sp.syncId)
                   + ExecuteAtSerializer.serializedSize(sp.syncId, sp.executeAt)
                   + DepsSerializers.deps.serializedSize(sp.waitFor)
                   + KeySerializers.fullRoute.serializedSize(sp.route);
        }
    };
}
