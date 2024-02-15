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

import accord.api.RoutingKey;
import accord.messages.SetGloballyDurable;
import accord.messages.SetShardDurable;
import accord.primitives.Deps;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SetDurableSerializers
{
    public static final IVersionedSerializer<SetShardDurable> shardDurable = new IVersionedSerializer<SetShardDurable>()
    {
        @Override
        public void serialize(SetShardDurable msg, DataOutputPlus out, int version) throws IOException
        {
            syncPoint.serialize(msg.exclusiveSyncPoint, out, version);
        }

        @Override
        public SetShardDurable deserialize(DataInputPlus in, int version) throws IOException
        {
            return new SetShardDurable(syncPoint.deserialize(in, version));
        }

        @Override
        public long serializedSize(SetShardDurable msg, int version)
        {
            return syncPoint.serializedSize(msg.exclusiveSyncPoint, version);
        }
    };

    public static final IVersionedSerializer<SetGloballyDurable> globallyDurable = new IVersionedSerializer<SetGloballyDurable>()
    {
        @Override
        public void serialize(SetGloballyDurable msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(msg.txnId, out, version);
            CommandStoreSerializers.durableBefore.serialize(msg.durableBefore, out, version);
        }

        @Override
        public SetGloballyDurable deserialize(DataInputPlus in, int version) throws IOException
        {
            return new SetGloballyDurable(CommandSerializers.txnId.deserialize(in, version), CommandStoreSerializers.durableBefore.deserialize(in, version));
        }

        @Override
        public long serializedSize(SetGloballyDurable msg, int version)
        {
            return CommandSerializers.txnId.serializedSize(msg.txnId, version)
                   + CommandStoreSerializers.durableBefore.serializedSize(msg.durableBefore, version);
        }
    };

    public static final IVersionedSerializer<SyncPoint> syncPoint = new IVersionedSerializer<SyncPoint>()
    {
        @Override
        public void serialize(SyncPoint sp, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(sp.syncId, out, version);
            DepsSerializer.deps.serialize(sp.waitFor, out, version);
            KeySerializers.seekables.serialize(sp.keysOrRanges, out, version);
            KeySerializers.routingKey.serialize(sp.homeKey, out, version);
        }

        @Override
        public SyncPoint deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId syncId = CommandSerializers.txnId.deserialize(in, version);
            Deps waitFor = DepsSerializer.deps.deserialize(in, version);
            Seekables<?, ?> keysOrRanges = KeySerializers.seekables.deserialize(in, version);
            RoutingKey homeKey = KeySerializers.routingKey.deserialize(in, version);
            return SyncPoint.SerializationSupport.construct(syncId, waitFor, keysOrRanges, homeKey);
        }

        @Override
        public long serializedSize(SyncPoint sp, int version)
        {
            return CommandSerializers.txnId.serializedSize(sp.syncId, version)
                   + DepsSerializer.deps.serializedSize(sp.waitFor, version)
                   + KeySerializers.seekables.serializedSize(sp.keysOrRanges, version)
                   + KeySerializers.routingKey.serializedSize(sp.homeKey, version);
        }
    };
}
