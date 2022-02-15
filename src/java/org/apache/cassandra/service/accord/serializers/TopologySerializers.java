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

import accord.local.Node;
import accord.messages.TxnRequest;
import accord.messages.TxnRequest.Scope.KeysForEpoch;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TopologySerializers
{
    private TopologySerializers() {}

    public static final IVersionedSerializer<Node.Id> nodeId = new IVersionedSerializer<Node.Id>()
    {
        @Override
        public void serialize(Node.Id id, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(id.id);
        }

        @Override
        public Node.Id deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Node.Id(in.readLong());
        }

        @Override
        public long serializedSize(Node.Id id, int version)
        {
            return TypeSizes.sizeof(id.id);
        }
    };

    private static final IVersionedSerializer<KeysForEpoch> keysForEpochSerializer = new IVersionedSerializer<KeysForEpoch>()
    {
        @Override
        public void serialize(KeysForEpoch keysForEpoch, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(keysForEpoch.epoch);
            KeySerializers.keys.serialize(keysForEpoch.keys, out, version);
        }

        @Override
        public KeysForEpoch deserialize(DataInputPlus in, int version) throws IOException
        {
            return new KeysForEpoch(in.readLong(), KeySerializers.keys.deserialize(in, version));
        }

        @Override
        public long serializedSize(KeysForEpoch keysForEpoch, int version)
        {
            return TypeSizes.sizeof(keysForEpoch.epoch)
                 + KeySerializers.keys.serializedSize(keysForEpoch.keys, version);
        }
    };

    public static final IVersionedSerializer<TxnRequest.Scope> requestScope = new IVersionedSerializer<>()
    {

        @Override
        public void serialize(TxnRequest.Scope scope, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(scope.maxEpoch());
            out.writeInt(scope.size());
            for (int i = 0, mi = scope.size(); i < mi; i++)
                keysForEpochSerializer.serialize(scope.get(i), out, version);
        }

        @Override
        public TxnRequest.Scope deserialize(DataInputPlus in, int version) throws IOException
        {
            long maxEpoch = in.readLong();
            TxnRequest.Scope.KeysForEpoch[] ranges = new TxnRequest.Scope.KeysForEpoch[in.readInt()];
            for (int i = 0; i < ranges.length; i++)
                ranges[i] = keysForEpochSerializer.deserialize(in, version);
            return new TxnRequest.Scope(maxEpoch, ranges);
        }

        @Override
        public long serializedSize(TxnRequest.Scope scope, int version)
        {
            long size = TypeSizes.sizeof(scope.maxEpoch());
            size += TypeSizes.sizeof(scope.size());
            for (int i = 0, mi = scope.size(); i < mi; i++)
                size += keysForEpochSerializer.serializedSize(scope.get(i), version);
            return size;
        }
    };
}
