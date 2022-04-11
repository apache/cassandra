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
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TopologySerializers
{
    private TopologySerializers() {}

    public static final IVersionedSerializer<Node.Id> nodeId = new IVersionedSerializer<>()
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

    public static final IVersionedSerializer<TxnRequest.Scope> requestScope = new IVersionedSerializer<>()
    {

        @Override
        public void serialize(TxnRequest.Scope scope, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(scope.minRequiredEpoch());
            KeySerializers.keys.serialize(scope.keys(), out, version);
        }

        @Override
        public TxnRequest.Scope deserialize(DataInputPlus in, int version) throws IOException
        {
            long maxEpoch = in.readLong();
            return new TxnRequest.Scope(maxEpoch, KeySerializers.keys.deserialize(in, version));
        }

        @Override
        public long serializedSize(TxnRequest.Scope scope, int version)
        {
            long size = TypeSizes.sizeof(scope.minRequiredEpoch());
            return size + KeySerializers.keys.serializedSize(scope.keys(), version);
        }
    };
}
