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
import accord.messages.TxnRequestScope;
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

    private static final IVersionedSerializer<TxnRequestScope.EpochRanges> epochRangesSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnRequestScope.EpochRanges ranges, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(ranges.epoch);
            KeySerializers.ranges.serialize(ranges.ranges, out, version);

        }

        @Override
        public TxnRequestScope.EpochRanges deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TxnRequestScope.EpochRanges(in.readLong(),
                                                   KeySerializers.ranges.deserialize(in, version));
        }

        @Override
        public long serializedSize(TxnRequestScope.EpochRanges ranges, int version)
        {
            return TypeSizes.sizeof(ranges.epoch) + KeySerializers.ranges.serializedSize(ranges.ranges, version);
        }
    };

    public static final IVersionedSerializer<TxnRequestScope> requestScope = new IVersionedSerializer<>()
    {

        @Override
        public void serialize(TxnRequestScope scope, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(scope.maxEpoch());
            out.writeInt(scope.size());
            for (int i = 0, mi = scope.size(); i < mi; i++)
                epochRangesSerializer.serialize(scope.get(i), out, version);
        }

        @Override
        public TxnRequestScope deserialize(DataInputPlus in, int version) throws IOException
        {
            long maxEpoch = in.readLong();
            TxnRequestScope.EpochRanges[] ranges = new TxnRequestScope.EpochRanges[in.readInt()];
            for (int i = 0; i < ranges.length; i++)
                ranges[i] = epochRangesSerializer.deserialize(in, version);
            return new TxnRequestScope(maxEpoch, ranges);
        }

        @Override
        public long serializedSize(TxnRequestScope scope, int version)
        {
            long size = TypeSizes.sizeof(scope.maxEpoch());
            size += TypeSizes.sizeof(scope.size());
            for (int i = 0, mi = scope.size(); i < mi; i++)
                size += epochRangesSerializer.serializedSize(scope.get(i), version);
            return size;
        }
    };
}
