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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.util.ArrayList;

import accord.api.UnresolvedData;
import accord.local.Node.Id;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessage;
import org.apache.cassandra.service.accord.EndpointMapping;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.txn.TxnUnresolvedReadResponses.UnresolvedDataEntry;

public class TxnUnresolvedReadResponses extends ArrayList<UnresolvedDataEntry> implements TxnUnresolvedData
{
    public TxnUnresolvedReadResponses()
    {
        super();
    }

    public TxnUnresolvedReadResponses(int initialCapacity)
    {
        super(initialCapacity);
    }

    @Override
    public UnresolvedData merge(UnresolvedData unresolvedData)
    {
        TxnUnresolvedReadResponses that = (TxnUnresolvedReadResponses)unresolvedData;
        TxnUnresolvedReadResponses merged = new TxnUnresolvedReadResponses();
        this.forEach(merged::add);
        that.forEach(merged::add);
        return merged;
    }

    @Override
    public UnresolvedDataKind unresolvedDataKind()
    {
        return UnresolvedDataKind.READ_RESPONSE;
    }

    public static class UnresolvedDataEntry implements IMessage<ReadResponse>
    {
        final TxnDataName txnDataName;
        final ReadResponse readResponse;
        final int nowInSec;
        final Id from;

        public UnresolvedDataEntry(TxnDataName txnDataName, ReadResponse readResponse, int nowInSec, Id from)
        {
            this.txnDataName = txnDataName;
            this.readResponse = readResponse;
            this.nowInSec = nowInSec;
            this.from = from;
        }

        public InetAddressAndPort from()
        {
            return EndpointMapping.getEndpoint(from);
        }

        @Override
        public ReadResponse payload()
        {
            return readResponse;
        }

        @Override
        public String toString()
        {
            return "UnresolvedDataEntry{" +
                   "txnDataName=" + txnDataName +
                   ", readResponse=" + readResponse +
                   ", nowInSec=" + nowInSec +
                   ", from=" + from +
                   '}';
        }
    }

    public static final IVersionedSerializer<TxnUnresolvedReadResponses> serializer = new IVersionedSerializer<TxnUnresolvedReadResponses>()
    {
        @Override
        public void serialize(TxnUnresolvedReadResponses data, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(data.size());
            for (UnresolvedDataEntry e : data)
            {
                TxnDataName.serializer.serialize(e.txnDataName, out, version);
                out.writeInt(e.nowInSec);
                TopologySerializers.nodeId.serialize(e.from, out, version);
                ReadResponse.serializer.serialize(e.readResponse, out, version);
            }
        }

        @Override
        public TxnUnresolvedReadResponses deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            TxnUnresolvedReadResponses data = new TxnUnresolvedReadResponses(size);
            for (int i = 0; i < size; i++)
            {
                TxnDataName name = TxnDataName.serializer.deserialize(in, version);
                int nowInSec = in.readInt();
                Id from = TopologySerializers.nodeId.deserialize(in, version);
                ReadResponse readResponse = ReadResponse.serializer.deserialize(in, version);
                data.add(new UnresolvedDataEntry(name, readResponse, nowInSec, from));
            }
            return data;
        }

        @Override
        public long serializedSize(TxnUnresolvedReadResponses data, int version)
        {
            // Num entries and nowInSec for each entry
            long size = TypeSizes.sizeofUnsignedVInt(data.size()) + (4 * data.size());
            for (UnresolvedDataEntry e : data)
            {
                size += TxnDataName.serializer.serializedSize(e.txnDataName, version);
                size += ReadResponse.serializer.serializedSize(e.readResponse, version);
                size += TopologySerializers.nodeId.serializedSize(e.from, version);
            }
            return size;
        }
    };
}
