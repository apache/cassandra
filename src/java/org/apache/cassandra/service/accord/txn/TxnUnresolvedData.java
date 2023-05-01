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
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnUnresolvedData.UnresolvedDataEntry;

public class TxnUnresolvedData extends ArrayList<UnresolvedDataEntry> implements UnresolvedData
{
    public TxnUnresolvedData()
    {
        super();
    }

    public TxnUnresolvedData(int initialCapacity)
    {
        super(initialCapacity);
    }

    @Override
    public UnresolvedData merge(UnresolvedData unresolvedData)
    {
        TxnUnresolvedData that = (TxnUnresolvedData)unresolvedData;
        TxnUnresolvedData merged = new TxnUnresolvedData();
        this.forEach(merged::add);
        that.forEach(merged::add);
        return merged;
    }

    public static class UnresolvedDataEntry
    {
        final TxnDataName txnDataName;
        final ReadResponse readResponse;
        final int nowInSec;

        public UnresolvedDataEntry(TxnDataName txnDataName, ReadResponse readResponse, int nowInSec)
        {
            this.txnDataName = txnDataName;
            this.readResponse = readResponse;
            this.nowInSec = nowInSec;
        }
    }

    public static final IVersionedSerializer<TxnUnresolvedData> serializer = new IVersionedSerializer<TxnUnresolvedData>()
    {
        @Override
        public void serialize(TxnUnresolvedData data, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(data.size());
            for (UnresolvedDataEntry e : data)
            {
                TxnDataName.serializer.serialize(e.txnDataName, out, version);
                ReadResponse.serializer.serialize(e.readResponse, out, version);
                out.writeInt(e.nowInSec);
            }
        }

        @Override
        public TxnUnresolvedData deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            TxnUnresolvedData data = new TxnUnresolvedData(size);
            for (int i = 0; i < size; i++)
            {
                TxnDataName name = TxnDataName.serializer.deserialize(in, version);
                ReadResponse readResponse = ReadResponse.serializer.deserialize(in, version);
                int nowInSec = in.readInt();
                data.add(new UnresolvedDataEntry(name, readResponse, nowInSec));
            }
            return data;
        }

        @Override
        public long serializedSize(TxnUnresolvedData data, int version)
        {
            // Num entries and nowInSec for each entry
            long size = TypeSizes.sizeofUnsignedVInt(data.size()) + (4 * data.size());
            for (UnresolvedDataEntry e : data)
            {
                size += TxnDataName.serializer.serializedSize(e.txnDataName, version);
                size += ReadResponse.serializer.serializedSize(e.readResponse, version);
            }
            return size;
        }
    };
}
