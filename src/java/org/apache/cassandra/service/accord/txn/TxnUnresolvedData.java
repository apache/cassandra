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

import accord.api.UnresolvedData;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.NullableSerializer;

public interface TxnUnresolvedData extends UnresolvedData
{
    IVersionedSerializer<UnresolvedData> serializer = new IVersionedSerializer<UnresolvedData>()
    {
        @Override
        public void serialize(UnresolvedData t, DataOutputPlus out, int version) throws IOException
        {
            byte id = ((TxnUnresolvedData)t).unresolvedDataKind().id;
            out.writeByte(id);
            if (id == 0)
                TxnUnresolvedReadResponses.serializer.serialize((TxnUnresolvedReadResponses) t, out, version);
            else
                TxnData.serializer.serialize((TxnData) t, out, version);
        }

        @Override
        public UnresolvedData deserialize(DataInputPlus in, int version) throws IOException
        {
            byte id = in.readByte();
            if (id == 0)
                return TxnUnresolvedReadResponses.serializer.deserialize(in, version);
            else
                return TxnData.serializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(UnresolvedData t, int version)
        {
            if (((TxnUnresolvedData)t).unresolvedDataKind().id == 0)
                return 1 + TxnUnresolvedReadResponses.serializer.serializedSize((TxnUnresolvedReadResponses) t, version);
            else
                return 1 + TxnData.serializer.serializedSize((TxnData) t, version);
        }
    };

    IVersionedSerializer<UnresolvedData> nullableSerializer = NullableSerializer.wrap(serializer);

    enum UnresolvedDataKind
    {
        READ_RESPONSE(0),
        TXN_DATA(1);

        public final byte id;

        UnresolvedDataKind(int id)
        {
            this.id = (byte) id;
        }

        UnresolvedDataKind fromId(byte id)
        {
            switch (id)
            {
                case 0:
                    return READ_RESPONSE;
                case 1:
                    return TXN_DATA;
                default:
                    throw new IllegalArgumentException("Unrecognized TxnUnresolvedData id " + id);
            }
        }
    }

    UnresolvedDataKind unresolvedDataKind();
}
