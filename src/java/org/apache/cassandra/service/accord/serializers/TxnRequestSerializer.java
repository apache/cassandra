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

import accord.messages.TxnRequest;
import accord.primitives.Keys;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public abstract class TxnRequestSerializer<T extends TxnRequest> implements IVersionedSerializer<T>
{
    void serializeHeader(T msg, DataOutputPlus out, int version) throws IOException
    {
        KeySerializers.keys.serialize(msg.scope(), out, version);
        out.writeLong(msg.waitForEpoch());
    }

    public abstract void serializeBody(T msg, DataOutputPlus out, int version) throws IOException;

    @Override
    public final void serialize(T msg, DataOutputPlus out, int version) throws IOException
    {
        serializeHeader(msg, out, version);
        serializeBody(msg, out, version);
    }

    public abstract T deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException;

    @Override
    public final T deserialize(DataInputPlus in, int version) throws IOException
    {
        Keys scope = KeySerializers.keys.deserialize(in, version);
        long waitForEpoch = in.readLong();
        return deserializeBody(in, version, scope, waitForEpoch);
    }

    long serializedHeaderSize(T msg, int version)
    {
        return KeySerializers.keys.serializedSize(msg.scope(), version) + TypeSizes.LONG_SIZE;
    }

    public abstract long serializedBodySize(T msg, int version);

    @Override
    public final long serializedSize(T msg, int version)
    {
        return serializedHeaderSize(msg, version) + serializedBodySize(msg, version);
    }

    public static abstract class WithUnsyncedSerializer<T extends TxnRequest.WithUnsynced> extends TxnRequestSerializer<T>
    {
        @Override
        void serializeHeader(T msg, DataOutputPlus out, int version) throws IOException
        {
            super.serializeHeader(msg, out, version);
            CommandSerializers.txnId.serialize(msg.txnId, out, version);
            out.writeLong(msg.minEpoch);
        }

        public abstract T deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch, TxnId txnId, long minEpoch) throws IOException;

        @Override
        public final T deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            long minEpoch = in.readLong();
            return deserializeBody(in, version, scope, waitForEpoch, txnId, minEpoch);
        }

        @Override
        long serializedHeaderSize(T msg, int version)
        {
            long size = super.serializedHeaderSize(msg, version);
            size += CommandSerializers.txnId.serializedSize(msg.txnId, version);
            size += TypeSizes.LONG_SIZE;
            return size;
        }
    }
}
