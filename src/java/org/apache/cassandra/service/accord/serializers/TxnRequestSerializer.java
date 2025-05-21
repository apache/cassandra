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
import accord.primitives.Route;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public abstract class TxnRequestSerializer<T extends TxnRequest<?>> implements IVersionedSerializer<T>
{
    void serializeHeader(T msg, DataOutputPlus out, Version version) throws IOException
    {
        CommandSerializers.txnId.serialize(msg.txnId, out);
        KeySerializers.route.serialize(msg.scope, out);
        out.writeUnsignedVInt(msg.waitForEpoch);
    }

    public abstract void serializeBody(T msg, DataOutputPlus out, Version version) throws IOException;

    @Override
    public final void serialize(T msg, DataOutputPlus out, Version version) throws IOException
    {
        serializeHeader(msg, out, version);
        serializeBody(msg, out, version);
    }

    public abstract T deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch) throws IOException;

    @Override
    public final T deserialize(DataInputPlus in, Version version) throws IOException
    {
        TxnId txnId = CommandSerializers.txnId.deserialize(in);
        Route<?> scope = KeySerializers.route.deserialize(in);
        // TODO (desired): there should be a base epoch
        long waitForEpoch = in.readUnsignedVInt();
        return deserializeBody(in, version, txnId, scope, waitForEpoch);
    }

    long serializedHeaderSize(T msg, Version version)
    {
        return CommandSerializers.txnId.serializedSize(msg.txnId)
               + KeySerializers.route.serializedSize(msg.scope()) +
               TypeSizes.sizeofUnsignedVInt(msg.waitForEpoch);
    }

    public abstract long serializedBodySize(T msg, Version version);

    @Override
    public final long serializedSize(T msg, Version version)
    {
        return serializedHeaderSize(msg, version) + serializedBodySize(msg, version);
    }

    public static abstract class WithUnsyncedSerializer<T extends TxnRequest.WithUnsynced<?>> extends TxnRequestSerializer<T>
    {
        @Override
        void serializeHeader(T msg, DataOutputPlus out, Version version) throws IOException
        {
            super.serializeHeader(msg, out, version);
            out.writeUnsignedVInt(msg.minEpoch);
        }

        public abstract T deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException;

        @Override
        public final T deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch) throws IOException
        {
            long minEpoch = in.readUnsignedVInt();
            return deserializeBody(in, version, txnId, scope, waitForEpoch, minEpoch);
        }

        @Override
        long serializedHeaderSize(T msg, Version version)
        {
            long size = super.serializedHeaderSize(msg, version);
            size += TypeSizes.sizeofUnsignedVInt(msg.minEpoch);
            return size;
        }
    }
}
