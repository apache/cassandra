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

import javax.annotation.Nullable;

import accord.primitives.Ranges;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;

/**
 * The result of either a key or range read during Accord transaction execution or a transaction result
 */
public interface TxnDataValue
{
    interface TxnDataValueSerializer<T extends TxnDataValue> extends IVersionedSerializer<T>
    {}

    enum Kind
    {
        key(0),
        range(1);
        int id;

        Kind(int id)
        {
            this.id = id;
        }

        public TxnDataValueSerializer serializer()
        {
            switch (this)
            {
                case key:
                    return TxnDataKeyValue.serializer;
                case range:
                    return TxnDataRangeValue.serializer;
                default:
                    throw new IllegalStateException("Unrecognized kind " + this);
            }
        }
    }

    TxnDataValue.Kind kind();

    TxnDataValue merge(TxnDataValue other);
    @Nullable TxnDataValue without(Ranges ranges);
    long maxTimestamp();

    long estimatedSizeOnHeap();

    IVersionedSerializer<TxnDataValue> serializer = new IVersionedSerializer<TxnDataValue>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(TxnDataValue txnDataValue, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte(txnDataValue.kind().ordinal());
            txnDataValue.kind().serializer().serialize(txnDataValue, out, version);
        }

        @Override
        public TxnDataValue deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnDataValue.Kind kind = TxnDataValue.Kind.values()[in.readByte()];
            return (TxnDataValue)kind.serializer().deserialize(in, version);
        }

        @SuppressWarnings("unchecked")
        @Override
        public long serializedSize(TxnDataValue txnDataValue, Version version)
        {
            return sizeof((byte)txnDataValue.kind().ordinal()) + txnDataValue.kind().serializer().serializedSize(txnDataValue, version);
        }
    };
}
