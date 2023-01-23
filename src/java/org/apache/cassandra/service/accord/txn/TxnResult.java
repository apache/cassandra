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

import accord.api.Result;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public abstract class TxnResult implements Result
{
    public interface TxnResultSerializer<T extends TxnResult> extends IVersionedSerializer<T> {}

    public enum Kind
    {
        txn_data(0),
        retry_new_protocol(1);

        int id;

        Kind(int id)
        {
            this.id = id;
        }

        public TxnResultSerializer serializer()
        {
            switch (this)
            {
                case txn_data:
                    return TxnData.serializer;
                case retry_new_protocol:
                    return RetryWithNewProtocolResult.serializer;
                default:
                    throw new IllegalStateException("Unrecognized kind " + this);
            }
        }
    }

    public abstract Kind kind();

    public abstract long estimatedSizeOnHeap();

    public static final IVersionedSerializer<TxnResult> serializer = new IVersionedSerializer<TxnResult>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(TxnResult txnResult, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(txnResult.kind().ordinal());
            txnResult.kind().serializer().serialize(txnResult, out, version);
        }

        @Override
        public TxnResult deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnResult.Kind kind = TxnResult.Kind.values()[in.readByte()];
            return (TxnResult)kind.serializer().deserialize(in, version);
        }

        @SuppressWarnings("unchecked")
        @Override
        public long serializedSize(TxnResult txnResult, int version)
        {
            return sizeof((byte)txnResult.kind().ordinal()) + txnResult.kind().serializer().serializedSize(txnResult, version);
        }
    };
}
