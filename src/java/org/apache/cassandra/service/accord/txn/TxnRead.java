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

import accord.api.Read;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public interface TxnRead extends Read
{
    ConsistencyLevel cassandraConsistencyLevel();

    public interface TxnReadSerializer<T extends TxnRead> extends IVersionedSerializer<T> {}

    enum Kind
    {
        key(0),
        range(1);

        int id;

        Kind(int id)
        {
            this.id = id;
        }

        public TxnReadSerializer serializer()
        {
            switch (this)
            {
                case key:
                    return TxnKeyRead.serializer;
                case range:
                    return TxnRangeRead.serializer;
                default:
                    throw new IllegalStateException("Unrecognized kind " + this);
            }
        }
    }

    long estimatedSizeOnHeap();

    Kind kind();

    IVersionedSerializer<TxnRead> serializer = new IVersionedSerializer<TxnRead>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(TxnRead txnRead, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(txnRead.kind().ordinal());
            txnRead.kind().serializer().serialize(txnRead, out, version);
        }

        @Override
        public TxnRead deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnRead.Kind kind = TxnRead.Kind.values()[in.readByte()];
            return (TxnRead)kind.serializer().deserialize(in, version);
        }

        @SuppressWarnings("unchecked")
        @Override
        public long serializedSize(TxnRead txnRead, int version)
        {
            return sizeof((byte)txnRead.kind().ordinal()) + txnRead.kind().serializer().serializedSize(txnRead, version);
        }
    };
}
