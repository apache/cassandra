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

import accord.utils.Invariants;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.txn_data;

public class TxnDataResult extends TxnData implements TxnResult
{
    public final long atMicros;

    public TxnDataResult(long atMicros)
    {
        this.atMicros = atMicros;
    }

    private TxnDataResult(long atMicros, int size)
    {
        super(capacityForExpectedSize(size));
        this.atMicros = atMicros;
    }

    @Override
    public Kind kind()
    {
        return txn_data;
    }

    public static TxnDataResult of(long timestamp, int key, TxnDataValue value)
    {
        TxnDataResult result = new TxnDataResult(timestamp);
        result.put(key, value);
        return result;
    }

    public static final IVersionedSerializer<TxnDataResult> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnDataResult data, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt(data.atMicros);
            TxnData.serializer.serialize(data, out, version);
        }

        @Override
        public TxnDataResult deserialize(DataInputPlus in, Version version) throws IOException
        {
            long atMicros = in.readUnsignedVInt();
            return CollectionSerializers.deserializeMap(in, version, INT32_SERIALIZER, TxnDataValue.serializer, expectedSize -> new TxnDataResult(atMicros, expectedSize));
        }

        @Override
        public long serializedSize(TxnDataResult data, Version version)
        {
            return TypeSizes.sizeofUnsignedVInt(data.atMicros) + TxnData.serializer.serializedSize(data, version);
        }
    };

    public static final UnversionedSerializer<PersistableResult> persistable = new UnversionedSerializer<>()
    {
        public void serialize(PersistableResult t, DataOutputPlus out)
        {
            Invariants.require(t == PERSISTABLE);
        }

        public PersistableResult deserialize(DataInputPlus in)
        {
            return PERSISTABLE;
        }

        public long serializedSize(PersistableResult t)
        {
            return 0;
        }
    };
}
