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
import java.util.Map;

import accord.api.Data;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.Int32Serializer;
import org.apache.cassandra.utils.NullableSerializer;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.utils.Invariants.checkArgument;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.txn_data;

/**
 * Fairly generic holder for result values for Accord txns as well as data exchange during Accord txn execution
 * when read results are returned to the coordinator to compute query results and writes.
 */
public class TxnData extends Int2ObjectHashMap<TxnDataValue> implements TxnResult, Data
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnData());

    private static final int TXN_DATA_NAME_INDEX_BITS = 32 - 6;
    private static final int TXN_DATA_NAME_INDEX_MASK = ~(~0 << TXN_DATA_NAME_INDEX_BITS);
    public static final int TXN_DATA_NAME_INDEX_MAX = ((1 << TXN_DATA_NAME_INDEX_BITS) - 1);

    public enum TxnDataNameKind
    {
        USER((byte) 0),
        RETURNING((byte) 1),
        AUTO_READ((byte) 2),
        CAS_READ((byte) 3);

        private final byte value;

        TxnDataNameKind(byte value)
        {
            this.value = value;
        }

        public static TxnDataNameKind from(byte b)
        {
            switch (b)
            {
                case 0:
                    return USER;
                case 1:
                    return RETURNING;
                case 2:
                    return AUTO_READ;
                case 3:
                    return CAS_READ;
                default:
                    throw new IllegalArgumentException("Unknown kind: " + b);
            }
        }
    }

    public static int txnDataName(TxnDataNameKind kind, int index)
    {
        checkArgument(index >= 0 && index <= TXN_DATA_NAME_INDEX_MAX);
        int kindInt = (int)(((long)kind.value) << TXN_DATA_NAME_INDEX_BITS);
        return kindInt | index;
    }

    public static int txnDataName(TxnDataNameKind kind)
    {
        return txnDataName(kind, 0);
    }

    public static TxnDataNameKind txnDataNameKind(int txnDataName)
    {
        int kind = txnDataName >>> TXN_DATA_NAME_INDEX_BITS;
        return TxnDataNameKind.from((byte)kind);
    }

    public static int txnDataNameIndex(int txnDataName)
    {
        return txnDataName & TXN_DATA_NAME_INDEX_MASK;
    }

    public TxnData() {}

    private TxnData(int size)
    {
        super(size, 0.65f);
    }

    public static TxnData of(int key, TxnDataValue value)
    {
        TxnData result = newWithExpectedSize(1);
        result.put(key, value);
        return result;
    }

    public static TxnData newWithExpectedSize(int size)
    {
        checkArgument(size >= 0, "size can't be negative");
        size = Math.max(4, size);
        return new TxnData(size < 1073741824 ? (int)((float)size / 0.75F + 1.0F) : Integer.MAX_VALUE);
    }

    @Override
    public TxnData merge(Data data)
    {
        TxnData that = (TxnData) data;
        TxnData merged = new TxnData();
        this.forEach(merged::put);
        for (Map.Entry<Integer, TxnDataValue> e : that.entrySet())
            merged.merge(e.getKey(), e.getValue(), TxnDataValue::merge);
        return merged;
    }

    public static Data merge(Data left, Data right)
    {
        if (left == null)
            return right;
        if (right == null)
            return null;

        return left.merge(right);
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE + (size() * TypeSizes.INT_SIZE);
        for (TxnDataValue value : values())
            size += value.estimatedSizeOnHeap();
        return size;
    }

    public static TxnData emptyPartition(int name, SinglePartitionReadCommand command)
    {
        TxnData result = new TxnData();
        TxnDataKeyValue empty = new TxnDataKeyValue(PartitionIterators.getOnlyElement(EmptyIterators.partition(), command));
        result.put(name, empty);
        return result;
    }

    @Override
    public Kind kind()
    {
        return txn_data;
    }

    public static final IVersionedSerializer<TxnData> serializer = new IVersionedSerializer<TxnData>()
    {
        @Override
        public void serialize(TxnData data, DataOutputPlus out, int version) throws IOException
        {
            CollectionSerializers.serializeMap(data, out, version, Int32Serializer.serializer, TxnDataValue.serializer);
        }

        @Override
        public TxnData deserialize(DataInputPlus in, int version) throws IOException
        {
            return CollectionSerializers.deserializeMap(in, version, Int32Serializer.serializer, TxnDataValue.serializer, TxnData::newWithExpectedSize);
        }

        @Override
        public long serializedSize(TxnData data, int version)
        {
            return CollectionSerializers.serializedMapSize(data, version, Int32Serializer.serializer, TxnDataValue.serializer);
        }
    };

    public static final IVersionedSerializer<TxnData> nullableSerializer = NullableSerializer.wrap(serializer);
}
