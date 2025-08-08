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
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.Int32Serializer;
import org.apache.cassandra.utils.NullableSerializer;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.utils.Invariants.requireArgument;
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
        requireArgument(index >= 0 && index <= TXN_DATA_NAME_INDEX_MAX);
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
        requireArgument(size >= 0, "size can't be negative");
        size = Math.max(4, size);
        return new TxnData(size < 1073741824 ? (int)((float)size / 0.75F + 1.0F) : Integer.MAX_VALUE);
    }

    @Override
    public TxnData merge(Data data)
    {
        return merge(this, (TxnData) data);
    }

    private static TxnData merge(TxnData a, TxnData b)
    {
        if (a.size() < b.size()) { TxnData tmp = a; a = b; b = tmp; }

        TxnData merged = null;
        int matches = 0;
        for (Map.Entry<Integer, TxnDataValue> e : a.entrySet())
        {
            Integer key = e.getKey();
            TxnDataValue av = e.getValue();
            TxnDataValue bv = b.get(key);
            if (bv != null || merged != null)
            {
                if (bv == null) merged.put(key, av);
                else
                {
                    ++matches;
                    TxnDataValue upd = e.getValue().merge(bv);
                    if (merged != null || upd != av)
                    {
                        if (merged == null) merged = new TxnData();
                        merged.put(key, upd);
                    }
                }
            }
        }

        if (matches == b.size())
            return merged == null ? a : merged;

        if (merged == null)
        {
            merged = new TxnData();
            a.forEach(merged::put);
        }

        b.forEach(merged::putIfAbsent);
        return merged;
    }

    @Override
    public Data without(Ranges ranges)
    {
        TxnData result = null;
        for (Map.Entry<Integer, TxnDataValue> e : entrySet())
        {
            TxnDataValue oldValue = e.getValue();
            TxnDataValue newValue = oldValue.without(ranges);
            if (oldValue == newValue)
            {
                if (result != null)
                    result.put(e.getKey(), oldValue);
            }
            else
            {
                if (result == null)
                {
                    result = new TxnData();
                    for (Map.Entry<Integer, TxnDataValue> e2 : entrySet())
                    {
                        if (e2.getKey() == e.getKey()) break;
                        result.put(e2.getKey(), e2.getValue());
                    }
                }
                if (newValue != null)
                    result.put(e.getKey(), newValue);
            }
        }

        return result != null ? result : this;
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
    public boolean validateReply(TxnId txnId, Timestamp executeAt, boolean futureReadPossible)
    {
        if (futureReadPossible)
        {
            for (TxnDataValue value : values())
            {
                if (value.maxTimestamp() >= executeAt.hlc())
                    return false;
            }
        }
        return true;
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

    private static final IVersionedSerializer<Integer> INT32_SERIALIZER = IVersionedSerializer.fromSerializer(Int32Serializer.serializer);
    public static final IVersionedSerializer<TxnData> serializer = new IVersionedSerializer<TxnData>()
    {
        @Override
        public void serialize(TxnData data, DataOutputPlus out, Version version) throws IOException
        {
            CollectionSerializers.serializeMap(data, out, version, INT32_SERIALIZER, TxnDataValue.serializer);
        }

        @Override
        public TxnData deserialize(DataInputPlus in, Version version) throws IOException
        {
            return CollectionSerializers.deserializeMap(in, version, INT32_SERIALIZER, TxnDataValue.serializer, TxnData::newWithExpectedSize);
        }

        @Override
        public long serializedSize(TxnData data, Version version)
        {
            return CollectionSerializers.serializedMapSize(data, version, INT32_SERIALIZER, TxnDataValue.serializer);
        }
    };

    public static final VersionedSerializer<TxnData, Version> nullableSerializer = NullableSerializer.wrap(serializer);
}
