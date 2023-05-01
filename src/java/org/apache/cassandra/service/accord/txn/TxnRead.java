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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import accord.api.DataStore;
import accord.api.Read;
import accord.api.UnresolvedData;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.txn.TxnDataName.Kind;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Simulate;

import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;

public class TxnRead extends AbstractKeySorted<TxnNamedRead> implements Read
{
    // There is only potentially one partition in a CAS and SERIAL/LOCAL_SERIAL read
    public static final String SERIAL_READ_NAME = "SERIAL_READ";
    public static final TxnDataName SERIAL_READ = TxnDataName.user(SERIAL_READ_NAME);

    public static final String CAS_READ_NAME = "CAS_READ";
    public static final TxnDataName CAS_READ = new TxnDataName(Kind.CAS_READ, CAS_READ_NAME);

    public static final TxnRead EMPTY_READ = new TxnRead(new TxnNamedRead[0], Keys.EMPTY, ConsistencyLevel.ANY);
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnRead(new TxnNamedRead[0], null, null));

    @Nonnull
    private final Keys txnKeys;

    // Cassandra's consistency level used by Accord to safely read data written outside of Accord
    @Nonnull
    private final ConsistencyLevel cassandraConsistencyLevel;

    public TxnRead(@Nonnull TxnNamedRead[] items, @Nonnull Keys txnKeys, @Nonnull ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        this.txnKeys = txnKeys;
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    public TxnRead(@Nonnull List<TxnNamedRead> items, @Nonnull Keys txnKeys, @Nonnull ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        this.txnKeys = txnKeys;
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    public static TxnRead createTxnRead(@Nonnull List<TxnNamedRead> items, @Nonnull Keys txnKeys, @Nonnull ConsistencyLevel consistencyLevel)
    {
        return new TxnRead(items, txnKeys, consistencyLevel);
    }

    public static TxnRead createSerialRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel)
    {
        TxnNamedRead read = new TxnNamedRead(SERIAL_READ, readCommand);
        return new TxnRead(ImmutableList.of(read), Keys.of(read.key()), consistencyLevel);
    }

    public static TxnRead createCasRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel)
    {
        TxnNamedRead read = new TxnNamedRead(CAS_READ, readCommand);
        return new TxnRead(ImmutableList.of(read), Keys.of(read.key()), consistencyLevel);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (TxnNamedRead read : items)
            size += read.estimatedSizeOnHeap();
        return size;
    }

    @Override
    int compareNonKeyFields(TxnNamedRead left, TxnNamedRead right)
    {
        return left.txnDataName().compareTo(right.txnDataName());
    }

    @Override
    PartitionKey getKey(TxnNamedRead read)
    {
        return read.key();
    }

    @Override
    TxnNamedRead[] newArray(int size)
    {
        return new TxnNamedRead[size];
    }

    @Override
    public Keys keys()
    {
        return txnKeys;
    }

    @Override
    public accord.primitives.ConsistencyLevel readDataCL()
    {
        switch (cassandraConsistencyLevel)
        {
            case ONE:
                return accord.primitives.ConsistencyLevel.ONE;
            case SERIAL:
            case QUORUM:
                return accord.primitives.ConsistencyLevel.QUORUM;
            default:
                throw new IllegalStateException("ConsistencyLevel " + cassandraConsistencyLevel + " is not supported as an Accord read ConsistencyLevel");
        }
    }

    public ConsistencyLevel cassandraConsistencyLevel()
    {
        return cassandraConsistencyLevel;
    }

    @Override
    public Read slice(Ranges ranges)
    {
        Keys keys = itemKeys.slice(ranges);
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());

        for (TxnNamedRead read : items)
            if (keys.contains(read.key()))
                reads.add(read);

        return createTxnRead(reads, txnKeys.slice(ranges), cassandraConsistencyLevel);
    }

    @Override
    public Read merge(Read read)
    {
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : (TxnRead) read)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return createTxnRead(reads, txnKeys.with((Keys)read.keys()), cassandraConsistencyLevel);
    }

    @Override
    public AsyncChain<UnresolvedData> read(Seekable key, Txn.Kind kind, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        List<AsyncChain<UnresolvedData>> results = new ArrayList<>();
        forEachWithKey((PartitionKey) key, read -> results.add(read.read(cassandraConsistencyLevel, kind.isWrite(), safeStore, executeAt)));

        if (results.isEmpty())
            return AsyncChains.success(new TxnUnresolvedData());

        if (results.size() == 1)
            return results.get(0);

        return AsyncChains.reduce(results, UnresolvedData::merge);
    }

    @Simulate(with = MONITORS)
    public static final IVersionedSerializer<TxnRead> serializer = new IVersionedSerializer<TxnRead>()
    {
        @Override
        public void serialize(TxnRead read, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.keys.serialize(read.txnKeys, out, version);
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
            serializeNullable(read.cassandraConsistencyLevel, out, version, consistencyLevelSerializer);
        }

        @Override
        public TxnRead deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);
            TxnNamedRead[] items = deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, version, consistencyLevelSerializer);
            return new TxnRead(items, keys, consistencyLevel);
        }

        @Override
        public long serializedSize(TxnRead read, int version)
        {
            long size = KeySerializers.keys.serializedSize(read.txnKeys, version);
            size += serializedArraySize(read.items, version, TxnNamedRead.serializer);
            size += serializedNullableSize(read.cassandraConsistencyLevel, version, consistencyLevelSerializer);
            return size;
        }
    };
}
