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
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Read;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Simulate;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.service.accord.IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;

public class TxnKeyRead extends AbstractKeySorted<TxnNamedRead> implements TxnRead
{
    public static final TxnKeyRead EMPTY = new TxnKeyRead(new TxnNamedRead[0], Keys.EMPTY, null);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY);

    public static final String CAS_READ_NAME = "CAS_READ";
    public static final TxnDataName CAS_READ = new TxnDataName(TxnDataName.Kind.CAS_READ, CAS_READ_NAME);

    @Nonnull
    private final Keys txnKeys;

    // Cassandra's consistency level used by Accord to safely read data written outside of Accord
    @Nullable
    private final ConsistencyLevel cassandraConsistencyLevel;

    public TxnKeyRead(@Nonnull TxnNamedRead[] items, @Nonnull Keys txnKeys, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.txnKeys = txnKeys;
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    public TxnKeyRead(@Nonnull List<TxnNamedRead> items, @Nonnull Keys txnKeys, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.txnKeys = txnKeys;
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    public static TxnKeyRead createTxnRead(@Nonnull List<TxnNamedRead> items, @Nonnull Keys txnKeys, @Nullable ConsistencyLevel consistencyLevel)
    {
        return new TxnKeyRead(items, txnKeys, consistencyLevel);
    }

    public static TxnKeyRead createSerialRead(List<SinglePartitionReadCommand> readCommands, ConsistencyLevel consistencyLevel)
    {
        List<TxnNamedRead> reads = new ArrayList<>(readCommands.size());
        for (int i = 0; i < readCommands.size(); i++)
            reads.add(new TxnNamedRead(TxnDataName.user(String.valueOf(i)), readCommands.get(i)));
        Keys keys = Keys.of(reads, TxnNamedRead::key);
        return new TxnKeyRead(reads, keys, consistencyLevel);
    }

    public static TxnKeyRead createCasRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel)
    {
        TxnNamedRead read = new TxnNamedRead(CAS_READ, readCommand);
        return new TxnKeyRead(ImmutableList.of(read), Keys.of(read.key()), consistencyLevel);
    }

    // A read that declares it will read from keys but doesn't actually read any data so dependent transactions will
    // still be applied first
    public static TxnKeyRead createNoOpRead(Keys keys)
    {
        return new TxnKeyRead(ImmutableList.of(), keys, null);
    }

    @Override
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
    public ConsistencyLevel cassandraConsistencyLevel()
    {
        return cassandraConsistencyLevel;
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return intersecting(itemKeys.slice(ranges));
    }

    @Override
    public Read intersecting(Participants<?> participants)
    {
        return intersecting(itemKeys.intersecting(participants));
    }

    private Read intersecting(Keys select)
    {
        Keys keys = itemKeys.intersecting(select);
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());

        for (TxnNamedRead read : items)
            if (keys.contains(read.key()))
                reads.add(read);

        return createTxnRead(reads, txnKeys.intersecting(select), cassandraConsistencyLevel);
    }

    @Override
    public Read merge(Read read)
    {
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : (TxnKeyRead) read)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return createTxnRead(reads, txnKeys.with((Keys)read.keys()), cassandraConsistencyLevel);
    }

    @Override
    public AsyncChain<Data> read(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        List<AsyncChain<Data>> results = new ArrayList<>();
        forEachWithKey((PartitionKey) key, read -> results.add(read.read(cassandraConsistencyLevel, executeAt)));

        if (results.isEmpty())
            // Result type must match everywhere
            return AsyncChains.success(new TxnData());

        if (results.size() == 1)
            return results.get(0);

        return AsyncChains.reduce(results, Data::merge);
    }

    @Override
    public Kind kind()
    {
        return Kind.key;
    }

    @Simulate(with = MONITORS)
    public static final TxnReadSerializer<TxnKeyRead> serializer = new TxnReadSerializer<TxnKeyRead>()
    {
        @Override
        public void serialize(TxnKeyRead read, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.keys.serialize(read.txnKeys, out, version);
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
            serializeNullable(read.cassandraConsistencyLevel, out, version, consistencyLevelSerializer);
        }

        @Override
        public TxnKeyRead deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);
            TxnNamedRead[] items = deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, version, consistencyLevelSerializer);
            return new TxnKeyRead(items, keys, consistencyLevel);
        }

        @Override
        public long serializedSize(TxnKeyRead read, int version)
        {
            long size = KeySerializers.keys.serializedSize(read.txnKeys, version);
            size += serializedArraySize(read.items, version, TxnNamedRead.serializer);
            size += serializedNullableSize(read.cassandraConsistencyLevel, version, consistencyLevelSerializer);
            return size;
        }
    };
}
