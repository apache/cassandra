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
import java.util.Comparator;
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
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.service.accord.IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.CAS_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.USER;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class TxnKeyRead extends AbstractKeySorted<TxnNamedRead> implements TxnRead
{
    public static final TxnKeyRead EMPTY = new TxnKeyRead(new TxnNamedRead[0], null);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY);
    private static final Comparator<TxnNamedRead> TXN_NAMED_READ_KEY_COMPARATOR = Comparator.comparing(TxnNamedRead::key);

    // Cassandra's consistency level used by Accord to safely read data written outside of Accord
    @Nullable
    private final ConsistencyLevel cassandraConsistencyLevel;

    private TxnKeyRead(@Nonnull TxnNamedRead[] items, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        checkNotNull(items, "items is null");
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    private TxnKeyRead(@Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        checkNotNull(items, "items is null");
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    public static TxnKeyRead createTxnRead(@Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel consistencyLevel)
    {
        items.sort(Comparator.comparing(TxnNamedRead::key));
        return new TxnKeyRead(items, consistencyLevel);
    }

    public static TxnKeyRead createSerialRead(List<SinglePartitionReadCommand> readCommands, ConsistencyLevel consistencyLevel)
    {
        List<TxnNamedRead> reads = new ArrayList<>(readCommands.size());
        for (int i = 0; i < readCommands.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), readCommands.get(i)));
        reads.sort(TXN_NAMED_READ_KEY_COMPARATOR);
        return new TxnKeyRead(reads, consistencyLevel);
    }

    public static TxnKeyRead createCasRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel)
    {
        TxnNamedRead read = new TxnNamedRead(txnDataName(CAS_READ), readCommand);
        return new TxnKeyRead(ImmutableList.of(read), consistencyLevel);
    }

    // A read that declares it will read from keys but doesn't actually read any data so dependent transactions will
    // still be applied first
    public static TxnKeyRead createNoOpRead(Keys keys)
    {
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), (PartitionKey)keys.get(i), null));
        return new TxnKeyRead(reads, null);
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
        return Integer.compare(left.txnDataName(), right.txnDataName());
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
        return itemKeys;
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

        return createTxnRead(reads, cassandraConsistencyLevel);
    }

    @Override
    public Read merge(Read read)
    {
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : (TxnKeyRead) read)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return createTxnRead(reads, cassandraConsistencyLevel);
    }

    @Override
    public void unmemoize()
    {
        for (TxnNamedRead read : items)
            read.unmemoize();
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

    public static final TxnReadSerializer<TxnKeyRead> serializer = new TxnReadSerializer<TxnKeyRead>()
    {
        @Override
        public void serialize(TxnKeyRead read, DataOutputPlus out, int version) throws IOException
        {
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
            serializeNullable(read.cassandraConsistencyLevel, out, version, consistencyLevelSerializer);
        }

        @Override
        public TxnKeyRead deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnNamedRead[] items = deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, version, consistencyLevelSerializer);
            return new TxnKeyRead(items, consistencyLevel);
        }

        @Override
        public long serializedSize(TxnKeyRead read, int version)
        {
            long size = 0;
            size += serializedArraySize(read.items, version, TxnNamedRead.serializer);
            size += serializedNullableSize(read.cassandraConsistencyLevel, version, consistencyLevelSerializer);
            return size;
        }
    };
}
