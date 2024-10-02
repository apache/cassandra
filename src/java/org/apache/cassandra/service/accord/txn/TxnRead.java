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
import accord.api.Key;
import accord.api.Read;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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

public class TxnRead extends AbstractKeySorted<TxnNamedRead> implements Read
{
    private static final TxnRead EMPTY_KEY = new TxnRead(Domain.Key);
    private static final TxnRead EMPTY_RANGE = new TxnRead(Domain.Range);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY_KEY);
    private static final Comparator<TxnNamedRead> TXN_NAMED_READ_KEY_COMPARATOR = Comparator.comparing(a -> ((PartitionKey) a.key()));
    private static final byte TYPE_EMPTY_KEY = 0;
    private static final byte TYPE_EMPTY_RANGE = 1;
    private static final byte TYPE_NOT_EMPTY = 2;

    public static TxnRead empty(Domain domain)
    {
        switch (domain)
        {
            default:
                throw new IllegalStateException("Unhandled domain " + domain);
            case Key:
                return EMPTY_KEY;
            case Range:
                return EMPTY_RANGE;
        }
    }

    // Cassandra's consistency level used by Accord to safely read data written outside of Accord
    @Nullable
    private final ConsistencyLevel cassandraConsistencyLevel;

    // Specifies the domain in case the TxnRead is empty and it can't be inferred
    private final Domain domain;

    private TxnRead(Domain domain)
    {
        super(new TxnNamedRead[0], domain);
        this.domain = domain;
        this.cassandraConsistencyLevel = null;
    }

    private TxnRead(@Nonnull TxnNamedRead[] items, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items, items[0].key().domain());
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read: %s", cassandraConsistencyLevel);
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        this.domain = items[0].key().domain();
    }

    private TxnRead(@Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items, items.get(0).key().domain());
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read: %s", cassandraConsistencyLevel);
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        this.domain = items.get(0).key().domain();
    }

    private static void sortReads(List<TxnNamedRead> reads)
    {
        if (reads.size() == 0)
            return;
        reads.sort(TXN_NAMED_READ_KEY_COMPARATOR);
    }

    public static TxnRead createTxnRead(@Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel consistencyLevel, Domain domain)
    {
        if (items.isEmpty())
            return empty(domain);
        sortReads(items);
        return new TxnRead(items, consistencyLevel);
    }

    public static TxnRead createSerialRead(List<SinglePartitionReadCommand> readCommands, ConsistencyLevel consistencyLevel)
    {
        List<TxnNamedRead> reads = new ArrayList<>(readCommands.size());
        for (int i = 0; i < readCommands.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), readCommands.get(i)));
        sortReads(reads);
        return new TxnRead(reads, consistencyLevel);
    }

    public static TxnRead createCasRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel)
    {
        TxnNamedRead read = new TxnNamedRead(txnDataName(CAS_READ), readCommand);
        return new TxnRead(ImmutableList.of(read), consistencyLevel);
    }

    // A read that declares it will read from keys but doesn't actually read any data so dependent transactions will
    // still be applied first
    public static TxnRead createNoOpRead(Keys keys)
    {
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), keys.get(i), null));
        return new TxnRead(reads, null);
    }

    public static TxnRead createRangeRead(PartitionRangeReadCommand command, AbstractBounds<PartitionPosition> range, ConsistencyLevel consistencyLevel)
    {
        return new TxnRead(ImmutableList.of(new TxnNamedRead(txnDataName(USER), range, command)), consistencyLevel);
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
        return Integer.compare(left.txnDataName(), right.txnDataName());
    }

    @Override
    Seekable getKey(TxnNamedRead read)
    {
        return read.key();
    }

    @Override
    TxnNamedRead[] newArray(int size)
    {
        return new TxnNamedRead[size];
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return itemKeys;
    }

    @Override
    public Domain domain()
    {
        return domain;
    }

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

    private Read intersecting(Seekables<?, ?> select)
    {
        // TODO (review): Why construct this keys at all and not just check against select?
        Seekables<?, ?> keys = (Seekables<?, ?>)itemKeys.intersecting(select);
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());

        switch (keys.domain())
        {
            case Key:
                for (TxnNamedRead read : items)
                    if (keys.contains((Key)read.key()))
                        reads.add(read);
                break;
            case Range:
                for (TxnNamedRead read : items)
                    if (keys.intersects((Range)read.key()))
                        reads.add(read);
                break;
            default:
                throw new IllegalStateException("Unhandled domain " + keys.domain());
        }

        return createTxnRead(reads, cassandraConsistencyLevel, keys.domain());
    }

    @Override
    public Read merge(Read read)
    {
        TxnRead txnRead = (TxnRead)read;
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : txnRead)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return createTxnRead(reads, cassandraConsistencyLevel, txnRead.domain);
    }

    public void unmemoize()
    {
        for (TxnNamedRead read : items)
            read.unmemoize();
    }

    @Override
    public AsyncChain<Data> read(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        // Set to null since we don't need it and interop can pass in null
        safeStore = null;
        ClusterMetadata cm = ClusterMetadata.current();
        checkState(cm.epoch.getEpoch() >= executeAt.epoch(), "TCM epoch %d is < executeAt epoch %d", cm.epoch.getEpoch(), executeAt.epoch());

        List<AsyncChain<Data>> results = new ArrayList<>();
        forEachWithKey(key, read -> results.add(read.read(cassandraConsistencyLevel, key, executeAt)));

        if (results.isEmpty())
            // Result type must match everywhere
            return AsyncChains.success(new TxnData());

        if (results.size() == 1)
            return results.get(0);

        return AsyncChains.reduce(results, Data::merge);
    }

    public static final IVersionedSerializer<TxnRead> serializer = new IVersionedSerializer<TxnRead>()
    {
        @Override
        public void serialize(TxnRead read, DataOutputPlus out, int version) throws IOException
        {
            if (read.items.length > 0)
            {
                out.write(TYPE_NOT_EMPTY);
                serializeArray(read.items, out, version, TxnNamedRead.serializer);
                serializeNullable(read.cassandraConsistencyLevel, out, version, consistencyLevelSerializer);
            }
            else
            {
                out.write(read.domain == Domain.Key ? TYPE_EMPTY_KEY : TYPE_EMPTY_RANGE);
            }
        }

        @Override
        public TxnRead deserialize(DataInputPlus in, int version) throws IOException
        {
            byte type = in.readByte();
            switch (type)
            {
                default:
                    throw new IllegalStateException("Unhandled type " + type);
                case TYPE_EMPTY_KEY:
                    return EMPTY_KEY;
                case TYPE_EMPTY_RANGE:
                    return EMPTY_RANGE;
                case TYPE_NOT_EMPTY:
                    TxnNamedRead[] items = deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
                    ConsistencyLevel consistencyLevel = deserializeNullable(in, version, consistencyLevelSerializer);
                    return new TxnRead(items, consistencyLevel);
            }
        }

        @Override
        public long serializedSize(TxnRead read, int version)
        {
            long size = 1; // type
            if (read.items.length > 0)
            {
                size += serializedArraySize(read.items, version, TxnNamedRead.serializer);
                size += serializedNullableSize(read.cassandraConsistencyLevel, version, consistencyLevelSerializer);
            }
            return size;
        }
    };
}
