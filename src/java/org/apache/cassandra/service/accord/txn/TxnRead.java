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
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Read;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
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
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

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

    // Used if the read is in a migrating range to do a coordinated C* read
    // Likely has uses with interoperability in the future
    @Nonnull
    private final ConsistencyLevel consistencyLevel;

    public TxnRead(@Nonnull TxnNamedRead[] items, @Nonnull Keys txnKeys, @Nonnull ConsistencyLevel consistencyLevel)
    {
        super(items);
        this.txnKeys = txnKeys;
        this.consistencyLevel = consistencyLevel;
    }

    public TxnRead(@Nonnull List<TxnNamedRead> items, @Nonnull Keys txnKeys, @Nonnull ConsistencyLevel consistencyLevel)
    {
        super(items);
        this.txnKeys = txnKeys;
        this.consistencyLevel = consistencyLevel;
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

    public Keys readKeys()
    {
        return itemKeys;
    }

    @Override
    public Read slice(Ranges ranges)
    {
        Keys keys = itemKeys.slice(ranges);
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());

        for (TxnNamedRead read : items)
            if (keys.contains(read.key()))
                reads.add(read);

        return createTxnRead(reads, txnKeys.slice(ranges), consistencyLevel);
    }

    @Override
    public Read merge(Read read)
    {
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : (TxnRead) read)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return createTxnRead(reads, txnKeys.with((Keys)read.keys()), consistencyLevel);
    }

    @Override
    public Future<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        List<Future<Data>> futures = new ArrayList<>();
        forEachWithKey((PartitionKey) key, read -> futures.add(read.read(consistencyLevel, kind.isWrite(), safeStore, executeAt)));

        if (futures.isEmpty())
            return ImmediateFuture.success(new TxnData());

        if (futures.size() == 1)
            return futures.get(0);

        return new MultiReadFuture(futures);
    }

    @Simulate(with = MONITORS)
    private static class MultiReadFuture extends AsyncPromise<Data> implements BiConsumer<Data, Throwable>
    {
        private Data result = null;
        private int pending;

        public MultiReadFuture(List<Future<Data>> futures)
        {
            pending = futures.size();
            listen(futures);
        }

        private synchronized void listen(List<Future<Data>> futures)
        {
            for (int i=0, mi=futures.size(); i<mi; i++)
                futures.get(i).addCallback(this);
        }

        @Override
        public synchronized void accept(Data data, Throwable throwable)
        {
            if (isDone())
                return;

            if (throwable != null)
                tryFailure(throwable);

            result = result != null ? result.merge(data) : data;
            if (--pending == 0)
                trySuccess(result);
        }
    }

    public static final IVersionedSerializer<TxnRead> serializer = new IVersionedSerializer<TxnRead>()
    {
        @Override
        public void serialize(TxnRead read, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.keys.serialize(read.txnKeys, out, version);
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
            serializeNullable(read.consistencyLevel, out, version, consistencyLevelSerializer);
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
            size += serializedNullableSize(read.consistencyLevel, version, consistencyLevelSerializer);
            return size;
        }
    };
}
