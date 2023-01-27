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
import org.apache.cassandra.db.SinglePartitionReadCommand;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;

public class TxnRead extends AbstractKeySorted<TxnNamedRead> implements Read
{
    // There is only potentially one partition in a CAS and SERIAL/LOCAL_SERIAL read
    public static final String SERIAL_READ_NAME = "SERIAL_READ";
    public static final TxnDataName SERIAL_READ = TxnDataName.user(SERIAL_READ_NAME);
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnRead(new TxnNamedRead[0], null));

    private final Keys txnKeys;
    
    public TxnRead(TxnNamedRead[] items, Keys txnKeys)
    {
        super(items);
        this.txnKeys = txnKeys;
    }

    public TxnRead(List<TxnNamedRead> items, Keys txnKeys)
    {
        super(items);
        this.txnKeys = txnKeys;
    }

    public static TxnRead createSerialRead(SinglePartitionReadCommand readCommand)
    {
        TxnNamedRead read = new TxnNamedRead(SERIAL_READ, readCommand);
        return new TxnRead(ImmutableList.of(read), Keys.of(read.key()));
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

        return new TxnRead(reads, txnKeys.slice(ranges));
    }

    @Override
    public Read merge(Read read)
    {
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : (TxnRead) read)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return new TxnRead(reads, txnKeys.with((Keys)read.keys()));
    }

    @Override
    public AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        List<AsyncChain<Data>> results = new ArrayList<>();
        forEachWithKey((PartitionKey) key, read -> results.add(read.read(kind.isWrite(), safeStore, executeAt)));

        if (results.isEmpty())
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
            KeySerializers.keys.serialize(read.txnKeys, out, version);
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
        }

        @Override
        public TxnRead deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);
            return new TxnRead(deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new), keys);
        }

        @Override
        public long serializedSize(TxnRead read, int version)
        {
            long size = KeySerializers.keys.serializedSize(read.txnKeys, version);
            size += serializedArraySize(read.items, version, TxnNamedRead.serializer);
            return size;
        }
    };
}
