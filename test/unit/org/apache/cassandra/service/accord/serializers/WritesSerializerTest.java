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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.primitives.Keys;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.SimpleBitSets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.WritesSerializer;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Writes.keys is only the context against which Writes.write is serialized; replicas derive the keys they apply
 * from their own definition of the transaction. AccordConfig.alpha_compatibility_serialize_write_keys decides if we continue
 * to serialize every key the transaction writes (as earlier versions require, to decide which keys to apply), or
 * only those whose writes cannot be derived from the definition (i.e. those TxnWrite carries updates for).
 *
 * @author LLM
 */
public class WritesSerializerTest
{
    private static TableMetadata table;
    private static TableMetadatas tables;
    private static Keys keys;
    private static TxnId txnId;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        table = TableMetadata.builder("ks", "tbl", TableId.fromUUID(new java.util.UUID(0, 1)))
                             .addPartitionKeyColumn("key", BytesType.instance)
                             .partitioner(Murmur3Partitioner.instance)
                             .build();
        tables = TableMetadatas.of(Collections.singletonList(table));
        List<PartitionKey> list = new ArrayList<>();
        for (int i = 0 ; i < 3 ; ++i)
            list.add(new PartitionKey(table.id, decorate(i)));
        keys = Keys.of(list);
        txnId = new TxnId(1, 1, Txn.Kind.Write, Domain.Key, new Node.Id(1));
    }

    private static DecoratedKey decorate(int i)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(i));
    }

    private static TxnWrite write(PartitionKey... updatesFor)
    {
        List<TxnWrite.Update> updates = new ArrayList<>();
        for (int i = 0 ; i < updatesFor.length ; ++i)
            updates.add(new TxnWrite.Update(updatesFor[i], i, PartitionUpdate.emptyUpdate(table, updatesFor[i].partitionKey()), tables));
        return new TxnWrite(tables, updates, SimpleBitSets.allSet(1));
    }

    /**
     * The serializer prior to the introduction of alpha_compatibility_serialize_write_keys
     */
    private static byte[] legacySerialize(Writes writes) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            CommandSerializers.txnId.serialize(writes.txnId, out);
            ExecuteAtSerializer.serialize(writes.txnId, writes.executeAt, out);
            out.writeBoolean(writes.write != null);
            KeySerializers.seekables.serialize(writes.keys, out);
            if (writes.write != null)
                CommandSerializers.write.serialize(writes.write, writes.keys, out, Version.LATEST);
            return out.toByteArray();
        }
    }

    private static byte[] serialize(Writes writes, boolean serializeKeys) throws IOException
    {
        boolean prev = WritesSerializer.serializeKeys();
        WritesSerializer.setSerializeKeys(serializeKeys);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            CommandSerializers.writes.serialize(writes, out, Version.LATEST);
            assertEquals(CommandSerializers.writes.serializedSize(writes, Version.LATEST), out.getLength());
            return out.toByteArray();
        }
        finally
        {
            WritesSerializer.setSerializeKeys(prev);
        }
    }

    private static Writes deserialize(byte[] bytes, boolean serializeKeys) throws IOException
    {
        boolean prev = WritesSerializer.serializeKeys();
        WritesSerializer.setSerializeKeys(serializeKeys);
        try
        {
            return deserialize(bytes);
        }
        finally
        {
            WritesSerializer.setSerializeKeys(prev);
        }
    }

    private static Writes deserialize(byte[] bytes) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            CommandSerializers.writes.deserialize(in, Version.LATEST);
            assertEquals(0, in.available());
        }
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            CommandSerializers.writes.skip(in, Version.LATEST);
            assertEquals(0, in.available());
        }
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return CommandSerializers.writes.deserialize(in, Version.LATEST);
        }
    }

    @Test
    public void defaultIsCompatible()
    {
        assertTrue(new org.apache.cassandra.config.AccordConfig().alpha_compatibility_serialize_write_keys);
        assertTrue(WritesSerializer.serializeKeys());
    }

    private static void test(Writes writes, Keys expectReducedKeys) throws IOException
    {
        // compatible: identical to the prior encoding, and lossless
        byte[] compatible = serialize(writes, true);
        assertArrayEquals(legacySerialize(writes), compatible);
        assertEquals(writes, deserialize(compatible));

        // reduced: only the keys TxnWrite carries updates for, against which the TxnWrite is serialized
        byte[] reduced = serialize(writes, false);
        Writes roundTrip = deserialize(reduced);
        assertEquals(writes, roundTrip); // Writes.equals ignores keys
        assertEquals(writes.txnId, roundTrip.txnId);
        assertEquals(writes.executeAt, roundTrip.executeAt);
        assertEquals(expectReducedKeys, roundTrip.keys);
        if (writes.write == null)
        {
            assertNull(roundTrip.write);
        }
        else
        {
            TxnWrite expect = (TxnWrite) writes.write, actual = (TxnWrite) roundTrip.write;
            assertEquals(expect, actual);
            assertEquals(expect.size(), actual.size());
            for (int i = 0 ; i < expect.size() ; ++i)
                assertEquals(expect.get(i).key, actual.get(i).key);
        }
        assertTrue(reduced.length <= compatible.length);
        assertEquals(expectReducedKeys.size() < writes.keys.size(), reduced.length < compatible.length);

        // decoding does not depend on the flag: data written in either mode is read identically in either mode
        for (boolean writeFlag : new boolean[] { true, false })
        {
            byte[] bytes = writeFlag ? compatible : reduced;
            Writes asWritten = deserialize(bytes, writeFlag);
            Writes inverted = deserialize(bytes, !writeFlag);
            assertEquals(writes, inverted);
            assertEquals(asWritten.keys, inverted.keys);
            assertEquals(writes.hashCode(), inverted.hashCode());
            // re-serializing without compatibility is always equivalent to serializing the original without it;
            // but once serialized without compatibility we cannot recover the derivable keys, so re-serializing such
            // data with compatibility includes only the keys we have (which is why compatibility must be the default
            // until no version uses Writes.keys to decide which keys to apply)
            assertArrayEquals(reduced, serialize(inverted, false));
            assertArrayEquals(writeFlag ? compatible : reduced, serialize(inverted, true));
        }
    }

    @Test
    public void derivableWritesOnly() throws IOException
    {
        // e.g. a blind write, whose updates are all derivable from the transaction definition
        test(new Writes(txnId, txnId, keys, write()), Keys.EMPTY);
    }

    @Test
    public void someNonDerivableWrites() throws IOException
    {
        PartitionKey key = (PartitionKey) keys.get(1);
        test(new Writes(txnId, txnId, keys, write(key)), Keys.of(key));
    }

    @Test
    public void allNonDerivableWrites() throws IOException
    {
        test(new Writes(txnId, txnId, keys, write((PartitionKey) keys.get(0), (PartitionKey) keys.get(1), (PartitionKey) keys.get(2))), keys);
    }

    @Test
    public void noWrite() throws IOException
    {
        test(new Writes(txnId, txnId, Keys.EMPTY, null), Keys.EMPTY);
        // Writes without a write may not claim any keys
        assertThatThrownBy(() -> new Writes(txnId, txnId, keys, null)).isInstanceOf(IllegalStateException.class);
    }
}
