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
package org.apache.cassandra.db;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CompoundDenseCellNameType;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;

import static org.apache.cassandra.db.composites.CellNames.compositeDense;
import static org.apache.cassandra.db.composites.CellNames.compositeSparse;
import static org.apache.cassandra.db.composites.CellNames.simpleDense;
import static org.apache.cassandra.db.composites.CellNames.simpleSparse;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class NativeCellTest
{

    private static final NativeAllocator nativeAllocator = new NativePool(Integer.MAX_VALUE, Integer.MAX_VALUE, 1f, null).newAllocator();
    private static final OpOrder.Group group = new OpOrder().start();

    static class Name
    {
        final CellName name;
        final CellNameType type;
        Name(CellName name, CellNameType type)
        {
            this.name = name;
            this.type = type;
        }
    }

    static ByteBuffer[] bytess(String ... strings)
    {
        ByteBuffer[] r = new ByteBuffer[strings.length];
        for (int i = 0 ; i < r.length ; i++)
            r[i] = bytes(strings[i]);
        return r;
    }

    final static Name[] TESTS = new Name[]
                          {
                              new Name(simpleDense(bytes("a")), new SimpleDenseCellNameType(UTF8Type.instance)),
                              new Name(simpleSparse(new ColumnIdentifier("a", true)), new SimpleSparseCellNameType(UTF8Type.instance)),
                              new Name(compositeDense(bytes("a"), bytes("b")), new CompoundDenseCellNameType(Arrays.<AbstractType<?>>asList(UTF8Type.instance, UTF8Type.instance))),
                              new Name(compositeSparse(bytess("b", "c"), new ColumnIdentifier("a", true), false), new CompoundSparseCellNameType(Arrays.<AbstractType<?>>asList(UTF8Type.instance, UTF8Type.instance))),
                              new Name(compositeSparse(bytess("b", "c"), new ColumnIdentifier("a", true), true), new CompoundSparseCellNameType(Arrays.<AbstractType<?>>asList(UTF8Type.instance, UTF8Type.instance)))
                          };

    private static final CFMetaData metadata = new CFMetaData("", "", ColumnFamilyType.Standard, null);
    static
    {
        try
        {
            metadata.addColumnDefinition(new ColumnDefinition(null, null, new ColumnIdentifier("a", true), UTF8Type.instance, null, null, null, null, null));
        }
        catch (ConfigurationException e)
        {
            throw new AssertionError();
        }
    }

    @Test
    public void testCells() throws IOException
    {
        Random rand = ThreadLocalRandom.current();
        for (Name test : TESTS)
        {
            byte[] bytes = new byte[16];
            rand.nextBytes(bytes);

            // test regular Cell
            Cell buf, nat;
            buf = new BufferCell(test.name, ByteBuffer.wrap(bytes), rand.nextLong());
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);

            // test DeletedCell
            buf = new BufferDeletedCell(test.name, rand.nextInt(100000), rand.nextLong());
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);

            // test ExpiringCell
            buf = new BufferExpiringCell(test.name, ByteBuffer.wrap(bytes), rand.nextLong(),  rand.nextInt(100000));
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);

            // test CounterCell
            buf = new BufferCounterCell(test.name, CounterContext.instance().createLocal(rand.nextLong()), rand.nextLong(),  rand.nextInt(100000));
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);
        }
    }


    @Test
    public void testComparator()
    {

        Random rand = ThreadLocalRandom.current();
        for (Name test : TESTS)
        {
            byte[] bytes = new byte[7];
            byte[] bytes2 = new byte[7];
            rand.nextBytes(bytes);
            rand.nextBytes(bytes2);

            // test regular Cell
            Cell buf, nat, buf2, nat2;
            buf = new BufferCell(test.name, ByteBuffer.wrap(bytes), rand.nextLong());
            nat = buf.localCopy(metadata, nativeAllocator, group);

            buf2 = new BufferCell(test.name, ByteBuffer.wrap(bytes2), rand.nextLong());
            nat2 = buf2.localCopy(metadata, nativeAllocator, group);

            assert test.type.compare(buf.name(), nat.name()) == 0;
            assert test.type.compare(buf2.name(), nat2.name()) == 0;

            int val = test.type.compare(buf.name(), buf2.name());
            assert test.type.compare(nat.name(), nat2.name()) == val;
            assert test.type.compare(nat.name(), buf2.name()) == val;
            assert test.type.compare(buf.name(), nat2.name()) == val;


            // test DeletedCell
            buf = new BufferDeletedCell(test.name, rand.nextInt(100000), rand.nextLong());
            nat = buf.localCopy(metadata, nativeAllocator, group);
            buf2 = new BufferDeletedCell(test.name, rand.nextInt(100000), rand.nextLong());
            nat2 = buf2.localCopy(metadata, nativeAllocator, group);

            assert test.type.compare(buf.name(), nat.name()) == 0;
            assert test.type.compare(buf2.name(), nat2.name()) == 0;

            val = test.type.compare(buf.name(), buf2.name());
            assert test.type.compare(nat.name(), nat2.name()) == val;
            assert test.type.compare(nat.name(), buf2.name()) == val;
            assert test.type.compare(buf.name(), nat2.name()) == val;



            // test ExpiringCell
            buf = new BufferExpiringCell(test.name, ByteBuffer.wrap(bytes), rand.nextLong(),  rand.nextInt(100000));
            nat = buf.localCopy(metadata, nativeAllocator, group);

            buf2 = new BufferExpiringCell(test.name, ByteBuffer.wrap(bytes2), rand.nextLong(),  rand.nextInt(100000));
            nat2 = buf2.localCopy(metadata, nativeAllocator, group);

            assert test.type.compare(buf.name(), nat.name()) == 0;
            assert test.type.compare(buf2.name(), nat2.name()) == 0;

            val = test.type.compare(buf.name(), buf2.name());
            assert test.type.compare(nat.name(), nat2.name()) == val;
            assert test.type.compare(nat.name(), buf2.name()) == val;
            assert test.type.compare(buf.name(), nat2.name()) == val;


            // test CounterCell
            buf = new BufferCounterCell(test.name, CounterContext.instance().createLocal(rand.nextLong()), rand.nextLong(),  rand.nextInt(100000));
            nat = buf.localCopy(metadata, nativeAllocator, group);

            buf2 = new BufferCounterCell(test.name, CounterContext.instance().createLocal(rand.nextLong()), rand.nextLong(),  rand.nextInt(100000));
            nat2 = buf2.localCopy(metadata, nativeAllocator, group);

            assert test.type.compare(buf.name(), nat.name()) == 0;
            assert test.type.compare(buf2.name(), nat2.name()) == 0;

            val = test.type.compare(buf.name(), buf2.name());
            assert test.type.compare(nat.name(), nat2.name()) == val;
            assert test.type.compare(nat.name(), buf2.name()) == val;
            assert test.type.compare(buf.name(), nat2.name()) == val;

        }
    }

    static void test(Name test, Cell buf, Cell nat) throws IOException
    {
        Assert.assertTrue(buf.equals(nat));
        Assert.assertTrue(nat.equals(buf));
        Assert.assertTrue(buf.equals(buf));
        Assert.assertTrue(nat.equals(nat));

        try
        {
            MessageDigest d1 = MessageDigest.getInstance("MD5");
            MessageDigest d2 = MessageDigest.getInstance("MD5");
            buf.updateDigest(d1);
            nat.updateDigest(d2);
            Assert.assertArrayEquals(d1.digest(), d2.digest());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalStateException(e);
        }

        byte[] serialized;
        try (DataOutputBuffer bufOut = new DataOutputBuffer())
        {
            test.type.columnSerializer().serialize(nat, bufOut);
            serialized = bufOut.getData();
        }

        ByteArrayInputStream bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        Cell deserialized = test.type.columnSerializer().deserialize(new DataInputStream(bufIn));
        Assert.assertTrue(buf.equals(deserialized));

    }



}
