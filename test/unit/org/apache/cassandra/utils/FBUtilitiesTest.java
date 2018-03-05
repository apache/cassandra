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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FBUtilitiesTest
{
    @Test
    public void testCompareByteSubArrays()
    {
        ByteBuffer bytes = ByteBuffer.allocate(16);

        // handle null
        assert ByteBufferUtil.compareSubArrays(
                null, 0, null, 0, 0) == 0;
        assert ByteBufferUtil.compareSubArrays(
                null, 0, ByteBufferUtil.bytes(524255231), 0, 4) == -1;
        assert ByteBufferUtil.compareSubArrays(
                ByteBufferUtil.bytes(524255231), 0, null, 0, 4) == 1;

        // handle comparisons
        System.arraycopy(Ints.toByteArray(524255231), 0, bytes.array(), 3, 4);
        assert ByteBufferUtil.compareSubArrays(
                bytes, 3, ByteBufferUtil.bytes(524255231), 0, 4) == 0;
        assert ByteBufferUtil.compareSubArrays(
                bytes, 3, ByteBufferUtil.bytes(524255232), 0, 4) == -1;
        assert ByteBufferUtil.compareSubArrays(
                bytes, 3, ByteBufferUtil.bytes(524255230), 0, 4) == 1;

        // check that incorrect length throws exception
        try
        {
            assert ByteBufferUtil.compareSubArrays(
                    bytes, 3, ByteBufferUtil.bytes(524255231), 0, 24) == 0;
            fail("Should raise an AssertionError.");
        } catch (AssertionError ae)
        {
        }
        try
        {
            assert ByteBufferUtil.compareSubArrays(
                    bytes, 3, ByteBufferUtil.bytes(524255231), 0, 12) == 0;
            fail("Should raise an AssertionError.");
        } catch (AssertionError ae)
        {
        }
    }

    @Test
    public void testToString()
    {
        // null turns to empty string
        assertEquals("", FBUtilities.toString(null));
        Map<String, String> map = new TreeMap<>();
        // empty map turns to empty string
        assertEquals("", FBUtilities.toString(map));
        map.put("aaa", "bbb");
        assertEquals("aaa:bbb", FBUtilities.toString(map));
        map.put("ccc", "ddd");
        assertEquals("aaa:bbb, ccc:ddd", FBUtilities.toString(map));
    }

    @Test(expected=CharacterCodingException.class)
    public void testDecode() throws IOException
    {
        ByteBuffer bytes = ByteBuffer.wrap(new byte[]{(byte)0xff, (byte)0xfe});
        ByteBufferUtil.string(bytes, StandardCharsets.UTF_8);
    }

    private static void assertPartitioner(String name, Class expected)
    {
        Assert.assertTrue(String.format("%s != %s", name, expected.toString()),
                          expected.isInstance(FBUtilities.newPartitioner(name)));
    }

    /**
     * Check that given a name, the correct partitioner instance is created.
     *
     * If the assertions in this test start failing, it likely means the sstabledump/sstablemetadata tools will
     * also fail to read existing sstables.
     */
    @Test
    public void testNewPartitionerNoArgConstructors()
    {
        assertPartitioner("ByteOrderedPartitioner", ByteOrderedPartitioner.class);
        assertPartitioner("LengthPartitioner", LengthPartitioner.class);
        assertPartitioner("Murmur3Partitioner", Murmur3Partitioner.class);
        assertPartitioner("OrderPreservingPartitioner", OrderPreservingPartitioner.class);
        assertPartitioner("RandomPartitioner", RandomPartitioner.class);
        assertPartitioner("org.apache.cassandra.dht.ByteOrderedPartitioner", ByteOrderedPartitioner.class);
        assertPartitioner("org.apache.cassandra.dht.LengthPartitioner", LengthPartitioner.class);
        assertPartitioner("org.apache.cassandra.dht.Murmur3Partitioner", Murmur3Partitioner.class);
        assertPartitioner("org.apache.cassandra.dht.OrderPreservingPartitioner", OrderPreservingPartitioner.class);
        assertPartitioner("org.apache.cassandra.dht.RandomPartitioner", RandomPartitioner.class);
    }

    /**
     * Check that we can instantiate local partitioner correctly and that we can pass the correct type
     * to it as a constructor argument.
     *
     * If the assertions in this test start failing, it likely means the sstabledump/sstablemetadata tools will
     * also fail to read existing sstables.
     */
    @Test
    public void testNewPartitionerLocalPartitioner()
    {
        for (String name : new String[] {"LocalPartitioner", "org.apache.cassandra.dht.LocalPartitioner"})
            for (AbstractType<?> type : new AbstractType<?>[] {UUIDType.instance, ListType.getInstance(Int32Type.instance, true)})
            {
                IPartitioner partitioner = FBUtilities.newPartitioner(name, Optional.of(type));
                Assert.assertTrue(String.format("%s != LocalPartitioner", partitioner.toString()),
                                  LocalPartitioner.class.isInstance(partitioner));
                Assert.assertEquals(partitioner.partitionOrdering(), type);
            }
    }

    @Test
    public void testGetBroadcastRpcAddress() throws Exception
    {
        //When both rpc_address and broadcast_rpc_address are null, it should return the local address (from DD.applyAddressConfig)
        FBUtilities.reset();
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_address = null;
        testConfig.broadcast_rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(FBUtilities.getLocalAddress(), FBUtilities.getBroadcastRpcAddress());

        //When rpc_address is defined and broadcast_rpc_address is null, it should return the rpc_address
        FBUtilities.reset();
        testConfig.rpc_address = "127.0.0.2";
        testConfig.broadcast_rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(InetAddress.getByName("127.0.0.2"), FBUtilities.getBroadcastRpcAddress());

        //When both rpc_address and broadcast_rpc_address are defined, it should return broadcast_rpc_address
        FBUtilities.reset();
        testConfig.rpc_address = "127.0.0.2";
        testConfig.broadcast_rpc_address = "127.0.0.3";
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(InetAddress.getByName("127.0.0.3"), FBUtilities.getBroadcastRpcAddress());

        FBUtilities.reset();
    }

    @Test
    public void testWaitFirstFuture() throws ExecutionException, InterruptedException
    {

        ExecutorService executor = Executors.newFixedThreadPool(4);
        FBUtilities.reset();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 4; i >= 1; i--)
        {
            final int sleep = i * 10;
            futures.add(executor.submit(() -> { TimeUnit.MILLISECONDS.sleep(sleep); return sleep; }));
        }
        Future<?> fut = FBUtilities.waitOnFirstFuture(futures, 3);
        int futSleep = (Integer) fut.get();
        assertEquals(futSleep, 10);
        futures.remove(fut);
        fut = FBUtilities.waitOnFirstFuture(futures, 3);
        futSleep = (Integer) fut.get();
        assertEquals(futSleep, 20);
        futures.remove(fut);
        fut = FBUtilities.waitOnFirstFuture(futures, 3);
        futSleep = (Integer) fut.get();
        assertEquals(futSleep, 30);
        futures.remove(fut);
        fut = FBUtilities.waitOnFirstFuture(futures, 3);
        futSleep = (Integer) fut.get();
        assertEquals(futSleep, 40);
    }

}
