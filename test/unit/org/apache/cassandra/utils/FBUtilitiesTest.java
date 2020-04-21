/**
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

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
        final int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try
        {
            List<Future<?>> futures = new ArrayList<>(threadCount);
            List<CountDownLatch> latches = new ArrayList<>(threadCount);

            for (int i = 0; i < threadCount; i++)
            {
                CountDownLatch latch = new CountDownLatch(1);
                latches.add(latch);
                int finalI = i;
                futures.add(executor.submit(() -> {
                    latch.await(10, TimeUnit.SECONDS);
                    // Sleep to emulate "work" done by the future to make it not return immediately
                    // after counting down the latch in order to test for delay and spinning done
                    // in FBUtilities#waitOnFirstFuture.
                    TimeUnit.MILLISECONDS.sleep(10);
                    return latch.getCount() == 0 ? finalI : -1;
                }));
            }

            for (int i = 0; i < threadCount; i++)
            {
                latches.get(i).countDown();
                Future<?> fut = FBUtilities.waitOnFirstFuture(futures, 3);
                int futSleep = (Integer) fut.get();
                assertEquals(futSleep, i);
                futures.remove(fut);
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

}
