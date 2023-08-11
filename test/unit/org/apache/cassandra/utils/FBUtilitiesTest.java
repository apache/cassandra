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
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FBUtilitiesTest
{

    public static final Logger LOGGER = LoggerFactory.getLogger(FBUtilitiesTest.class);

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
                Assert.assertEquals(partitioner.partitionOrdering(null), type);
            }
    }

    @Test
    public void testGetBroadcastNativeAddress() throws Exception
    {
        //When both rpc_address and broadcast_rpc_address are null, it should return the local address (from DD.applyAddressConfig)
        FBUtilities.reset();
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_address = null;
        testConfig.broadcast_rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(FBUtilities.getJustLocalAddress(), FBUtilities.getJustBroadcastNativeAddress());

        //When rpc_address is defined and broadcast_rpc_address is null, it should return the rpc_address
        FBUtilities.reset();
        testConfig.rpc_address = "127.0.0.2";
        testConfig.broadcast_rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(InetAddress.getByName("127.0.0.2"), FBUtilities.getJustBroadcastNativeAddress());

        //When both rpc_address and broadcast_rpc_address are defined, it should return broadcast_rpc_address
        FBUtilities.reset();
        testConfig.rpc_address = "127.0.0.2";
        testConfig.broadcast_rpc_address = "127.0.0.3";
        DatabaseDescriptor.applyAddressConfig(testConfig);
        assertEquals(InetAddress.getByName("127.0.0.3"), FBUtilities.getJustBroadcastNativeAddress());

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

    @Test
    public void testCamelToSnake()
    {
        AssertionError error = null;
        for (Pair<String, String> a : Arrays.asList(Pair.create("Testing", "testing"),
                                                    Pair.create("fooBarBaz", "foo_bar_baz"),
                                                    Pair.create("foo_bar_baz", "foo_bar_baz")
        ))
        {
            try
            {
                assertThat(FBUtilities.camelToSnake(a.left)).isEqualTo(a.right);
            }
            catch (AssertionError e)
            {
                if (error == null)
                    error = e;
                else
                    error.addSuppressed(e);
            }
        }
        if (error != null)
            throw error;
    }

    @Test
    public void testPrettyPrintAndParse()
    {
        String[] tests = new String[]{
        "1", "", "", "1",
        "1K", "", "", "1e3",
        "1 KiB", " ", "B", "1024",
        "10 B/s", " ", "B/s", "10",
        "10.2 MiB/s", null, "B/s", "10695475.2",
        "10e+5", "", "", "10e5",
        "10*2^20", "", "", "10485760",
        "1024*2^-10", "", "", "1",
        "1024 miB", " ", "B", "1",
        "1000000um", "", "m", "1",
        "10e+25s", "", "s", "10e25",
        "1.12345e-25", "", "", "1.12345e-25",
        "10e+45", "", "", "10e45",
        "1.12345e-45", "", "", "1.12345e-45",
        "55.3 garbage", null, null, "55.3",
        "0.00TiB", "", "B", "0",
        "-23", null, null, "-23",
        "-55 Gt", " ", "t", "-55e9",
        "-123e+3", null, null, "-123000",
        "-876ns", "", "s", "-876e-9",
        Long.toString(Long.MAX_VALUE), null, null, Long.toString(Long.MAX_VALUE),
        Long.toString(Long.MIN_VALUE), null, null, Long.toString(Long.MIN_VALUE),
        "Infinity Kg", " ", "Kg", "+Infinity",
        "NaN", "", "", "NaN",
        "-Infinity", "", "", "-Infinity",
        };

        for (int i = 0; i < tests.length; i += 4)
        {
            String v = tests[i];
            String sep = tests[i + 1];
            String unit = tests[i + 2];
            double exp = Double.parseDouble(tests[i+3]);
            String vBin = FBUtilities.prettyPrintBinary(exp, unit == null ? "" : unit, sep == null ? " " : sep);
            String vDec = FBUtilities.prettyPrintDecimal(exp, unit == null ? "w" : unit, sep == null ? "\t" : sep);
            LOGGER.info("{} binary {} decimal {} expected {}", v, vBin, vDec, exp);
            Assert.assertEquals(exp, FBUtilities.parseHumanReadable(v, sep, unit), getDelta(exp));
            Assert.assertEquals(exp, FBUtilities.parseHumanReadable(vBin, sep, unit), getDelta(exp));
            Assert.assertEquals(exp, FBUtilities.parseHumanReadable(vDec, sep, unit), getDelta(exp));

            if (((long) exp) == exp)
                Assert.assertEquals(exp,
                                    FBUtilities.parseHumanReadable(FBUtilities.prettyPrintMemory((long) exp),
                                                                   null,
                                                                   "B"),
                                    getDelta(exp));
        }
    }

    private static double getDelta(double exp)
    {
        return Math.max(0.001 * Math.abs(exp), 1e-305);
    }

    @Test
    public void testPrettyPrintAndParseRange()
    {
        String unit = "";
        String sep = "";
        for (int exp = -100; exp < 100; ++exp)
        {
            for (double base = -1.0; base <= 1.0; base += 0.12) // avoid hitting 0 exactly
            {
                for (boolean binary : new boolean[] {false, true})
                {
                    double value = binary
                                   ? Math.scalb(base, exp * 10)
                                   : base * Math.pow(10, exp);
                    String vBin = FBUtilities.prettyPrintBinary(value, unit, sep);
                    String vDec = FBUtilities.prettyPrintDecimal(value, unit, sep);
                    LOGGER.info("{} binary {} decimal {}", value, vBin, vDec);
                    Assert.assertEquals(value, FBUtilities.parseHumanReadable(vBin, sep, unit), getDelta(value));
                    Assert.assertEquals(value, FBUtilities.parseHumanReadable(vDec, sep, unit), getDelta(value));
                }
            }
        }
    }

    @Test
    public void testPrettyPrintAndParseRandom()
    {
        Random rand = new Random();
        String unit = "";
        String sep = "";
        for (int i = 0; i < 1000; ++i)
        {
            long bits = rand.nextLong();
            double value = Double.longBitsToDouble(bits);
            if (Double.isNaN(value))
                value = Double.NaN; // to avoid failures on non-bitwise-equal NaNs
            String vBin = FBUtilities.prettyPrintBinary(value, unit, sep);
            String vDec = FBUtilities.prettyPrintDecimal(value, unit, sep);
            LOGGER.info("{} binary {} decimal {}", value, vBin, vDec);
            Assert.assertEquals(value, FBUtilities.parseHumanReadable(vBin, sep, unit), getDelta(value));
            Assert.assertEquals(value, FBUtilities.parseHumanReadable(vDec, sep, unit), getDelta(value));
        }
    }
}
