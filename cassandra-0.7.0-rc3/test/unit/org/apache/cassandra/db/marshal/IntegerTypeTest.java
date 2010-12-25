/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.marshal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.ComparisonFailure;
import org.junit.Test;

public class IntegerTypeTest
{
    private static void assertSignum(String message, int expected, double value)
    {
        int signum = (int)Math.signum(value);
        if (signum != expected)
            throw new ComparisonFailure(message, Integer.toString(expected), Integer.toString(signum));
    }

    private final IntegerType comparator = IntegerType.instance;

    @Test
    public void testTrimming()
    {
        ByteBuffer n1, n2;
        n1 = ByteBuffer.wrap(new byte[] {0});
        n2 = ByteBuffer.wrap(new byte[] {0, 0, 0, 0});
        assertEquals(0, comparator.compare(n1, n2));
        n1 = ByteBuffer.wrap(new byte[] {1, 0, 0, 1});
        n2 = ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0, 0, 1});
        assertEquals(0, comparator.compare(n1, n2));
        n1 = ByteBuffer.wrap(new byte[] {-1, 0, 0, -1 });
        n2 = ByteBuffer.wrap(new byte[] {-1, -1, -1, -1, 0, 0, -1});
        assertEquals(0, comparator.compare(n1, n2));
        n1 = ByteBuffer.wrap(new byte[] {-1, 0});
        n2 = ByteBuffer.wrap(new byte[] {0, -1, 0});
        assertSignum("", -1, comparator.compare(n1, n2));
        n1 = ByteBuffer.wrap(new byte[] {1, 0});
        n2 = ByteBuffer.wrap(new byte[] {0, -1, 0});
        assertSignum("", -1, comparator.compare(n1, n2));
    }

    @Test(expected = NullPointerException.class)
    public void testNullLeft()
    {
        comparator.compare(null, ByteBuffer.wrap(new byte[1]));
    }

    @Test(expected = NullPointerException.class)
    public void testNullRight()
    {
        comparator.compare(ByteBuffer.wrap(new byte[1]), null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullBoth()
    {
        comparator.compare(null, null);
    }

    @Test
    public void testZeroLengthArray()
    {
        assertSignum("0-1", -1, comparator.compare(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[1])));
        assertSignum("1-0", 1, comparator.compare(ByteBuffer.wrap(new byte[1]), ByteBuffer.wrap(new byte[0])));
        assertSignum("0-0", 0, comparator.compare(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0])));
    }

    @Test
    public void testSanity()
    {
        ByteBuffer nN = ByteBuffer.wrap(new byte[] {-1});
        ByteBuffer nZ = ByteBuffer.wrap(new byte[] {0});
        ByteBuffer nP = ByteBuffer.wrap(new byte[] {1});
        assertSignum("ZN", 1, comparator.compare(nZ, nN));
        assertSignum("NZ", -1, comparator.compare(nN, nZ));
        assertSignum("ZP", -1, comparator.compare(nZ, nP));
        assertSignum("PZ", 1, comparator.compare(nP, nZ));
        assertSignum("PN", 1, comparator.compare(nP, nN));
        assertSignum("NP", -1, comparator.compare(nN, nP));
    }

    @Test
    public void testSameLength()
    {
        ByteBuffer n1 = ByteBuffer.wrap(new byte[] {-2, 2, -4, -5});
        ByteBuffer n2 = ByteBuffer.wrap(new byte[] {-2, 3, -5, -4});
        ByteBuffer p1 = ByteBuffer.wrap(new byte[] {2, 3, -4, -5});
        ByteBuffer p2 = ByteBuffer.wrap(new byte[] {2, -2, -5, -4});

        assertSignum("n1n2", -1, comparator.compare(n1, n2));
        assertSignum("n2n1", 1, comparator.compare(n2, n1));

        assertSignum("p1p2", -1, comparator.compare(p1, p2));
        assertSignum("p2p1", 1, comparator.compare(p2, p1));

        assertSignum("p1n1", 1, comparator.compare(p1, n1));
        assertSignum("p1n2", 1, comparator.compare(p1, n2));
        assertSignum("n1p1", -1, comparator.compare(n1, p1));
        assertSignum("n2p1", -1, comparator.compare(n2, p1));
    }

    @Test
    public void testCommonPrefix()
    {
        ByteBuffer[] data = {
                ByteBuffer.wrap(new byte[]{1, 0, 0, 1}),
                ByteBuffer.wrap(new byte[]{1, 0, 0, 1, 0}),
                ByteBuffer.wrap(new byte[]{1, 0, 0, 1}),
                ByteBuffer.wrap(new byte[]{1, 0, 0, 1, 0}),
                ByteBuffer.wrap(new byte[]{-1, 0, 0, 1}),
                ByteBuffer.wrap(new byte[]{-1, 0, 0, 1, 0}),
                ByteBuffer.wrap(new byte[]{-1, 0, 0, 1}),
                ByteBuffer.wrap(new byte[]{-1, 0, 0, 1, 0})
        };

        Arrays.sort(data, comparator);
        assertArrayEquals(new byte[]{-1, 0, 0, 1, 0}, data[0].array());
        assertArrayEquals(new byte[]{-1, 0, 0, 1, 0},data[1].array());
        assertArrayEquals(new byte[]{-1, 0, 0, 1},data[2].array());
        assertArrayEquals(new byte[]{-1, 0, 0, 1},data[3].array());
        assertArrayEquals(new byte[]{1, 0, 0, 1},data[4].array());
        assertArrayEquals(new byte[]{1, 0, 0, 1},data[5].array());
        assertArrayEquals(new byte[]{1, 0, 0, 1, 0},data[6].array());
        assertArrayEquals(new byte[]{1, 0, 0, 1, 0},data[7].array());
    }

    @Test
    public void testSorting()
    {
        ByteBuffer[] data = {
                ByteBuffer.wrap(new byte[]{ 1, 0, 0, 0}),
                ByteBuffer.wrap(new byte[]{-2, 0, 0}),
                ByteBuffer.wrap(new byte[]{ 3, 0}),
                ByteBuffer.wrap(new byte[]{-4}),
                ByteBuffer.wrap(new byte[]{ 4}),
                ByteBuffer.wrap(new byte[]{-3, 0}),
                ByteBuffer.wrap(new byte[]{ 2, 0, 0}),
                ByteBuffer.wrap(new byte[]{-1, 0, 0, 0})
        };

        Arrays.sort(data, comparator);
        assertArrayEquals("-1", new byte[] {-1, 0, 0, 0}, data[0].array());
        assertArrayEquals("-2", new byte[] {-2, 0, 0}, data[1].array());
        assertArrayEquals("-3", new byte[] {-3, 0}, data[2].array());
        assertArrayEquals("-4", new byte[] {-4}, data[3].array());
        assertArrayEquals(" 4", new byte[] { 4}, data[4].array());
        assertArrayEquals(" 3", new byte[] { 3, 0}, data[5].array());
        assertArrayEquals(" 2", new byte[] { 2, 0, 0}, data[6].array());
        assertArrayEquals(" 1", new byte[] { 1, 0, 0, 0}, data[7].array());
    }

    @Test
    public void testSortingSpecialExtendedVersion()
    {
        Random rng = new Random(-9078270684023566599L);

        ByteBuffer[] data = new ByteBuffer[10000];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = ByteBuffer.allocate(rng.nextInt(32) + 1);
            rng.nextBytes(data[i].array());
        }

        Arrays.sort(data, comparator);

        for (int i = 1; i < data.length; i++)
        {
            BigInteger i0 = new BigInteger(data[i - 1].array());
            BigInteger i1 = new BigInteger(data[i].array());
            assertTrue("#" + i, i0.compareTo(i1) <= 0);
        }
    }
}
