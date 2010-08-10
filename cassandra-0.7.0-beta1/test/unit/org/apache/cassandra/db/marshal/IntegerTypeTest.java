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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import org.junit.ComparisonFailure;
import org.junit.Test;

import static org.junit.Assert.*;

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
        byte[] n1, n2;
        n1 = new byte[] {0};
        n2 = new byte[] {0, 0, 0, 0};
        assertEquals(0, comparator.compare(n1, n2));
        n1 = new byte[] {1, 0, 0, 1};
        n2 = new byte[] {0, 0, 0, 1, 0, 0, 1};
        assertEquals(0, comparator.compare(n1, n2));
        n1 = new byte[] {-1, 0, 0, -1 };
        n2 = new byte[] {-1, -1, -1, -1, 0, 0, -1};
        assertEquals(0, comparator.compare(n1, n2));
        n1 = new byte[] {-1, 0};
        n2 = new byte[] {0, -1, 0};
        assertSignum("", -1, comparator.compare(n1, n2));
        n1 = new byte[] {1, 0};
        n2 = new byte[] {0, -1, 0};
        assertSignum("", -1, comparator.compare(n1, n2));
    }

    @Test(expected = NullPointerException.class)
    public void testNullLeft()
    {
        comparator.compare(null, new byte[1]);
    }

    @Test(expected = NullPointerException.class)
    public void testNullRight()
    {
        comparator.compare(new byte[1], null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullBoth()
    {
        comparator.compare(null, null);
    }

    @Test
    public void testZeroLengthArray()
    {
        assertSignum("0-1", -1, comparator.compare(new byte[0], new byte[1]));
        assertSignum("1-0", 1, comparator.compare(new byte[1], new byte[0]));
        assertSignum("0-0", 0, comparator.compare(new byte[0], new byte[0]));
    }

    @Test
    public void testSanity()
    {
        byte[] nN = new byte[] {-1};
        byte[] nZ = new byte[] {0};
        byte[] nP = new byte[] {1};
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
        byte[] n1 = new byte[] {-2, 2, -4, -5};
        byte[] n2 = new byte[] {-2, 3, -5, -4};
        byte[] p1 = new byte[] {2, 3, -4, -5};
        byte[] p2 = new byte[] {2, -2, -5, -4};

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
        byte[][] data = {
                {1, 0, 0, 1},
                {1, 0, 0, 1, 0},
                {1, 0, 0, 1},
                {1, 0, 0, 1, 0},
                {-1, 0, 0, 1},
                {-1, 0, 0, 1, 0},
                {-1, 0, 0, 1},
                {-1, 0, 0, 1, 0}
        };

        Arrays.sort(data, comparator);
        assertArrayEquals(new byte[]{-1, 0, 0, 1, 0}, data[0]);
        assertArrayEquals(new byte[]{-1, 0, 0, 1, 0}, data[1]);
        assertArrayEquals(new byte[]{-1, 0, 0, 1}, data[2]);
        assertArrayEquals(new byte[]{-1, 0, 0, 1}, data[3]);
        assertArrayEquals(new byte[]{1, 0, 0, 1}, data[4]);
        assertArrayEquals(new byte[]{1, 0, 0, 1}, data[5]);
        assertArrayEquals(new byte[]{1, 0, 0, 1, 0}, data[6]);
        assertArrayEquals(new byte[]{1, 0, 0, 1, 0}, data[7]);
    }

    @Test
    public void testSorting()
    {
        byte[][] data = {
                { 1, 0, 0, 0},
                {-2, 0, 0},
                { 3, 0},
                {-4},
                { 4},
                {-3, 0},
                { 2, 0, 0},
                {-1, 0, 0, 0}
        };

        Arrays.sort(data, comparator);
        assertArrayEquals("-1", new byte[] {-1, 0, 0, 0}, data[0]);
        assertArrayEquals("-2", new byte[] {-2, 0, 0}, data[1]);
        assertArrayEquals("-3", new byte[] {-3, 0}, data[2]);
        assertArrayEquals("-4", new byte[] {-4}, data[3]);
        assertArrayEquals(" 4", new byte[] { 4}, data[4]);
        assertArrayEquals(" 3", new byte[] { 3, 0}, data[5]);
        assertArrayEquals(" 2", new byte[] { 2, 0, 0}, data[6]);
        assertArrayEquals(" 1", new byte[] { 1, 0, 0, 0}, data[7]);
    }

    @Test
    public void testSortingSpecialExtendedVersion()
    {
        Random rng = new Random(-9078270684023566599L);

        byte[][] data = new byte[10000][];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = new byte[rng.nextInt(32) + 1];
            rng.nextBytes(data[i]);
        }

        Arrays.sort(data, comparator);

        for (int i = 1; i < data.length; i++)
        {
            BigInteger i0 = new BigInteger(data[i - 1]);
            BigInteger i1 = new BigInteger(data[i]);
            assertTrue("#" + i, i0.compareTo(i1) <= 0);
        }
    }
}
