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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class FastByteOperationsTest
{

    private static final FastByteOperations.PureJavaOperations PJO = new FastByteOperations.PureJavaOperations();
    private static final FastByteOperations.UnsafeOperations UO = new FastByteOperations.UnsafeOperations();
    private static final Random rand = new Random(0);
    private static final ByteBuffer dbuf1 = ByteBuffer.allocateDirect(150);
    private static final ByteBuffer dbuf2 = ByteBuffer.allocateDirect(150);
    private static final ByteBuffer hbuf1 = ByteBuffer.allocate(150);
    private static final ByteBuffer hbuf2 = ByteBuffer.allocate(150);

    @Test
    public void testFastByteCopy()
    {
        byte[] bytes1 = new byte[128];
        byte[] empty = new byte[128];
        rand.nextBytes(bytes1);
        testCopy(bytes1, wrap1(bytes1, true), wrap2(empty, true), PJO);
        testCopy(bytes1, wrap1(bytes1, true), wrap2(empty, false), PJO);
        testCopy(bytes1, wrap1(bytes1, false), wrap2(empty, true), PJO);
        testCopy(bytes1, wrap1(bytes1, false), wrap2(empty, false), PJO);
        testCopy(bytes1, wrap1(bytes1, true), wrap2(empty, true), UO);
        testCopy(bytes1, wrap1(bytes1, true), wrap2(empty, false), UO);
        testCopy(bytes1, wrap1(bytes1, false), wrap2(empty, true), UO);
        testCopy(bytes1, wrap1(bytes1, false), wrap2(empty, false), UO);
    }

    private void testCopy(byte[] canon, ByteBuffer src, ByteBuffer trg, FastByteOperations.ByteOperations ops)
    {
        byte[] result = new byte[src.remaining()];
        ops.copy(src, src.position(), trg, trg.position(), src.remaining());
        ops.copy(trg, trg.position(), result, 0, trg.remaining());
        assert firstdiff(canon, result) < 0;
    }

    private static int firstdiff(byte[] canon, byte[] test)
    {
        for (int i = 0 ; i < canon.length ; i++)
            if (canon[i] != test[i])
                return i;
        return -1;
    }

    @Test
    public void testFastByteComparisons()
    {
        byte[] bytes1 = new byte[128];
        for (int i = 0 ; i < 1000 ; i++)
        {
            rand.nextBytes(bytes1);
            for (int j = 0 ; j < 16 ; j++)
            {
                byte[] bytes2 = Arrays.copyOf(bytes1, bytes1.length - j);
                testTwiddleOneByteComparisons(bytes1, bytes2, 16, true, 1);
                testTwiddleOneByteComparisons(bytes1, bytes2, 16, true, -1);
                testTwiddleOneByteComparisons(bytes1, bytes2, 16, false, 1);
                testTwiddleOneByteComparisons(bytes1, bytes2, 16, false, -1);
                testTwiddleOneByteComparisons(bytes1, bytes2, 16, true, 128);
                testTwiddleOneByteComparisons(bytes1, bytes2, 16, false, 128);
            }
        }
    }

    private void testTwiddleOneByteComparisons(byte[] bytes1, byte[] bytes2, int count, boolean start, int inc)
    {
        for (int j = 0 ; j < count ; j++)
        {
            int index = start ? j : bytes2.length - (j + 1);
            bytes2[index] += inc;
            testComparisons(bytes1, bytes2);
            bytes2[index] -= inc;
        }
    }

    private static ByteBuffer wrap1(byte[] bytes, boolean direct)
    {
        return slice(bytes, direct ? dbuf1 : hbuf1);
    }

    private static ByteBuffer wrap2(byte[] bytes, boolean direct)
    {
        return slice(bytes, direct ? dbuf2 : hbuf2);
    }

    private static ByteBuffer slice(byte[] bytes, ByteBuffer buf)
    {
        buf = buf.duplicate();
        buf.position((buf.limit() - bytes.length) / 2);
        buf.limit(buf.position() + bytes.length);
        buf.duplicate().put(bytes);
        return buf;
    }

    private void testComparisons(byte[] bytes1, byte[] bytes2)
    {
        testComparison(bytes1, bytes2);
        testComparison(bytes2, bytes1);
        testComparison(wrap1(bytes1, false), bytes2);
        testComparison(wrap2(bytes2, false), bytes1);
        testComparison(wrap1(bytes1, false), wrap2(bytes2, false));
        testComparison(wrap2(bytes2, false), wrap1(bytes1, false));
        testComparison(wrap1(bytes1, true), bytes2);
        testComparison(wrap2(bytes2, true), bytes1);
        testComparison(wrap1(bytes1, true), wrap2(bytes2, true));
        testComparison(wrap2(bytes2, true), wrap1(bytes1, true));
        testComparison(wrap1(bytes1, true), wrap2(bytes2, false));
        testComparison(wrap1(bytes1, false), wrap2(bytes2, true));
        testComparison(wrap2(bytes2, true), wrap1(bytes1, false));
        testComparison(wrap2(bytes2, false), wrap1(bytes1, true));
    }

    private void testComparison(byte[] bytes1, byte[] bytes2)
    {
        assert sameComparisonResult(PJO.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length), UO.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length));
        assert sameComparisonResult(PJO.compare(bytes1, 10, bytes1.length - 10, bytes2, 10, bytes2.length - 10), UO.compare(bytes1, 10, bytes1.length - 10, bytes2, 10, bytes2.length - 10));
    }

    private void testComparison(ByteBuffer bytes1, byte[] bytes2)
    {
        assert sameComparisonResult(PJO.compare(bytes1, bytes2, 0, bytes2.length), UO.compare(bytes1, bytes2, 0, bytes2.length));
        assert sameComparisonResult(PJO.compare(bytes1, bytes2, 10, bytes2.length - 10), UO.compare(bytes1, bytes2, 10, bytes2.length - 10));
    }

    private void testComparison(ByteBuffer bytes1, ByteBuffer bytes2)
    {
        assert sameComparisonResult(PJO.compare(bytes1, bytes2), UO.compare(bytes1, bytes2));
    }

    static boolean sameComparisonResult(int exp, int act)
    {
        if (exp < 0)
            return act < 0;
        if (exp > 0)
            return act > 0;
        return act == 0;
    }
}
