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

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class DecimalTypeTest
{
    private static final String LOW = "12.34";
    private static final String HIGH = "34.5678";

    private static BigDecimal zero = new BigDecimal("0.0");
    private static BigDecimal minus = new BigDecimal("-1.000001");
    private static BigDecimal low = new BigDecimal(LOW);
    private static BigDecimal high = new BigDecimal(HIGH);

    @Test
    public void test1Decompose_compose()
    {
        ByteBuffer bb = DecimalType.instance.decompose(low);

        String string = DecimalType.instance.compose(bb).toPlainString();

        // check that the decomposed buffer when re-composed is equal to the initial string.
        assertEquals(LOW, string);

        // check that a null argument yields an empty byte buffer
        bb = DecimalType.instance.decompose(null);
        assertEquals(bb, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Test
    public void test2Compare()
    {
        ByteBuffer lowBB = DecimalType.instance.decompose(low);
        ByteBuffer low2BB = DecimalType.instance.decompose(low);
        ByteBuffer highBB = DecimalType.instance.decompose(high);
        assertEquals(-1, DecimalType.instance.compare(lowBB, highBB));

        lowBB = DecimalType.instance.decompose(low);
        highBB = DecimalType.instance.decompose(high);
        assertEquals(1, DecimalType.instance.compare(highBB, lowBB));

        lowBB = DecimalType.instance.decompose(low);
        assertEquals(0, DecimalType.instance.compare(low2BB, lowBB));

        lowBB = DecimalType.instance.decompose(low);
        assertEquals(-1, DecimalType.instance.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, lowBB));

        lowBB = DecimalType.instance.decompose(low);
        assertEquals(1, DecimalType.instance.compare(lowBB,ByteBufferUtil.EMPTY_BYTE_BUFFER));

        assertEquals(0, DecimalType.instance.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER,ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    @Test
    public void test3Sort()
    {
        ByteBuffer zeroBB = DecimalType.instance.decompose(zero);
        ByteBuffer minusBB = DecimalType.instance.decompose(minus);
        ByteBuffer lowBB = DecimalType.instance.decompose(low);
        ByteBuffer highBB = DecimalType.instance.decompose(high);

        ByteBuffer[] array = {highBB,minusBB,lowBB,lowBB,zeroBB,minusBB};

        // Sort the array of ByteBuffer using a DecimalType comparator
        Arrays.sort(array, DecimalType.instance);

        // Check that the array is in order
        for (int i = 1; i < array.length; i++)
        {
            BigDecimal i0 = DecimalType.instance.compose(array[i - 1]);
            BigDecimal i1 = DecimalType.instance.compose(array[i]);
            assertTrue("#" + i, i0.compareTo(i1) <= 0);
        }
    }
}
