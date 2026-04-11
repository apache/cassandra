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
package org.apache.cassandra.index.sai.cql.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

import static org.junit.Assert.assertTrue;

public class NumericTypeSortingTest extends SAIRandomizedTester
{
    @Test
    public void testBigDecimalEncoding()
    {
        BigDecimal[] data = new BigDecimal[10000];
        for (int i = 0; i < data.length; i++)
        {
            BigDecimal divider = new BigDecimal(getRandom().nextBigInteger(1000).add(BigInteger.ONE));
            BigDecimal randomNumber = new BigDecimal(getRandom().nextBigInteger(1000)).divide(divider, RoundingMode.HALF_DOWN);
            if (getRandom().nextBoolean())
                randomNumber = randomNumber.negate();

            data[i] = randomNumber;
        }

        IndexTermType indexTermType = createIndexTermType(DecimalType.instance);

        Arrays.sort(data, BigDecimal::compareTo);

        for (int i = 1; i < data.length; i++)
        {
            BigDecimal i0 = data[i - 1];
            BigDecimal i1 = data[i];
            assertTrue(i0 + " <= " + i1, i0.compareTo(i1) <= 0);

            ByteBuffer b0 = indexTermType.asIndexBytes(DecimalType.instance.decompose(i0));

            ByteBuffer b1 = indexTermType.asIndexBytes(DecimalType.instance.decompose(i1));

            assertTrue(i0 + " <= " + i1, indexTermType.compare(b0, b1) <= 0);
        }
    }

    @Test
    public void testBigIntEncoding()
    {
        BigInteger[] data = new BigInteger[10000];
        for (int i = 0; i < data.length; i++)
        {
            BigInteger divider = getRandom().nextBigInteger(1000).add(BigInteger.ONE);
            BigInteger randomNumber = getRandom().nextBigInteger(1000).divide(divider);
            if (getRandom().nextBoolean())
                randomNumber = randomNumber.negate();

            data[i] = randomNumber;
        }

        Arrays.sort(data, BigInteger::compareTo);

        IndexTermType indexTermType = createIndexTermType(IntegerType.instance);

        for (int i = 1; i < data.length; i++)
        {
            BigInteger i0 = data[i - 1];
            BigInteger i1 = data[i];
            assertTrue(i0 + " <= " + i1, i0.compareTo(i1) <= 0);

            ByteBuffer b0 = indexTermType.asIndexBytes(IntegerType.instance.decompose(i0));

            ByteBuffer b1 = indexTermType.asIndexBytes(IntegerType.instance.decompose(i1));

            assertTrue(i0 + " <= " + i1, indexTermType.compare(b0, b1) <= 0);
        }
    }
}
