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

package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertEquals;

public class MathFctsTest
{
    @Test
    public void testAbs()
    {
        assertAbsEquals(1, Int32Type.instance, 1);
        assertAbsEquals(0, Int32Type.instance, 0);
        assertAbsEquals(3, Int32Type.instance, -3);

        assertAbsEquals((byte) 1, ByteType.instance, (byte) 1);
        assertAbsEquals((byte) 0, ByteType.instance, (byte) 0);
        assertAbsEquals((byte) 3, ByteType.instance, (byte) -3);

        assertAbsEquals((short) 1, ShortType.instance, (short) 1);
        assertAbsEquals((short) 0, ShortType.instance, (short) 0);
        assertAbsEquals((short) 3, ShortType.instance, (short) -3);

        assertAbsEquals(1F, FloatType.instance, 1F);
        assertAbsEquals(1.5F, FloatType.instance, 1.5F);
        assertAbsEquals(0F, FloatType.instance, 0F);
        assertAbsEquals(3F, FloatType.instance, -3F);
        assertAbsEquals(3.7F, FloatType.instance, -3.7F);

        assertAbsEquals(1L, LongType.instance, 1L);
        assertAbsEquals(0L, LongType.instance, 0L);
        assertAbsEquals(3L, LongType.instance, -3L);

        assertAbsEquals(1L, CounterColumnType.instance, 1L);
        assertAbsEquals(0L, CounterColumnType.instance, 0L);
        assertAbsEquals(3L, CounterColumnType.instance, -3L);

        assertAbsEquals(1.0, DoubleType.instance, 1.0);
        assertAbsEquals(1.5, DoubleType.instance, 1.5);
        assertAbsEquals(0.0, DoubleType.instance, 0.0);
        assertAbsEquals(3.0, DoubleType.instance, -3.0);
        assertAbsEquals(3.7, DoubleType.instance, -3.7);

        assertAbsEquals(BigInteger.ONE, IntegerType.instance, BigInteger.ONE);
        assertAbsEquals(BigInteger.ZERO, IntegerType.instance, BigInteger.ZERO);
        assertAbsEquals(BigInteger.valueOf(3), IntegerType.instance, BigInteger.valueOf(-3));

        assertAbsEquals(BigDecimal.ONE, DecimalType.instance, BigDecimal.ONE);
        assertAbsEquals(BigDecimal.valueOf(1.5), DecimalType.instance, BigDecimal.valueOf(1.5));
        assertAbsEquals(BigDecimal.ZERO, DecimalType.instance, BigDecimal.ZERO);
        assertAbsEquals(BigDecimal.valueOf(3), DecimalType.instance, BigDecimal.valueOf(-3));
        assertAbsEquals(BigDecimal.valueOf(3.7), DecimalType.instance, BigDecimal.valueOf(-3.7));
    }

    private <T extends Number> void assertAbsEquals(T expected, NumberType<T> inputType, T inputValue)
    {
        assertFctEquals(MathFcts.absFct(inputType), expected, inputType, inputValue);
    }

    @Test
    public void testExp()
    {
        assertExpEquals((int) Math.pow(Math.E, 5), Int32Type.instance, 5);
        assertExpEquals((int) Math.E, Int32Type.instance, 1);
        assertExpEquals(1, Int32Type.instance, 0);
        assertExpEquals((int) Math.pow(Math.E, -1), Int32Type.instance, -1);

        assertExpEquals((byte) Math.pow(Math.E, 5), ByteType.instance, (byte) 5);
        assertExpEquals((byte) Math.E, ByteType.instance, (byte) 1);
        assertExpEquals((byte) 1, ByteType.instance, (byte) 0);
        assertExpEquals((byte) Math.pow(Math.E, -1), ByteType.instance, (byte) -1);

        assertExpEquals((short) Math.pow(Math.E, 5), ShortType.instance, (short) 5);
        assertExpEquals((short) Math.E, ShortType.instance, (short) 1);
        assertExpEquals((short) 1, ShortType.instance, (short) 0);
        assertExpEquals((short) Math.pow(Math.E, -1), ShortType.instance, (short) -1);

        assertExpEquals((float) Math.pow(Math.E, 5), FloatType.instance, 5F);
        assertExpEquals((float) Math.E, FloatType.instance, 1F);
        assertExpEquals(1F, FloatType.instance, 0F);
        assertExpEquals((float) Math.pow(Math.E, -1), FloatType.instance, -1F);
        assertExpEquals((float) Math.pow(Math.E, -2.5), FloatType.instance, -2.5F);

        assertExpEquals((long) Math.pow(Math.E, 5), LongType.instance, 5L);
        assertExpEquals((long) Math.E, LongType.instance, 1L);
        assertExpEquals(1L, LongType.instance, 0L);
        assertExpEquals((long) Math.pow(Math.E, -1), LongType.instance, -1L);

        assertExpEquals((long) Math.pow(Math.E, 5), CounterColumnType.instance, 5L);
        assertExpEquals((long) Math.E, CounterColumnType.instance, 1L);
        assertExpEquals(1L, CounterColumnType.instance, 0L);
        assertExpEquals((long) Math.pow(Math.E, -1), CounterColumnType.instance, -1L);

        assertExpEquals(BigInteger.valueOf((long) Math.pow(Math.E, 5)), IntegerType.instance, BigInteger.valueOf(5));
        assertExpEquals(BigInteger.valueOf((long) Math.E), IntegerType.instance, BigInteger.valueOf(1));
        assertExpEquals(BigInteger.valueOf(1), IntegerType.instance, BigInteger.valueOf(0));
        assertExpEquals(BigInteger.valueOf((long) Math.pow(Math.E, -1)), IntegerType.instance, BigInteger.valueOf(-1));

        assertExpEquals(new BigDecimal("148.41315910257660342111558004055"), DecimalType.instance, BigDecimal.valueOf(5));
        assertExpEquals(new BigDecimal("2.7182818284590452353602874713527"), DecimalType.instance, BigDecimal.valueOf(1));
        assertExpEquals(new BigDecimal("1"), DecimalType.instance, BigDecimal.valueOf(0));
        assertExpEquals(new BigDecimal("0.36787944117144232159552377016146"), DecimalType.instance, BigDecimal.valueOf(-1));
    }

    private <T extends Number> void assertExpEquals(T expected, NumberType<T> inputType, T inputValue)
    {
        assertFctEquals(MathFcts.expFct(inputType), expected, inputType, inputValue);
    }



    @Test
    public void testLog()
    {
        assertLogEquals((int) Math.log(5), Int32Type.instance, 5);
        assertLogEquals(0, Int32Type.instance, (int) Math.E);
        assertLogEquals(0, Int32Type.instance, 1);
        assertLogEquals((int) Math.log(-1), Int32Type.instance, -1);

        assertLogEquals((byte) Math.log(5), ByteType.instance, (byte) 5);
        assertLogEquals((byte) 0, ByteType.instance, (byte) Math.E);
        assertLogEquals((byte) 0, ByteType.instance, (byte) 1);
        assertLogEquals((byte) Math.log(-1), ByteType.instance, (byte) -1);

        assertLogEquals((short) Math.log(5), ShortType.instance, (short) 5);
        assertLogEquals((short) 0, ShortType.instance, (short) Math.E);
        assertLogEquals((short) 0, ShortType.instance, (short) 1);
        assertLogEquals((short) Math.log(-1), ShortType.instance, (short) -1);

        assertLogEquals((float) Math.log(5.5F), FloatType.instance, 5.5F, 0.0000001);
        assertLogEquals(1F, FloatType.instance, (float) Math.E, 0.0000001);
        assertLogEquals(0F, FloatType.instance, 1F, 0.0000001);
        assertLogEquals((float) Math.log(-1F), FloatType.instance, -1F, 0.0000001);


        assertLogEquals((long) Math.log(5), LongType.instance, 5L);
        assertLogEquals(0L, LongType.instance, (long) Math.E);
        assertLogEquals(0L, LongType.instance, 1L);
        assertLogEquals((long) Math.log(-1), LongType.instance, -1L);

        assertLogEquals((long) Math.log(5), CounterColumnType.instance, 5L);
        assertLogEquals(0L, CounterColumnType.instance, (long) Math.E);
        assertLogEquals(0L, CounterColumnType.instance, 1L);
        assertLogEquals((long) Math.log(-1), CounterColumnType.instance, -1L);

        assertLogEquals(BigInteger.valueOf((long) Math.log(5)), IntegerType.instance, BigInteger.valueOf(5));
        assertLogEquals(BigInteger.valueOf(0), IntegerType.instance, BigInteger.valueOf((long) Math.E));
        assertLogEquals(BigInteger.valueOf(0), IntegerType.instance, BigInteger.valueOf(1));

        assertLogEquals(new BigDecimal("1.6094379124341003746007593332262"), DecimalType.instance, BigDecimal.valueOf(5));
        assertLogEquals(new BigDecimal("0"), DecimalType.instance, BigDecimal.valueOf(1));
    }

    private <T extends Number> void assertLogEquals(T expected, NumberType<T> inputType, T inputValue)
    {
        assertFctEquals(MathFcts.logFct(inputType), expected, inputType, inputValue);
    }

    private <T extends Number> void assertLogEquals(T expected, NumberType<T> inputType, T inputValue, double delta)
    {
        assertFctEquals(MathFcts.logFct(inputType), expected, inputType, inputValue, delta);
    }
    
    @Test
    public void testLog10()
    {
        assertLog10Equals((int) Math.log10(5), Int32Type.instance, 5);
        assertLog10Equals(0, Int32Type.instance, (int) Math.E);
        assertLog10Equals(0, Int32Type.instance, 1);
        assertLog10Equals((int) Math.log10(-1), Int32Type.instance, -1);

        assertLog10Equals((byte) Math.log10(5), ByteType.instance, (byte) 5);
        assertLog10Equals((byte) 0, ByteType.instance, (byte) Math.E);
        assertLog10Equals((byte) 0, ByteType.instance, (byte) 1);
        assertLog10Equals((byte) Math.log10(-1), ByteType.instance, (byte) -1);

        assertLog10Equals((short) Math.log10(5), ShortType.instance, (short) 5);
        assertLog10Equals((short) 0, ShortType.instance, (short) Math.E);
        assertLog10Equals((short) 0, ShortType.instance, (short) 1);
        assertLog10Equals((short) Math.log10(-1), ShortType.instance, (short) -1);

        assertLog10Equals((float) Math.log10(5.5F), FloatType.instance, 5.5F, 0.0000001);
        assertLog10Equals(1F, FloatType.instance, 10F, 0.0000001);
        assertLog10Equals(0F, FloatType.instance, 1F, 0.0000001);
        assertLog10Equals((float) Math.log10(-1F), FloatType.instance, -1F, 0.0000001);


        assertLog10Equals((long) Math.log10(5), LongType.instance, 5L);
        assertLog10Equals(0L, LongType.instance, (long) Math.E);
        assertLog10Equals(0L, LongType.instance, 1L);
        assertLog10Equals((long) Math.log10(-1), LongType.instance, -1L);

        assertLog10Equals((long) Math.log10(5), CounterColumnType.instance, 5L);
        assertLog10Equals(0L, CounterColumnType.instance, (long) Math.E);
        assertLog10Equals(0L, CounterColumnType.instance, 1L);
        assertLog10Equals((long) Math.log10(-1), CounterColumnType.instance, -1L);

        assertLog10Equals(BigInteger.valueOf((long) Math.log10(5)), IntegerType.instance, BigInteger.valueOf(5));
        assertLog10Equals(BigInteger.valueOf(0), IntegerType.instance, BigInteger.valueOf((long) Math.E));
        assertLog10Equals(BigInteger.valueOf(0), IntegerType.instance, BigInteger.valueOf(1));

        assertLog10Equals(new BigDecimal("0.69897000433601880478626110527551"), DecimalType.instance, BigDecimal.valueOf(5));
        assertLog10Equals(new BigDecimal("0"), DecimalType.instance, BigDecimal.valueOf(1));
    }

    private <T extends Number> void assertLog10Equals(T expected, NumberType<T> inputType, T inputValue)
    {
        assertFctEquals(MathFcts.log10Fct(inputType), expected, inputType, inputValue);
    }

    private <T extends Number> void assertLog10Equals(T expected, NumberType<T> inputType, T inputValue, double delta)
    {
        assertFctEquals(MathFcts.log10Fct(inputType), expected, inputType, inputValue, delta);
    }

    @Test
    public void testRound()
    {
        assertRoundEquals(5, Int32Type.instance, 5);
        assertRoundEquals(0, Int32Type.instance, 0);
        assertRoundEquals(-1, Int32Type.instance, -1);

        assertRoundEquals((byte) 5, ByteType.instance, (byte) 5);
        assertRoundEquals((byte) 0, ByteType.instance, (byte) 0);
        assertRoundEquals((byte) -1, ByteType.instance, (byte) -1);

        assertRoundEquals((short) 5, ShortType.instance, (short) 5);
        assertRoundEquals((short) 0, ShortType.instance, (short) 0);
        assertRoundEquals((short) -1, ShortType.instance, (short) -1);

        assertRoundEquals((float) Math.round(5.5F), FloatType.instance, 5.5F);
        assertRoundEquals(1F, FloatType.instance, 1F);
        assertRoundEquals(0F, FloatType.instance, 0F);
        assertRoundEquals((float) Math.round(-1.5F), FloatType.instance, -1.5F);


        assertRoundEquals(5L, LongType.instance, 5L);
        assertRoundEquals(0L, LongType.instance, 0L);
        assertRoundEquals(-1L, LongType.instance, -1L);

        assertRoundEquals(5L, CounterColumnType.instance, 5L);
        assertRoundEquals(0L, CounterColumnType.instance, 0L);
        assertRoundEquals(-1L, CounterColumnType.instance, -1L);

        assertRoundEquals(BigInteger.valueOf(5), IntegerType.instance, BigInteger.valueOf(5));
        assertRoundEquals(BigInteger.valueOf(0), IntegerType.instance, BigInteger.valueOf(0));
        assertRoundEquals(BigInteger.valueOf(-1), IntegerType.instance, BigInteger.valueOf(-1));

        assertRoundEquals(new BigDecimal("6"), DecimalType.instance, BigDecimal.valueOf(5.5));
        assertRoundEquals(new BigDecimal("1"), DecimalType.instance, BigDecimal.valueOf(1));
        assertRoundEquals(new BigDecimal("0"), DecimalType.instance, BigDecimal.valueOf(0));
        assertRoundEquals(new BigDecimal("-2"), DecimalType.instance, BigDecimal.valueOf(-1.5));
    }

    private <T extends Number> void assertRoundEquals(T expected, NumberType<T> inputType, T inputValue)
    {
        assertFctEquals(MathFcts.roundFct(inputType), expected, inputType, inputValue);
    }

    private static ByteBuffer executeFunction(Function function, ByteBuffer input)
    {
        Arguments arguments = function.newArguments(ProtocolVersion.CURRENT);
        arguments.set(0, input);
        return ((ScalarFunction) function).execute(arguments);
    }

    private <T extends Number> void assertFctEquals(NativeFunction fct, T expected, NumberType<T> inputType, T inputValue)
    {
        ByteBuffer input = inputType.decompose(inputValue);
        if (expected instanceof BigDecimal)
        {
            // This block is to deal with the edgecase where two BigDecimals' values are equal but not their scale.
            BigDecimal bdExpected = (BigDecimal) expected;
            BigDecimal bdInputValue = (BigDecimal) inputType.compose(executeFunction(fct, input));
            assertEquals(bdExpected.compareTo(bdInputValue), 0);
        }
        else
        {
            assertEquals(expected, inputType.compose(executeFunction(fct, input)));
        }
    }

    private <T extends Number> void assertFctEquals(NativeFunction fct, T expected, NumberType<T> inputType, T inputValue, double delta)
    {
        ByteBuffer input = inputType.decompose(inputValue);
        T actual = inputType.compose(executeFunction(fct, input));
        if (Double.isNaN(expected.doubleValue())) {
            Assert.assertTrue(Double.isNaN(actual.doubleValue()));
        } else
        {
            Assert.assertTrue(Math.abs(expected.doubleValue() - actual.doubleValue()) <= delta);
        }

    }
}
