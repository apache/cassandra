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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class MathFctsTest
{

    @Test
    public void testAbs()
    {
        final ByteBuffer zeroByte = ByteType.instance.fromString("0");
        final ByteBuffer negativeThreeShort = ShortType.instance.fromString("-3");
        final ByteBuffer one32 = Int32Type.instance.fromString("1");
        final ByteBuffer oneFloat = FloatType.instance.fromString("1");
        final ByteBuffer oneLong = LongType.instance.fromString("1");
        final ByteBuffer oneDouble = DoubleType.instance.fromString("1");
        final ByteBuffer oneInteger = IntegerType.instance.fromString("1");
        final ByteBuffer oneDecimal = DecimalType.instance.fromString("1");

        final NativeScalarFunction byteFunc = MathFcts.absFct(ByteType.instance);
        final NativeScalarFunction shortFunc = MathFcts.absFct(ShortType.instance);
        final NativeScalarFunction i32Func = MathFcts.absFct(Int32Type.instance);
        final NativeScalarFunction floatFunc = MathFcts.absFct(FloatType.instance);
        final NativeScalarFunction longFunc = MathFcts.absFct(LongType.instance);
        final NativeScalarFunction doubleFunc = MathFcts.absFct(DoubleType.instance);
        final NativeScalarFunction integerFunc = MathFcts.absFct(IntegerType.instance);
        final NativeScalarFunction decimalFunc = MathFcts.absFct(DecimalType.instance);
        final NativeScalarFunction ccFunc = MathFcts.absFct(CounterColumnType.instance);

        assertEquals(1, ByteBufferUtil.toInt(executeFunction(i32Func, one32)));
        assertEquals(0, ByteBufferUtil.toByte(executeFunction(byteFunc, zeroByte)));
        assertEquals(3, ByteBufferUtil.toShort(executeFunction(shortFunc, negativeThreeShort)));
        assertEquals(1, ByteBufferUtil.toFloat(executeFunction(floatFunc, oneFloat)), 0);
        assertEquals(1, ByteBufferUtil.toLong(executeFunction(longFunc, oneLong)));
        assertEquals(1, ByteBufferUtil.toDouble(executeFunction(doubleFunc, oneDouble)), 0);
        assertEquals(oneInteger, executeFunction(integerFunc, oneInteger));
        assertEquals(oneDecimal, executeFunction(decimalFunc, oneDecimal));
        assertEquals(1, ByteBufferUtil.toLong(executeFunction(ccFunc, oneLong)));

    }

    @Test
    public void testExp()
    {
        final ByteBuffer fiveByte = ByteType.instance.fromString("5");
        final ByteBuffer oneShort = ShortType.instance.fromString("1");
        final ByteBuffer one32 = Int32Type.instance.fromString("1");
        final ByteBuffer oneFloat = FloatType.instance.fromString("1");
        final ByteBuffer zeroLong = LongType.instance.fromString("0");
        final ByteBuffer oneLong = LongType.instance.fromString("1");
        final ByteBuffer oneDouble = DoubleType.instance.fromString("1");
        final ByteBuffer zeroInteger = IntegerType.instance.fromString("0");
        final ByteBuffer oneDecimal = DecimalType.instance.fromString("1");
        final ByteBuffer oneInteger = IntegerType.instance.fromString("1");
        final ByteBuffer zeroDecimal = DecimalType.instance.fromString("0");

        final NativeScalarFunction byteFunc = MathFcts.expFct(ByteType.instance);
        final NativeScalarFunction shortFunc = MathFcts.expFct(ShortType.instance);
        final NativeScalarFunction i32Func = MathFcts.expFct(Int32Type.instance);
        final NativeScalarFunction floatFunc = MathFcts.expFct(FloatType.instance);
        final NativeScalarFunction longFunc = MathFcts.expFct(LongType.instance);
        final NativeScalarFunction doubleFunc = MathFcts.expFct(DoubleType.instance);
        final NativeScalarFunction integerFunc = MathFcts.expFct(IntegerType.instance);
        final NativeScalarFunction decimalFunc = MathFcts.expFct(DecimalType.instance);
        final NativeScalarFunction ccFunc = MathFcts.expFct(CounterColumnType.instance);

        assertEquals(
        (byte) Math.pow(Math.E, 5),ByteBufferUtil.toByte(executeFunction(byteFunc, fiveByte)));
        assertEquals((short) Math.E, ByteBufferUtil.toShort(executeFunction(shortFunc, oneShort)));
        assertEquals((int) Math.E, ByteBufferUtil.toInt(executeFunction(i32Func, one32)), 0);
        assertEquals((float) Math.E, ByteBufferUtil.toFloat(executeFunction(floatFunc, oneFloat)), 0);
        assertEquals(1L, ByteBufferUtil.toLong(executeFunction(longFunc, zeroLong)));
        assertEquals(Math.E, ByteBufferUtil.toDouble(executeFunction(doubleFunc, oneDouble)), 0);
        assertEquals(oneInteger, executeFunction(integerFunc, zeroInteger));
        assertEquals(oneDecimal, executeFunction(decimalFunc, zeroDecimal));
        assertEquals((long) Math.E, ByteBufferUtil.toLong(executeFunction(ccFunc, oneLong)), 0);

    }

    @Test
    public void testLog()
    {
        final ByteBuffer zeroByte = ByteType.instance.fromString("0");
        final ByteBuffer zeroShort = ShortType.instance.fromString("0");
        final ByteBuffer zero32 = Int32Type.instance.fromString("0");
        final ByteBuffer zeroFloat = FloatType.instance.fromString("0");
        final ByteBuffer zeroLong = LongType.instance.fromString("0");
        final ByteBuffer eDouble = DoubleType.instance.fromString(String.format("%f", Math.E));
        final ByteBuffer fiveInt = IntegerType.instance.fromString("5");
        final ByteBuffer fiveDecimal = DecimalType.instance.fromString("5");
        final ByteBuffer onePointSixishDecimal = DecimalType.instance.fromString(
        "1.6094379124341003746007593332262"
        );
        final ByteBuffer oneInt = IntegerType.instance.fromString("1");

        final NativeScalarFunction byteFunc = MathFcts.logFct(ByteType.instance);
        final NativeScalarFunction shortFunc = MathFcts.logFct(ShortType.instance);
        final NativeScalarFunction i32Func = MathFcts.logFct(Int32Type.instance);
        final NativeScalarFunction floatFunc = MathFcts.logFct(FloatType.instance);
        final NativeScalarFunction longFunc = MathFcts.logFct(LongType.instance);
        final NativeScalarFunction doubleFunc = MathFcts.logFct(DoubleType.instance);
        final NativeScalarFunction integerFunc = MathFcts.logFct(IntegerType.instance);
        final NativeScalarFunction decimalFunc = MathFcts.logFct(DecimalType.instance);
        final NativeScalarFunction ccFunc = MathFcts.logFct(CounterColumnType.instance);

        assertEquals(0, ByteBufferUtil.toByte(executeFunction(byteFunc, zeroByte)), 0);
        assertEquals(0, ByteBufferUtil.toShort(executeFunction(shortFunc, zeroShort)), 0);
        assertEquals((int) Double.NEGATIVE_INFINITY, ByteBufferUtil.toInt(executeFunction(i32Func, zero32)), 0);
        assertEquals(Float.NEGATIVE_INFINITY, ByteBufferUtil.toFloat(executeFunction(floatFunc, zeroFloat)), 0);
        assertEquals((long) Double.NEGATIVE_INFINITY, ByteBufferUtil.toLong(executeFunction(longFunc, zeroLong)));
        assertEquals(1, ByteBufferUtil.toDouble(executeFunction(doubleFunc, eDouble)), 0.000001);
        assertEquals(oneInt, executeFunction(integerFunc, fiveInt));
        assertEquals(onePointSixishDecimal, executeFunction(decimalFunc, fiveDecimal));
        assertEquals((long) Double.NEGATIVE_INFINITY, ByteBufferUtil.toLong(executeFunction(ccFunc, zeroLong)), 0);

    }

    @Test
    public void testLog10()
    {
        final ByteBuffer zeroByte = ByteType.instance.fromString("0");
        final ByteBuffer zeroShort = ShortType.instance.fromString("0");
        final ByteBuffer onehundredInt = Int32Type.instance.fromString("100");
        final ByteBuffer negativeOneFloat = FloatType.instance.fromString("-1");
        final ByteBuffer zeroLong = LongType.instance.fromString("0");
        final ByteBuffer zeroDble = DoubleType.instance.fromString("0");
        final ByteBuffer sixtyfourInteger = IntegerType.instance.fromString("64");
        final ByteBuffer sixtyfourDecimal = DecimalType.instance.fromString("64");
        final ByteBuffer onePointEightishDecimal = DecimalType.instance.fromString(
        "1.8061799739838871712824333683470"
        );
        final ByteBuffer oneInt = IntegerType.instance.fromString("1");


        final NativeScalarFunction byteFunc = MathFcts.log10Fct(ByteType.instance);
        final NativeScalarFunction shortFunc = MathFcts.log10Fct(ShortType.instance);
        final NativeScalarFunction i32Func = MathFcts.log10Fct(Int32Type.instance);
        final NativeScalarFunction floatFunc = MathFcts.log10Fct(FloatType.instance);
        final NativeScalarFunction longFunc = MathFcts.log10Fct(LongType.instance);
        final NativeScalarFunction doubleFunc = MathFcts.log10Fct(DoubleType.instance);
        final NativeScalarFunction integerFunc = MathFcts.log10Fct(IntegerType.instance);
        final NativeScalarFunction decimalFunc = MathFcts.log10Fct(DecimalType.instance);
        final NativeScalarFunction ccFunc = MathFcts.log10Fct(CounterColumnType.instance);

        assertEquals(0, ByteBufferUtil.toByte(executeFunction(byteFunc, zeroByte)));
        assertEquals(0, ByteBufferUtil.toShort(executeFunction(shortFunc, zeroShort)), 0);
        assertEquals(2, ByteBufferUtil.toInt(executeFunction(i32Func, onehundredInt)));
        assertEquals(Float.NaN, ByteBufferUtil.toFloat(executeFunction(floatFunc, negativeOneFloat)), 0);
        assertEquals((long) Double.NEGATIVE_INFINITY, ByteBufferUtil.toLong(executeFunction(longFunc, zeroLong)));
        assertEquals(Double.NEGATIVE_INFINITY, ByteBufferUtil.toDouble(executeFunction(doubleFunc, zeroDble)), 0);
        assertEquals(oneInt, executeFunction(integerFunc, sixtyfourInteger));
        assertEquals(onePointEightishDecimal, executeFunction(decimalFunc, sixtyfourDecimal));
        assertEquals((long) Double.NEGATIVE_INFINITY, ByteBufferUtil.toLong(executeFunction(ccFunc, zeroLong)));
    }

    @Test
    public void testRound()
    {
        final ByteBuffer zeroByte = ByteType.instance.fromString("0");
        final ByteBuffer zeroShort = ShortType.instance.fromString("0");
        final ByteBuffer zero32 = Int32Type.instance.fromString("0");
        final ByteBuffer negativeTwoPointNineFloat = FloatType.instance.fromString("-2.9");
        final ByteBuffer fourLong = LongType.instance.fromString("4");
        final ByteBuffer fivePointFiveDecimal = DecimalType.instance.fromString("5.5");
        final ByteBuffer sixInt = IntegerType.instance.fromString("6");
        final ByteBuffer zeroDouble = DoubleType.instance.fromString("0");
        final ByteBuffer sixDecimal = DecimalType.instance.fromString("6");

        final NativeScalarFunction byteFunc = MathFcts.roundFct(ByteType.instance);
        final NativeScalarFunction shortFunc = MathFcts.roundFct(ShortType.instance);
        final NativeScalarFunction i32Func = MathFcts.roundFct(Int32Type.instance);
        final NativeScalarFunction floatFunc = MathFcts.roundFct(FloatType.instance);
        final NativeScalarFunction longFunc = MathFcts.roundFct(LongType.instance);
        final NativeScalarFunction doubleFunc = MathFcts.roundFct(DoubleType.instance);
        final NativeScalarFunction integerFunc = MathFcts.roundFct(IntegerType.instance);
        final NativeScalarFunction decimalFunc = MathFcts.roundFct(DecimalType.instance);
        final NativeScalarFunction ccFunc = MathFcts.roundFct(CounterColumnType.instance);

        assertEquals(0, ByteBufferUtil.toByte(executeFunction(byteFunc, zeroByte)));
        assertEquals(0, ByteBufferUtil.toShort(executeFunction(shortFunc, zeroShort)));
        assertEquals(0, ByteBufferUtil.toInt(executeFunction(i32Func, zero32)));
        assertEquals(-3f, ByteBufferUtil.toFloat(executeFunction(floatFunc, negativeTwoPointNineFloat)), 0);
        assertEquals(4, ByteBufferUtil.toLong(executeFunction(longFunc, fourLong)));
        assertEquals(sixInt, executeFunction(integerFunc, sixInt));
        assertEquals(0d, ByteBufferUtil.toDouble(executeFunction(doubleFunc, zeroDouble)), 0);
        assertEquals(sixDecimal, executeFunction(decimalFunc, fivePointFiveDecimal));
        assertEquals(4, ByteBufferUtil.toLong(executeFunction(ccFunc, fourLong)));
    }

    private static ByteBuffer executeFunction(Function function, ByteBuffer input)
    {
        List<ByteBuffer> params = Collections.singletonList(input);
        return ((ScalarFunction) function).execute(ProtocolVersion.CURRENT, params);
    }
}
