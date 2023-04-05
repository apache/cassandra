/**
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
package org.apache.cassandra.serializers;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class TimeSerializerTest
{
    @Test
    public void testSerializerFromString()
    {
        // nano
        long expected = 5;
        Long time = TimeSerializer.timeStringToLong("00:00:00.000000005");
        assert time == expected : String.format("Failed nano conversion.  Expected %s, got %s", expected, time);

        // usec
        expected = TimeUnit.MICROSECONDS.toNanos(123);
        time = TimeSerializer.timeStringToLong("00:00:00.000123000");
        assert time == expected : String.format("Failed usec conversion.  Expected %s, got %s", expected, time);

        // milli
        expected = TimeUnit.MILLISECONDS.toNanos(123);
        time = TimeSerializer.timeStringToLong("00:00:00.123000");
        assert time == expected : String.format("Failed milli conversion.  Expected %s, got %s", expected, time);

        // sec
        expected = TimeUnit.SECONDS.toNanos(15);
        time = TimeSerializer.timeStringToLong("00:00:15.000");
        assert time == expected : String.format("Failed sec conversion.  Expected %s, got %s", expected, time);

        // min
        expected = TimeUnit.MINUTES.toNanos(13);
        time = TimeSerializer.timeStringToLong("00:13:00.000");
        assert time == expected : String.format("Failed min conversion.  Expected %s, got %s", expected, time);

        // hour
        expected = TimeUnit.HOURS.toNanos(2);
        time = TimeSerializer.timeStringToLong("02:0:00.000");
        assert time == expected : String.format("Failed min conversion.  Expected %s, got %s", expected, time);

        // complex
        expected = buildExpected(4, 31, 12, 123, 456, 789);
        time = TimeSerializer.timeStringToLong("4:31:12.123456789");
        assert time == expected : String.format("Failed complex conversion.  Expected %s, got %s", expected, time);

        // upper bound
        expected = buildExpected(23, 59, 59, 999, 999, 999);
        time = TimeSerializer.timeStringToLong("23:59:59.999999999");
        assert time == expected : String.format("Failed upper bounds conversion.  Expected %s, got %s", expected, time);

        // Test partial nano
        expected = buildExpected(12, 13, 14, 123, 654, 120);
        time = TimeSerializer.timeStringToLong("12:13:14.12365412");
        assert time == expected : String.format("Failed partial nano timestring.  Expected %s, got %s", expected, time);

        // Test raw long value
        expected = 10;
        time = TimeSerializer.timeStringToLong("10");
        assert time == expected : String.format("Failed long conversion.  Expected %s, got %s", expected, time);

        // Test 0 long
        expected = 0;
        time = TimeSerializer.timeStringToLong("0");
        assert time == expected : String.format("Failed long conversion.  Expected %s, got %s", expected, time);
    }

    private long buildExpected(int hour, int minute, int second, int milli, int micro, int nano)
    {
        return  TimeUnit.HOURS.toNanos(hour) +
                TimeUnit.MINUTES.toNanos(minute) +
                TimeUnit.SECONDS.toNanos(second) +
                TimeUnit.MILLISECONDS.toNanos(milli) +
                TimeUnit.MICROSECONDS.toNanos(micro) +
                nano;
    }

    @Test
    public void testSerializerToString()
    {
        String source = "00:00:00.000000011";
        Long time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        source = "00:00:00.000012311";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        source = "00:00:00.123000000";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        source = "00:00:12.123450000";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        source = "00:34:12.123450000";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        source = "15:00:12.123450000";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        // boundaries
        source = "00:00:00.000000000";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        source = "23:59:59.999999999";
        time = TimeSerializer.timeStringToLong(source);
        assert(source.equals(TimeSerializer.instance.toString(time)));

        // truncated
        source = "01:14:18.12";
        time = TimeSerializer.timeStringToLong(source);
        String result = TimeSerializer.instance.toString(time);
        assert(result.equals("01:14:18.120000000"));

        source = "01:14:18.1201";
        time = TimeSerializer.timeStringToLong(source);
        result = TimeSerializer.instance.toString(time);
        assert(result.equals("01:14:18.120100000"));

        source = "01:14:18.1201098";
        time = TimeSerializer.timeStringToLong(source);
        result = TimeSerializer.instance.toString(time);
        assert(result.equals("01:14:18.120109800"));
    }

    @Test public void testSerialization()
    {
        String source = "01:01:01.123123123";
        Long nt = TimeSerializer.timeStringToLong(source);

        ByteBuffer buf = TimeSerializer.instance.serialize(nt);
        TimeSerializer.instance.validate(buf);

        Long result = TimeSerializer.instance.deserialize(buf);
        String strResult = TimeSerializer.instance.toString(result);

        assert(strResult.equals(source));
    }

    @Test (expected=MarshalException.class)
    public void testBadHourLow()
    {
        Long time = TimeSerializer.timeStringToLong("-1:0:0.123456789");
    }

    @Test (expected=MarshalException.class)
    public void testBadHourHigh()
    {
        Long time = TimeSerializer.timeStringToLong("24:0:0.123456789");
    }

    @Test (expected=MarshalException.class)
    public void testBadMinuteLow()
    {
        Long time = TimeSerializer.timeStringToLong("23:-1:0.123456789");
    }

    @Test (expected=MarshalException.class)
    public void testBadMinuteHigh()
    {
        Long time = TimeSerializer.timeStringToLong("23:60:0.123456789");
    }

    @Test (expected=MarshalException.class)
    public void testEmpty()
    {
        Long time = TimeSerializer.timeStringToLong("");
    }

    @Test (expected=MarshalException.class)
    public void testBadSecondLow()
    {
        Long time = TimeSerializer.timeStringToLong("23:59:-1.123456789");
    }

    @Test (expected=MarshalException.class)
    public void testBadSecondHigh()
    {
        Long time = TimeSerializer.timeStringToLong("23:59:60.123456789");
    }

    @Test (expected=MarshalException.class)
    public void testBadSecondHighNoMilli()
    {
        Long time = TimeSerializer.timeStringToLong("23:59:60");
    }

    @Test (expected=MarshalException.class)
    public void testBadNanoLow()
    {
        Long time = TimeSerializer.timeStringToLong("23:59:59.-123456789");
    }

    @Test (expected=MarshalException.class)
    public void testBadNanoHigh()
    {
        Long time = TimeSerializer.timeStringToLong("23:59:59.1234567899");
    }

    @Test (expected=MarshalException.class)
    public void testBadNanoCharacter()
    {
        Long time = TimeSerializer.timeStringToLong("23:59:59.12345A789");
    }

    @Test (expected=MarshalException.class)
    public void testNegativeLongTime()
    {
        Long time = TimeSerializer.timeStringToLong("-10");
    }

    @Test (expected=MarshalException.class)
    public void testRawLongOverflow()
    {
        Long input = TimeUnit.DAYS.toNanos(1) + 1;
        Long time = TimeSerializer.timeStringToLong(input.toString());
    }
}
