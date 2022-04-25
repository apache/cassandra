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
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class TimestampSerializerTest
{
    public static final long ONE_SECOND = 1000L;
    public static final long ONE_MINUTE = 60 * ONE_SECOND;
    public static final long ONE_HOUR = 60 * ONE_MINUTE;
    public static final long ONE_DAY = 24 * ONE_HOUR;
    public static final long BASE_OFFSET = TimestampSerializer.dateStringToTimestamp("1970-01-01");

    @Test
    public void testFormatResults() throws MarshalException
    {
        validateStringTimestamp("1970-01-01 00:00", BASE_OFFSET);
        validateStringTimestamp("1970-01-01 00:01", BASE_OFFSET + ONE_MINUTE);
        validateStringTimestamp("1970-01-01 01:00", BASE_OFFSET + ONE_HOUR);
        validateStringTimestamp("1970-01-02 00:00", BASE_OFFSET + ONE_DAY);
        validateStringTimestamp("1970-01-02 00:00 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01 00:01+01", ONE_MINUTE - ONE_HOUR);
        validateStringTimestamp("1970-01-01 01:00+0100", ONE_HOUR - ONE_HOUR);
        validateStringTimestamp("1970-01-02 00:00+01:00", ONE_DAY - ONE_HOUR);
        validateStringTimestamp("1970-01-01 01:00-0200", ONE_HOUR + 2 * ONE_HOUR);
        validateStringTimestamp("1970-01-01 01:00Z", ONE_HOUR);

        validateStringTimestamp("1970-01-01 00:00:00", BASE_OFFSET);
        validateStringTimestamp("1970-01-01 00:00:01", BASE_OFFSET + ONE_SECOND);
        validateStringTimestamp("1970-01-01 00:01:00", BASE_OFFSET + ONE_MINUTE);
        validateStringTimestamp("1970-01-01 01:00:00", BASE_OFFSET + ONE_HOUR);
        validateStringTimestamp("1970-01-02 00:00:00", BASE_OFFSET + ONE_DAY);
        validateStringTimestamp("1970-01-02 00:00:00 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01 00:01:00+01", ONE_MINUTE - ONE_HOUR);
        validateStringTimestamp("1970-01-01 01:00:00+0100", ONE_HOUR - ONE_HOUR);
        validateStringTimestamp("1970-01-02 00:00:00+01:00", ONE_DAY - ONE_HOUR);
        validateStringTimestamp("1970-01-01 01:00:00-0200", ONE_HOUR + 2 * ONE_HOUR);
        validateStringTimestamp("1970-01-01 01:00:00Z", ONE_HOUR);

        validateStringTimestamp("1970-01-01 00:00:00.000", BASE_OFFSET);
        validateStringTimestamp("1970-01-01 00:00:00.000", BASE_OFFSET);
        validateStringTimestamp("1970-01-01 00:00:01.000", BASE_OFFSET + ONE_SECOND);
        validateStringTimestamp("1970-01-01 00:01:00.000", BASE_OFFSET + ONE_MINUTE);
        validateStringTimestamp("1970-01-01 01:00:00.000", BASE_OFFSET + ONE_HOUR);
        validateStringTimestamp("1970-01-02 00:00:00.000", BASE_OFFSET + ONE_DAY);
        validateStringTimestamp("1970-01-02 00:00:00.000 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01 00:00:00.100 UTC", 100L);
        validateStringTimestamp("1970-01-01 00:01:00.001+01", ONE_MINUTE - ONE_HOUR + 1);
        validateStringTimestamp("1970-01-01 01:00:00.002+0100", ONE_HOUR - ONE_HOUR + 2);
        validateStringTimestamp("1970-01-02 00:00:00.003+01:00", ONE_DAY - ONE_HOUR + 3);
        validateStringTimestamp("1970-01-01 01:00:00.004-0200", ONE_HOUR + 2 * ONE_HOUR + 4);
        validateStringTimestamp("1970-01-01 01:00:00.004Z", ONE_HOUR + 4);

        validateStringTimestamp("1970-01-01T00:00", BASE_OFFSET);
        validateStringTimestamp("1970-01-01T00:01", BASE_OFFSET + ONE_MINUTE);
        validateStringTimestamp("1970-01-01T01:00", BASE_OFFSET + ONE_HOUR);
        validateStringTimestamp("1970-01-02T00:00", BASE_OFFSET + ONE_DAY);
        validateStringTimestamp("1970-01-02T00:00 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01T00:01+01", ONE_MINUTE - ONE_HOUR);
        validateStringTimestamp("1970-01-01T01:00+0100", ONE_HOUR - ONE_HOUR);
        validateStringTimestamp("1970-01-02T00:00+01:00", ONE_DAY - ONE_HOUR);
        validateStringTimestamp("1970-01-01T01:00-0200", ONE_HOUR + 2 * ONE_HOUR);
        validateStringTimestamp("1970-01-01T01:00Z", ONE_HOUR);

        validateStringTimestamp("1970-01-01T00:00:00", BASE_OFFSET);
        validateStringTimestamp("1970-01-01T00:00:01", BASE_OFFSET + ONE_SECOND);
        validateStringTimestamp("1970-01-01T00:01:00", BASE_OFFSET + ONE_MINUTE);
        validateStringTimestamp("1970-01-01T01:00:00", BASE_OFFSET + ONE_HOUR);
        validateStringTimestamp("1970-01-02T00:00:00", BASE_OFFSET + ONE_DAY);
        validateStringTimestamp("1970-01-02T00:00:00 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01T00:01:00+01", ONE_MINUTE - ONE_HOUR);
        validateStringTimestamp("1970-01-01T01:00:00+0100", ONE_HOUR - ONE_HOUR);
        validateStringTimestamp("1970-01-02T00:00:00+01:00", ONE_DAY - ONE_HOUR);
        validateStringTimestamp("1970-01-01T01:00:00-0200", ONE_HOUR + 2 * ONE_HOUR);
        validateStringTimestamp("1970-01-01T01:00:00Z", ONE_HOUR);

        validateStringTimestamp("1970-01-01T00:00:00.000", BASE_OFFSET);
        validateStringTimestamp("1970-01-01T00:00:00.000", BASE_OFFSET);
        validateStringTimestamp("1970-01-01T00:00:01.000", BASE_OFFSET + ONE_SECOND);
        validateStringTimestamp("1970-01-01T00:01:00.000", BASE_OFFSET + ONE_MINUTE);
        validateStringTimestamp("1970-01-01T01:00:00.000", BASE_OFFSET + ONE_HOUR);
        validateStringTimestamp("1970-01-02T00:00:00.000", BASE_OFFSET + ONE_DAY);
        validateStringTimestamp("1970-01-02T00:00:00.000 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01T00:00:00.100 UTC", 100L);
        validateStringTimestamp("1970-01-01T00:01:00.001+01", ONE_MINUTE - ONE_HOUR + 1);
        validateStringTimestamp("1970-01-01T01:00:00.002+0100", ONE_HOUR - ONE_HOUR + 2);
        validateStringTimestamp("1970-01-02T00:00:00.003+01:00", ONE_DAY - ONE_HOUR + 3);
        validateStringTimestamp("1970-01-01T01:00:00.004-0200", ONE_HOUR + 2 * ONE_HOUR + 4);
        validateStringTimestamp("1970-01-01T01:00:00.004Z", ONE_HOUR + 4);

        validateStringTimestamp("1970-01-01", BASE_OFFSET);
        validateStringTimestamp("1970-01-02 UTC", ONE_DAY);
        validateStringTimestamp("1970-01-01+01", -ONE_HOUR);
        validateStringTimestamp("1970-01-01+0100", -ONE_HOUR);
        validateStringTimestamp("1970-01-02+01:00", ONE_DAY - ONE_HOUR);
        validateStringTimestamp("1970-01-01-0200", 2 * ONE_HOUR);
        validateStringTimestamp("1970-01-01Z", 0L);
    }

    @Test
    public void testGeneralTimeZoneFormats()
    {
        // Central Standard Time; CST, GMT-06:00
        final long cstOffset = 6 * ONE_HOUR;
        validateStringTimestamp("1970-01-01 00:00:00 Central Standard Time", cstOffset);
        validateStringTimestamp("1970-01-01 00:00:00 CST", cstOffset);
        validateStringTimestamp("1970-01-01 00:00:00 -0600", cstOffset);
        validateStringTimestamp("1970-01-01 00:00:00-0600", cstOffset);
        validateStringTimestamp("1970-01-01T00:00:00 GMT-06:00", cstOffset);
        validateStringTimestamp("1970-01-01T00:00:00 -0600", cstOffset);
        validateStringTimestamp("1970-01-01T00:00:00-0600", cstOffset);
    }

    @Test
    public void testVaryingFractionalPrecision() // CASSANDRA-15976
    {
        validateStringTimestamp("1970-01-01 00:00:00.10 UTC", 100L);
        validateStringTimestamp("1970-01-01 00:00:00.1+00", 100L);
        validateStringTimestamp("1970-01-01T00:00:00.10-0000", 100L);
        validateStringTimestamp("1970-01-01T00:00:00.1Z", 100L);
        validateStringTimestamp("1970-01-01 00:00:00.1 Central Standard Time", 6 * ONE_HOUR + 100L);
        validateStringTimestamp("1970-01-01 00:00:00.10+00:00", 100L);
        validateStringTimestamp("1970-01-01T00:00:00.1", BASE_OFFSET + 100L);
        validateStringTimestamp("1970-01-01T00:00:00.10-00:00", 100L);
    }

    @Test
    public void testZeroPadding() // CASSANDRA-16105
    {
        long expected = ONE_HOUR + ONE_MINUTE + ONE_SECOND;
        validateStringTimestamp("1970-01-01 01:01:01Z", expected);
        validateStringTimestamp("1970-1-1 1:1:1Z", expected);
        validateStringTimestamp("1970-01-01 01:01:01", BASE_OFFSET + expected);
        validateStringTimestamp("1970-1-1 1:1:1", BASE_OFFSET + expected);

        expected = -31556905139000L;
        validateStringTimestamp("0970-01-01 01:01:01Z", expected);
        validateStringTimestamp("970-1-1 1:1:1Z", expected);

        expected = 10*ONE_MINUTE + 100L;
        validateStringTimestamp("1970-01-01T0:0000000010:0.1Z", expected);
        validateStringTimestamp("0001970-01-01T0:010:0.1Z", expected);
        validateStringTimestamp("1970-1-1T0:10:0.1Z", expected);
        validateStringTimestamp("1970-0001-0001T0:010:0.1Z", expected);

        validateStringTimestamp("1970-1-01T1:1Z", ONE_HOUR + ONE_MINUTE);
        validateStringTimestamp("1970-1-01T1:1", BASE_OFFSET + ONE_HOUR + ONE_MINUTE);
    }

    @Test
    public void testInvalidTimezones()
    {
        List<String> timestamps = new ArrayList<String>(
            Arrays.asList(
                "1970-01-01 00:00:00 xentral Standard Time",
                "1970-01-01 00:00:00-060"
            )
        );
        expectStringTimestampsExcept(timestamps);
    }

    @Test
    public void testInvalidFormat()
    {
        List<String> timestamps = new ArrayList<String>(
            Arrays.asList(
                "1970-01-01 00:00:00andsomeextra",
                "prefix1970-01-01 00:00:00",
                "1970-01-01 00.56",
                "1970-01-0100:00:00"
            )
        );
        expectStringTimestampsExcept(timestamps);
    }

    @Test
    public void testNumeric()
    {
        // now (positive
        final long now = currentTimeMillis();
        assertEquals(now, TimestampSerializer.dateStringToTimestamp(Long.toString(now)));

        // negative
        final long then = -11234L;
        assertEquals(then, TimestampSerializer.dateStringToTimestamp(Long.toString(then)));

        // overflows
        for (Long l: Arrays.asList(Long.MAX_VALUE, Long.MIN_VALUE))
        {
            try
            {
                TimestampSerializer.dateStringToTimestamp(Long.toString(l) + "1");
                fail("Expected exception was not raised while parsing overflowed long.");
            }
            catch (MarshalException e)
            {
                continue;
            }
        }
    }

    @Test
    public void testNow()
    {
        final long threshold = 5;
        final long now = currentTimeMillis();
        final long parsed = TimestampSerializer.dateStringToTimestamp("now");
        assertTrue("'now' timestamp not within expected tolerance.", now <= parsed && parsed <= now + threshold);
    }

    private void validateStringTimestamp(String s, long expected)
    {
        try
        {
            long ts = TimestampSerializer.dateStringToTimestamp(s);
            assertEquals( "Failed to parse expected timestamp value from " + s, expected, ts);
        }
        catch (MarshalException e)
        {
            fail(String.format("Failed to parse \"%s\" as timestamp.", s));
        }
    }

    private void expectStringTimestampsExcept(List<String> timeStrings)
    {
        for (String s: timeStrings)
        {
            try
            {
                TimestampSerializer.dateStringToTimestamp(s);
                fail(String.format("Parsing succeeded while expecting failure from \"%s\"", s));
            }
            catch(MarshalException e)
            {
                continue;
            }

        }
    }
}
