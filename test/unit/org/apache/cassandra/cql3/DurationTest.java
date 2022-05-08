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
package org.apache.cassandra.cql3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.TimeSerializer;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.cql3.Duration.*;

public class DurationTest
{
    @Test
    public void testFromStringWithStandardPattern()
    {
        assertEquals(Duration.newInstance(14, 0, 0), Duration.from("1y2mo"));
        assertEquals(Duration.newInstance(-14, 0, 0), Duration.from("-1y2mo"));
        assertEquals(Duration.newInstance(14, 0, 0), Duration.from("1Y2MO"));
        assertEquals(Duration.newInstance(0, 14, 0), Duration.from("2w"));
        assertEquals(Duration.newInstance(0, 2, 10 * NANOS_PER_HOUR), Duration.from("2d10h"));
        assertEquals(Duration.newInstance(0, 2, 0), Duration.from("2d"));
        assertEquals(Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR), Duration.from("30h"));
        assertEquals(Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE), Duration.from("30h20m"));
        assertEquals(Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE), Duration.from("20m"));
        assertEquals(Duration.newInstance(0, 0, 56 * NANOS_PER_SECOND), Duration.from("56s"));
        assertEquals(Duration.newInstance(0, 0, 567 * NANOS_PER_MILLI), Duration.from("567ms"));
        assertEquals(Duration.newInstance(0, 0, 1950 * NANOS_PER_MICRO), Duration.from("1950us"));
        assertEquals(Duration.newInstance(0, 0, 1950 * NANOS_PER_MICRO), Duration.from("1950µs"));
        assertEquals(Duration.newInstance(0, 0, 1950000), Duration.from("1950000ns"));
        assertEquals(Duration.newInstance(0, 0, 1950000), Duration.from("1950000NS"));
        assertEquals(Duration.newInstance(0, 0, -1950000), Duration.from("-1950000ns"));
        assertEquals(Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE), Duration.from("1y3mo2h10m"));
    }

    @Test
    public void testFromStringWithIso8601Pattern()
    {
        assertEquals(Duration.newInstance(12, 2, 0), Duration.from("P1Y2D"));
        assertEquals(Duration.newInstance(14, 0, 0), Duration.from("P1Y2M"));
        assertEquals(Duration.newInstance(0, 14, 0), Duration.from("P2W"));
        assertEquals(Duration.newInstance(12, 0, 2 * NANOS_PER_HOUR), Duration.from("P1YT2H"));
        assertEquals(Duration.newInstance(-14, 0, 0), Duration.from("-P1Y2M"));
        assertEquals(Duration.newInstance(0, 2, 0), Duration.from("P2D"));
        assertEquals(Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR), Duration.from("PT30H"));
        assertEquals(Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE), Duration.from("PT30H20M"));
        assertEquals(Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE), Duration.from("PT20M"));
        assertEquals(Duration.newInstance(0, 0, 56 * NANOS_PER_SECOND), Duration.from("PT56S"));
        assertEquals(Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE), Duration.from("P1Y3MT2H10M"));
    }

    @Test
    public void testFromStringWithIso8601AlternativePattern()
    {
        assertEquals(Duration.newInstance(12, 2, 0), Duration.from("P0001-00-02T00:00:00"));
        assertEquals(Duration.newInstance(14, 0, 0), Duration.from("P0001-02-00T00:00:00"));
        assertEquals(Duration.newInstance(12, 0, 2 * NANOS_PER_HOUR), Duration.from("P0001-00-00T02:00:00"));
        assertEquals(Duration.newInstance(-14, 0, 0), Duration.from("-P0001-02-00T00:00:00"));
        assertEquals(Duration.newInstance(0, 2, 0), Duration.from("P0000-00-02T00:00:00"));
        assertEquals(Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR), Duration.from("P0000-00-00T30:00:00"));
        assertEquals(Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE), Duration.from("P0000-00-00T30:20:00"));
        assertEquals(Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE), Duration.from("P0000-00-00T00:20:00"));
        assertEquals(Duration.newInstance(0, 0, 56 * NANOS_PER_SECOND), Duration.from("P0000-00-00T00:00:56"));
        assertEquals(Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE), Duration.from("P0001-03-00T02:10:00"));
    }

    @Test
    public void testInvalidDurations()
    {
        assertInvalidDuration(Long.MAX_VALUE + "d", "Invalid duration. The total number of days must be less or equal to 2147483647");
        assertInvalidDuration("2µ", "Unable to convert '2µ' to a duration");
        assertInvalidDuration("-2µ", "Unable to convert '2µ' to a duration");
        assertInvalidDuration("12.5s", "Unable to convert '12.5s' to a duration");
        assertInvalidDuration("2m12.5s", "Unable to convert '2m12.5s' to a duration");
        assertInvalidDuration("2m-12s", "Unable to convert '2m-12s' to a duration");
        assertInvalidDuration("12s3s", "Invalid duration. The seconds are specified multiple times");
        assertInvalidDuration("12s3m", "Invalid duration. The seconds should be after minutes");
        assertInvalidDuration("1Y3M4D", "Invalid duration. The minutes should be after days");
        assertInvalidDuration("P2Y3W", "Unable to convert 'P2Y3W' to a duration");
        assertInvalidDuration("P0002-00-20", "Unable to convert 'P0002-00-20' to a duration");
    }

    @Test
    public void testAddTo()
    {
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("0m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("10us").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T00:10:00.000", Duration.from("10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T01:30:00.000", Duration.from("90m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T02:10:00.000", Duration.from("2h10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-23T00:10:00.000", Duration.from("2d10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-24T01:00:00.000", Duration.from("2d25h").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-10-21T00:00:00.000", Duration.from("1mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2017-11-21T00:00:00.000", Duration.from("14mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2017-02-28T00:00:00.000", Duration.from("12mo").addTo(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testAddToWithNegativeDurations()
    {
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("-0m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("-10us").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-20T23:50:00.000", Duration.from("-10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-20T22:30:00.000", Duration.from("-90m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-20T21:50:00.000", Duration.from("-2h10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-18T23:50:00.000", Duration.from("-2d10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-17T23:00:00.000", Duration.from("-2d25h").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-08-21T00:00:00.000", Duration.from("-1mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2015-07-21T00:00:00.000", Duration.from("-14mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2015-02-28T00:00:00.000", Duration.from("-12mo").addTo(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testSubstractFrom()
    {
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("0m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("10us").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-20T23:50:00.000", Duration.from("10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-20T22:30:00.000", Duration.from("90m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-20T21:50:00.000", Duration.from("2h10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-18T23:50:00.000", Duration.from("2d10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-17T23:00:00.000", Duration.from("2d25h").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-08-21T00:00:00.000", Duration.from("1mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2015-07-21T00:00:00.000", Duration.from("14mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2015-02-28T00:00:00.000", Duration.from("12mo").substractFrom(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testSubstractWithNegativeDurations()
    {
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("-0m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T00:00:00.000", Duration.from("-10us").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T00:10:00.000", Duration.from("-10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T01:30:00.000", Duration.from("-90m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-21T02:10:00.000", Duration.from("-2h10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-23T00:10:00.000", Duration.from("-2d10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-09-24T01:00:00.000", Duration.from("-2d25h").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2016-10-21T00:00:00.000", Duration.from("-1mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2017-11-21T00:00:00.000", Duration.from("-14mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertTimeEquals("2017-02-28T00:00:00.000", Duration.from("-12mo").substractFrom(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testFloorTime()
    {
        long time = floorTime("16:12:00", "2h");
        Duration result = Duration.newInstance(0, 0, time);
        Duration expected = Duration.from("16h");
        assertEquals(expected, result);
    }

    @Test
    public void testInvalidFloorTimestamp()
    {
        try
        {
            floorTimestamp("2016-09-27T16:12:00", "2h", "2017-09-01T00:00:00");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals("The floor function starting time is greater than the provided time", e.getMessage());
        }

        try
        {
            floorTimestamp("2016-09-27T16:12:00", "-2h", "2016-09-27T00:00:00");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals("Negative durations are not supported by the floor function", e.getMessage());
        }
    }

    @Test
    public void testFloorTimestampWithDurationInHours()
    {
        // Test floor with a timestamp equals to the start time
        long result = floorTimestamp("2016-09-27T16:12:00", "2h", "2016-09-27T16:12:00");
        assertTimeEquals("2016-09-27T16:12:00.000", result);

        // Test floor with a duration equals to zero
        result = floorTimestamp("2016-09-27T18:12:00", "0h", "2016-09-27T16:12:00");
        assertTimeEquals("2016-09-27T18:12:00.000", result);

        // Test floor with a timestamp exactly equals to the start time + (1 x duration)
        result = floorTimestamp("2016-09-27T18:12:00", "2h", "2016-09-27T16:12:00");
        assertTimeEquals("2016-09-27T18:12:00.000", result);

        // Test floor with a timestamp in the first bucket
        result = floorTimestamp("2016-09-27T16:13:00", "2h", "2016-09-27T16:12:00");
        assertTimeEquals("2016-09-27T16:12:00.000", result);

        // Test floor with a timestamp in another bucket
        result = floorTimestamp("2016-09-27T16:12:00", "2h", "2016-09-27T00:00:00");
        assertTimeEquals("2016-09-27T16:00:00.000", result);
    }

    @Test
    public void testFloorTimestampWithDurationInDays()
    {
        // Test floor with a start time at the beginning of the month
        long result = floorTimestamp("2016-09-27T16:12:00", "2d", "2016-09-01T00:00:00");
        assertTimeEquals("2016-09-27T00:00:00.000", result);

        // Test floor with a start time in the previous month
        result = floorTimestamp("2016-09-27T16:12:00", "2d", "2016-08-01T00:00:00");
        assertTimeEquals("2016-09-26T00:00:00.000", result);
    }

    @Test
    public void testFloorTimestampWithDurationInDaysAndHours()
    {
        long result = floorTimestamp("2016-09-27T16:12:00", "2d12h", "2016-09-01T00:00:00");
        assertTimeEquals("2016-09-26T00:00:00.000", result);
    }

    @Test
    public void testFloorTimestampWithDurationInMonths()
    {
        // Test floor with a timestamp equals to the start time
        long result = floorTimestamp("2016-09-01T00:00:00", "2mo", "2016-09-01T00:00:00");
        assertTimeEquals("2016-09-01T00:00:00.000", result);

        // Test floor with a timestamp in the first bucket
        result = floorTimestamp("2016-09-27T16:12:00", "2mo", "2016-09-01T00:00:00");
        assertTimeEquals("2016-09-01T00:00:00.000", result);

        // Test floor with a start time at the beginning of the year (LEAP YEAR)
        result = floorTimestamp("2016-09-27T16:12:00", "1mo", "2016-01-01T00:00:00");
        assertTimeEquals("2016-09-01T00:00:00.000", result);

        // Test floor with a start time at the beginning of the previous year
        result = floorTimestamp("2016-09-27T16:12:00", "2mo", "2015-01-01T00:00:00");
        assertTimeEquals("2016-09-01T00:00:00.000", result);

        // Test floor with a start time in the previous year
        result = floorTimestamp("2016-09-27T16:12:00", "2mo", "2015-02-02T00:00:00");
        assertTimeEquals("2016-08-02T00:00:00.000", result);

    }

    @Test
    public void testFloorTimestampWithDurationInMonthsAndDays()
    {
        long result = floorTimestamp("2016-09-27T16:12:00", "2mo2d", "2016-01-01T00:00:00");
        assertTimeEquals("2016-09-09T00:00:00.000", result);

        result = floorTimestamp("2016-09-27T16:12:00", "2mo5d", "2016-01-01T00:00:00");
        assertTimeEquals("2016-09-21T00:00:00.000", result);

        // Test floor with a timestamp in the first bucket
        result = floorTimestamp("2016-09-04T16:12:00", "2mo5d", "2016-07-01T00:00:00");
        assertTimeEquals("2016-07-01T00:00:00.000", result);

        // Test floor with a timestamp in a bucket starting on the last day of the month
        result = floorTimestamp("2016-09-27T16:12:00", "2mo10d", "2016-01-01T00:00:00");
        assertTimeEquals("2016-07-31T00:00:00.000", result);

        // Test floor with a timestamp in a bucket starting on the first day of the month
        result = floorTimestamp("2016-09-27T16:12:00", "2mo12d", "2016-01-01T00:00:00");
        assertTimeEquals("2016-08-06T00:00:00.000", result);

        // Test leap years
        result = floorTimestamp("2016-04-27T16:12:00", "1mo30d", "2016-01-01T00:00:00");
        assertTimeEquals("2016-03-02T00:00:00.000", result);

        result = floorTimestamp("2015-04-27T16:12:00", "1mo30d", "2015-01-01T00:00:00");
        assertTimeEquals("2015-03-03T00:00:00.000", result);
    }

    @Test
    public void testFloorTimestampWithDurationSmallerThanPrecision()
    {
        long result = floorTimestamp("2016-09-27T18:14:00", "5us", "2016-09-27T16:12:00");
        assertTimeEquals("2016-09-27T18:14:00.000", result);

        result = floorTimestamp("2016-09-27T18:14:00", "1h5us", "2016-09-27T16:12:00");
        assertTimeEquals("2016-09-27T18:12:00.000", result);
    }

    @Test
    public void testFloorTimestampWithLeapSecond()
    {
        long result = floorTimestamp("2016-07-02T00:00:00", "2m", "2016-06-30T23:58:00");
        assertTimeEquals("2016-07-02T00:00:00.000", result);
    }

    @Test
    public void testFloorTimestampWithComplexDuration()
    {
        long result = floorTimestamp("2016-07-02T00:00:00", "2mo2d8h", "2016-01-01T00:00:00");
        assertTimeEquals("2016-05-05T16:00:00.000", result);
    }

    @Test
    public void testInvalidFloorTime()
    {
        try
        {
            floorTime("16:12:00", "2d");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals("For time values, the floor can only be computed for durations smaller that a day", e.getMessage());
        }

        try
        {
            floorTime("16:12:00", "25h");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals("For time values, the floor can only be computed for durations smaller that a day", e.getMessage());
        }

        try
        {
            floorTime("16:12:00", "-2h");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals("Negative durations are not supported by the floor function", e.getMessage());
        }
    }

    private static long toMillis(String timeAsString)
    {
        OffsetDateTime dateTime = LocalDateTime.parse(timeAsString, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
                                               .atOffset(ZoneOffset.UTC);

        return Instant.from(dateTime).toEpochMilli();
    }

    private static String fromMillis(long timeInMillis)
    {
        return Instant.ofEpochMilli(timeInMillis)
                      .atOffset(ZoneOffset.UTC)
                      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
    }

    private static void assertTimeEquals(String expected, long actualTimeInMillis)
    {
        assertEquals(expected, fromMillis(actualTimeInMillis));
    }

    private static long floorTimestamp(String time, String duration, String startingTime)
    {
        return Duration.floorTimestamp(toMillis(time), Duration.from(duration), toMillis(startingTime));
    }

    private static long floorTime(String time, String duration)
    {
        return Duration.floorTime(timeInNanos(time), Duration.from(duration));
    }

    private static long timeInNanos(String timeAsString)
    {
        return TimeSerializer.timeStringToLong(timeAsString);
    }

    private static void assertInvalidDuration(String duration, String expectedErrorMessage)
    {
        try
        {
            Duration.from(duration);
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }
}
