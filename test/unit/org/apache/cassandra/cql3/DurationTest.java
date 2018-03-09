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

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.Duration.*;
import static org.apache.cassandra.cql3.Duration.NANOS_PER_HOUR;

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
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("0m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("10us").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T00:10:00"), Duration.from("10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T01:30:00"), Duration.from("90m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T02:10:00"), Duration.from("2h10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-23T00:10:00"), Duration.from("2d10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-24T01:00:00"), Duration.from("2d25h").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-10-21T00:00:00"), Duration.from("1mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2017-11-21T00:00:00"), Duration.from("14mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2017-02-28T00:00:00"), Duration.from("12mo").addTo(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testAddToWithNegativeDurations()
    {
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("-0m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("-10us").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-20T23:50:00"), Duration.from("-10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-20T22:30:00"), Duration.from("-90m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-20T21:50:00"), Duration.from("-2h10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-18T23:50:00"), Duration.from("-2d10m").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-17T23:00:00"), Duration.from("-2d25h").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-08-21T00:00:00"), Duration.from("-1mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2015-07-21T00:00:00"), Duration.from("-14mo").addTo(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2015-02-28T00:00:00"), Duration.from("-12mo").addTo(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testSubstractFrom()
    {
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("0m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("10us").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-20T23:50:00"), Duration.from("10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-20T22:30:00"), Duration.from("90m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-20T21:50:00"), Duration.from("2h10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-18T23:50:00"), Duration.from("2d10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-17T23:00:00"), Duration.from("2d25h").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-08-21T00:00:00"), Duration.from("1mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2015-07-21T00:00:00"), Duration.from("14mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2015-02-28T00:00:00"), Duration.from("12mo").substractFrom(toMillis("2016-02-29T00:00:00")));
    }

    @Test
    public void testSubstractWithNegativeDurations()
    {
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("-0m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T00:00:00"), Duration.from("-10us").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T00:10:00"), Duration.from("-10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T01:30:00"), Duration.from("-90m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-21T02:10:00"), Duration.from("-2h10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-23T00:10:00"), Duration.from("-2d10m").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-09-24T01:00:00"), Duration.from("-2d25h").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2016-10-21T00:00:00"), Duration.from("-1mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2017-11-21T00:00:00"), Duration.from("-14mo").substractFrom(toMillis("2016-09-21T00:00:00")));
        assertEquals(toMillis("2017-02-28T00:00:00"), Duration.from("-12mo").substractFrom(toMillis("2016-02-29T00:00:00")));
    }

    private long toMillis(String timeAsString)
    {
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = parser.parse(timeAsString, new ParsePosition(0));
        return DateUtils.truncate(date, Calendar.SECOND).getTime();
    }

    public void assertInvalidDuration(String duration, String expectedErrorMessage)
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
