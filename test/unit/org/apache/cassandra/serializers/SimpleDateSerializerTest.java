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

import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.utils.Pair;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class SimpleDateSerializerTest
{
    private static final long millisPerDay = 1000 * 60 * 60 * 24;

    private String dates[] = new String[]
    {
            "1970-01-01",
            "1970-01-02",
            "1969-12-31",
            "-0001-01-02",
            "-5877521-01-02",
            "2014-01-01",
            "5881580-01-10",
            "1920-12-01",
            "1582-10-19"
    };

    private static GregorianCalendar testCalendar = new GregorianCalendar();
    private static SimpleDateFormat dateFormatUTC = new SimpleDateFormat("yyyy-MM-dd");

    {
        testCalendar.setGregorianChange(new Date(Long.MIN_VALUE));
        testCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        dateFormatUTC.setCalendar(testCalendar);
        dateFormatUTC.setLenient(false);
    }

    @Test
    public void testDateStringToTimestamp()
    {
        List<String> unparsedDates = new ArrayList<>();
        List<String> badParseResults = new ArrayList<>();
        for (String date : dates)
        {
            try
            {
                Integer days = SimpleDateSerializer.dateStringToDays(date);
                ByteBuffer value = SimpleDateSerializer.instance.serialize(days);
                Integer deserialized = SimpleDateSerializer.instance.deserialize(value);

                String toStringValue = SimpleDateSerializer.instance.toString(deserialized);
                if (!date.equals(toStringValue)) {
                    badParseResults.add(String.format("Failed to parse date correctly.  Expected %s, got %s\n", date, toStringValue));
                }
            }
            catch (MarshalException e)
            {
                System.err.println("Got an exception: " + e);
                unparsedDates.add(date);
            }
        }
        assert unparsedDates.isEmpty() : "Unable to parse: " + unparsedDates;
        assert badParseResults.isEmpty() : "Incorrect parse results: " + badParseResults;
    }

    @Test
    public void testDaysStringToInt()
    {
        Integer value = SimpleDateSerializer.dateStringToDays("12345");
        assert value.compareTo(12345) == 0 : String.format("Failed to parse integer based date.  Expected %s, got %s",
                12345,
                value);
    }

    @Test
    public void testProlepticRange()
    {
        for (int i = 1; i < 31; ++i)
        {
            String date = "1582-10-";
            if (i < 10) date += "0";
            date += i;

            Integer days = SimpleDateSerializer.dateStringToDays(date);

            ByteBuffer value = SimpleDateType.instance.fromString(days.toString());
            Integer deserialized = SimpleDateSerializer.instance.deserialize(value);

            // Serialized values are unsigned int, unwrap bits w/overflow
            deserialized -= Integer.MIN_VALUE;

            Timestamp ts = new Timestamp(deserialized * millisPerDay);
            testCalendar.setTime(ts);

            Date newDate = testCalendar.getTime();
            assert (dateFormatUTC.format(newDate)).equals(date) :
                    String.format("Expected [%s], got [%s]", date, dateFormatUTC.format(newDate).toString());
        }
    }

    @Test (expected=MarshalException.class)
    public void testOutOfBoundsLow()
    {
        Integer days = SimpleDateSerializer.dateStringToDays(new Date(Integer.MIN_VALUE * millisPerDay - millisPerDay).toString());
    }

    @Test (expected=MarshalException.class)
    public void testOutOfBoundsHigh()
    {
        Integer days = SimpleDateSerializer.dateStringToDays(new Date(Integer.MAX_VALUE * millisPerDay + millisPerDay).toString());
    }

    @Test (expected=MarshalException.class)
    public void testBadInput()
    {
        Integer days = SimpleDateSerializer.dateStringToDays("12A-01-01");
    }

    @Test (expected=MarshalException.class)
    public void testBadMonth()
    {
        Integer days = SimpleDateSerializer.dateStringToDays("1000-13-01");
    }

    @Test (expected=MarshalException.class)
    public void testBadDay()
    {
        Integer days = SimpleDateSerializer.dateStringToDays("1000-12-32");
    }

    @Test (expected=MarshalException.class)
    public void testBadDayToMonth()
    {
        Integer days = SimpleDateSerializer.dateStringToDays("1000-09-31");
    }
}
