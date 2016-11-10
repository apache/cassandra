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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TimeFctsTest
{
    @Test
    public void testMinTimeUuid()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = TimestampType.instance.fromString("2015-05-21 11:03:02+00");
        ByteBuffer output = executeFunction(TimeFcts.minTimeuuidFct, input);
        assertEquals(UUIDGen.minTimeUUID(timeInMillis), TimeUUIDType.instance.compose(output));
    }

    @Test
    public void testMaxTimeUuid()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = TimestampType.instance.fromString("2015-05-21 11:03:02+00");
        ByteBuffer output = executeFunction(TimeFcts.maxTimeuuidFct, input);
        assertEquals(UUIDGen.maxTimeUUID(timeInMillis), TimeUUIDType.instance.compose(output));
    }

    @Test
    public void testDateOf()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(TimeFcts.dateOfFct, input);
        assertEquals(dateTime.toDate(), TimestampType.instance.compose(output));
    }

    @Test
    public void testTimeUuidToTimestamp()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(TimeFcts.timeUuidToTimestamp, input);
        assertEquals(dateTime.toDate(), TimestampType.instance.compose(output));
    }

    @Test
    public void testUnixTimestampOfFct()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(TimeFcts.unixTimestampOfFct, input);
        assertEquals(timeInMillis, LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimeUuidToUnixTimestamp()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(TimeFcts.timeUuidToUnixTimestamp, input);
        assertEquals(timeInMillis, LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimeUuidToDate()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                .withZone(DateTimeZone.UTC)
                .parseDateTime("2015-05-21 11:03:02");

        long timeInMillis = dateTime.getMillis();
        ByteBuffer input = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(TimeFcts.timeUuidtoDate, input);

        long expectedTime = DateTimeFormat.forPattern("yyyy-MM-dd")
                                          .withZone(DateTimeZone.UTC)
                                          .parseDateTime("2015-05-21")
                                          .getMillis();

        assertEquals(expectedTime, SimpleDateType.instance.toTimeInMillis(output));
    }

    @Test
    public void testDateToTimestamp()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd")
                                          .withZone(DateTimeZone.UTC)
                                          .parseDateTime("2015-05-21");

        ByteBuffer input = SimpleDateType.instance.fromString("2015-05-21");
        ByteBuffer output = executeFunction(TimeFcts.dateToTimestamp, input);
        assertEquals(dateTime.toDate(), TimestampType.instance.compose(output));
    }

    @Test
    public void testDateToUnixTimestamp()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd")
                                          .withZone(DateTimeZone.UTC)
                                          .parseDateTime("2015-05-21");

        ByteBuffer input = SimpleDateType.instance.fromString("2015-05-21");
        ByteBuffer output = executeFunction(TimeFcts.dateToUnixTimestamp, input);
        assertEquals(dateTime.getMillis(), LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimestampToDate()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd")
                                          .withZone(DateTimeZone.UTC)
                                          .parseDateTime("2015-05-21");

        ByteBuffer input = TimestampType.instance.fromString("2015-05-21 11:03:02+00");
        ByteBuffer output = executeFunction(TimeFcts.timestampToDate, input);
        assertEquals(dateTime.getMillis(), SimpleDateType.instance.toTimeInMillis(output));
    }

    @Test
    public void testTimestampToDateWithEmptyInput()
    {
        ByteBuffer output = executeFunction(TimeFcts.timestampToDate, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertNull(output);
    }

    @Test
    public void testTimestampToUnixTimestamp()
    {
        DateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
                                          .withZone(DateTimeZone.UTC)
                                          .parseDateTime("2015-05-21 11:03:02");

        ByteBuffer input = TimestampType.instance.decompose(dateTime.toDate());
        ByteBuffer output = executeFunction(TimeFcts.timestampToUnixTimestamp, input);
        assertEquals(dateTime.getMillis(), LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimestampToUnixTimestampWithEmptyInput()
    {
        ByteBuffer output = executeFunction(TimeFcts.timestampToUnixTimestamp, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertNull(output);
    }

    private static ByteBuffer executeFunction(Function function, ByteBuffer input)
    {
        List<ByteBuffer> params = Arrays.asList(input);
        return ((ScalarFunction) function).execute(Server.CURRENT_VERSION, params);
    }
}
