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
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.cql3.functions.TimeFcts.*;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillisAsBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TimeFctsTest
{
    private static final LocalDate LOCAL_DATE = LocalDate.of(2019, 8, 3);

    private static final ZonedDateTime DATE = LOCAL_DATE.atStartOfDay(ZoneOffset.UTC);
    private static final LocalTime LOCAL_TIME = LocalTime.of(11, 3, 2);
    private static final ZonedDateTime DATE_TIME =
            ZonedDateTime.of(LOCAL_DATE, LOCAL_TIME, ZoneOffset.UTC);
    private static final String DATE_STRING = DATE.format(DateTimeFormatter.ISO_LOCAL_DATE);
    private static final String DATE_TIME_STRING =
            DATE_STRING + " " + LOCAL_TIME.format(DateTimeFormatter.ISO_LOCAL_TIME);

    @Test
    public void testMinTimeUuid()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = TimestampType.instance.fromString(DATE_TIME_STRING + "+00");
        ByteBuffer output = executeFunction(TimeFcts.minTimeuuidFct, input);
        assertEquals(TimeUUID.minAtUnixMillis(timeInMillis), TimeUUIDType.instance.compose(output));
    }

    @Test
    public void testMinTimeUuidFromBigInt()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = LongType.instance.decompose(timeInMillis);
        ByteBuffer output = executeFunction(TimeFcts.minTimeuuidFct, input);
        assertEquals(TimeUUID.minAtUnixMillis(timeInMillis), TimeUUIDType.instance.compose(output));
    }

    @Test
    public void testMaxTimeUuid()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = TimestampType.instance.fromString(DATE_TIME_STRING + "+00");
        ByteBuffer output = executeFunction(TimeFcts.maxTimeuuidFct, input);
        assertEquals(TimeUUID.maxAtUnixMillis(timeInMillis), TimeUUIDType.instance.compose(output));
    }

    @Test
    public void testMaxTimeUuidFromBigInt()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = LongType.instance.decompose(timeInMillis);
        ByteBuffer output = executeFunction(TimeFcts.maxTimeuuidFct, input);
        assertEquals(TimeUUID.maxAtUnixMillis(timeInMillis), TimeUUIDType.instance.compose(output));
    }

    @Test
    public void testTimeUuidToTimestamp()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = ByteBuffer.wrap(atUnixMillisAsBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(toTimestamp(TimeUUIDType.instance), input);
        assertEquals(Date.from(DATE_TIME.toInstant()), TimestampType.instance.compose(output));
    }

    @Test
    public void testTimeUuidToUnixTimestamp()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = ByteBuffer.wrap(atUnixMillisAsBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(toUnixTimestamp(TimeUUIDType.instance), input);
        assertEquals(timeInMillis, LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimeUuidToDate()
    {
        long timeInMillis = DATE_TIME.toInstant().toEpochMilli();
        ByteBuffer input = ByteBuffer.wrap(atUnixMillisAsBytes(timeInMillis, 0));
        ByteBuffer output = executeFunction(toDate(TimeUUIDType.instance), input);

        long expectedTime = DATE.toInstant().toEpochMilli();

        assertEquals(expectedTime, SimpleDateType.instance.toTimeInMillis(output));
    }

    @Test
    public void testDateToTimestamp()
    {
        ByteBuffer input = SimpleDateType.instance.fromString(DATE_STRING);
        ByteBuffer output = executeFunction(toTimestamp(SimpleDateType.instance), input);
        assertEquals(Date.from(DATE.toInstant()), TimestampType.instance.compose(output));
    }

    @Test
    public void testDateToUnixTimestamp()
    {
        ByteBuffer input = SimpleDateType.instance.fromString(DATE_STRING);
        ByteBuffer output = executeFunction(toUnixTimestamp(SimpleDateType.instance), input);
        assertEquals(DATE.toInstant().toEpochMilli(), LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimestampToDate()
    {
        ByteBuffer input = TimestampType.instance.fromString(DATE_TIME_STRING + "+00");
        ByteBuffer output = executeFunction(toDate(TimestampType.instance), input);
        assertEquals(DATE.toInstant().toEpochMilli(), SimpleDateType.instance.toTimeInMillis(output));
    }

    @Test
    public void testBigIntegerToDate()
    {
        long millis = DATE.toInstant().toEpochMilli();

        ByteBuffer input = LongType.instance.decompose(millis);
        ByteBuffer output = executeFunction(toDate(TimestampType.instance), input);
        assertEquals(DATE.toInstant().toEpochMilli(), SimpleDateType.instance.toTimeInMillis(output));
    }

    @Test
    public void testTimestampToDateWithEmptyInput()
    {
        ByteBuffer output = executeFunction(toDate(TimestampType.instance), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertNull(output);
    }

    @Test
    public void testTimestampToUnixTimestamp()
    {
        ByteBuffer input = TimestampType.instance.decompose(Date.from(DATE_TIME.toInstant()));
        ByteBuffer output = executeFunction(toUnixTimestamp(TimestampType.instance), input);
        assertEquals(DATE_TIME.toInstant().toEpochMilli(), LongType.instance.compose(output).longValue());
    }

    @Test
    public void testBigIntegerToTimestamp()
    {
        long millis = DATE_TIME.toInstant().toEpochMilli();

        ByteBuffer input = LongType.instance.decompose(millis);
        ByteBuffer output = executeFunction(toTimestamp(TimestampType.instance), input);
        assertEquals(DATE_TIME.toInstant().toEpochMilli(), LongType.instance.compose(output).longValue());
    }

    @Test
    public void testTimestampToUnixTimestampWithEmptyInput()
    {
        ByteBuffer output = executeFunction(TimeFcts.toUnixTimestamp(TimestampType.instance), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertNull(output);
    }

    private static ByteBuffer executeFunction(Function function, ByteBuffer input)
    {
        Arguments arguments = function.newArguments(ProtocolVersion.CURRENT);
        arguments.set(0, input);
        return ((ScalarFunction) function).execute(arguments);
    }
}
