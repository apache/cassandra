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
package org.apache.cassandra.serializers;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;


public class TimestampSerializer implements TypeSerializer<Date>
{

    private static final List<DateTimeFormatter> dateFormatters = generateFormatters();

    private static List<DateTimeFormatter> generateFormatters()
    {
        List<DateTimeFormatter> formatters = new ArrayList<>();

        final String[] dateTimeFormats = new String[]
                                         {
                                         "yyyy-MM-dd'T'HH:mm[:ss]",
                                         "yyyy-MM-dd HH:mm[:ss]"
                                         };
        final String[] offsetFormats = new String[]
                                         {
                                         " z",
                                         "X",
                                         " zzzz",
                                         "XXX"
                                         };

        for (String dateTimeFormat: dateTimeFormats)
        {
            // local date time
            formatters.add(
            new DateTimeFormatterBuilder()
            .appendPattern(dateTimeFormat)
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 9, true)
            .toFormatter()
            .withZone(ZoneId.systemDefault()));
            for (String offset : offsetFormats)
            {
                formatters.add(
                new DateTimeFormatterBuilder()
                .appendPattern(dateTimeFormat)
                .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 9, true)
                .appendPattern(offset)
                .toFormatter()
                );
            }
        }

        for (String offset: offsetFormats)
        {
            formatters.add(
            new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .appendPattern(offset)
            .parseDefaulting(ChronoField.NANO_OF_DAY, 0)
            .toFormatter()
            );
        }

        // local date
        formatters.add(
        new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_DATE)
        .parseDefaulting(ChronoField.NANO_OF_DAY, 0)
        .toFormatter().withZone(ZoneId.systemDefault()));

        return formatters;
    }

    private static final Pattern timestampPattern = Pattern.compile("^-?\\d+$");

    private static final FastThreadLocal<SimpleDateFormat> FORMATTER = new FastThreadLocal<SimpleDateFormat>()
    {
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat("yyyy-MM-dd HH:mmXX");
        }
    };

    private static final FastThreadLocal<SimpleDateFormat> FORMATTER_UTC = new FastThreadLocal<SimpleDateFormat>()
    {
        protected SimpleDateFormat initialValue()
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
        }
    };

    private static final FastThreadLocal<SimpleDateFormat> FORMATTER_TO_JSON = new FastThreadLocal<SimpleDateFormat>()
    {
        protected SimpleDateFormat initialValue()
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSX");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
        }
    };



    public static final TimestampSerializer instance = new TimestampSerializer();

    public Date deserialize(ByteBuffer bytes)
    {
        return bytes.remaining() == 0 ? null : new Date(ByteBufferUtil.toLong(bytes));
    }

    public ByteBuffer serialize(Date value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value.getTime());
    }

    public static long dateStringToTimestamp(String source) throws MarshalException
    {
        if (source.equalsIgnoreCase("now"))
            return System.currentTimeMillis();

        // Milliseconds since epoch?
        if (timestampPattern.matcher(source).matches())
        {
            try
            {
                return Long.parseLong(source);
            }
            catch (NumberFormatException e)
            {
                throw new MarshalException(String.format("Unable to make long (for date) from: '%s'", source), e);
            }
        }

        for (DateTimeFormatter fmt: dateFormatters)
        {
            try
            {
                return ZonedDateTime.parse(source, fmt).toInstant().toEpochMilli();
            }
            catch (DateTimeParseException e)
            {
                continue;
            }
        }
        throw new MarshalException(String.format("Unable to parse a date/time from '%s'", source));
    }

    public static SimpleDateFormat getJsonDateFormatter()
    {
    	return FORMATTER_TO_JSON.get();
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long for date (%d)", bytes.remaining()));
    }

    public String toString(Date value)
    {
        return value == null ? "" : FORMATTER.get().format(value);
    }

    public String toStringUTC(Date value)
    {
        return value == null ? "" : FORMATTER_UTC.get().format(value);
    }

    public Class<Date> getType()
    {
        return Date.class;
    }

    /**
     * Builds CQL literal for a timestamp using time zone UTC and fixed date format.
     * @see #FORMATTER_UTC
     */
    @Override
    public String toCQLLiteral(ByteBuffer buffer)
    {
        return buffer == null || !buffer.hasRemaining()
             ? "null"
             : FORMATTER_UTC.get().format(deserialize(buffer));
    }
}
