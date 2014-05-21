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

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateUtils;

public class TimestampSerializer implements TypeSerializer<Date>
{
    private static final String[] dateStringPatterns = new String[] {
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mmX",
            "yyyy-MM-dd HH:mmXX",
            "yyyy-MM-dd HH:mmXXX",
            "yyyy-MM-dd HH:mm:ssX",
            "yyyy-MM-dd HH:mm:ssXX",
            "yyyy-MM-dd HH:mm:ssXXX",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSSX",
            "yyyy-MM-dd HH:mm:ss.SSSXX",
            "yyyy-MM-dd HH:mm:ss.SSSXXX",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mmX",
            "yyyy-MM-dd'T'HH:mmXX",
            "yyyy-MM-dd'T'HH:mmXXX",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssX",
            "yyyy-MM-dd'T'HH:mm:ssXX",
            "yyyy-MM-dd'T'HH:mm:ssXXX",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd",
            "yyyy-MM-ddX",
            "yyyy-MM-ddXX",
            "yyyy-MM-ddXXX"
    };

    private static final String DEFAULT_FORMAT = dateStringPatterns[3];
    private static final Pattern timestampPattern = Pattern.compile("^-?\\d+$");

    private static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>()
    {
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat(DEFAULT_FORMAT);
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
                throw new MarshalException(String.format("unable to make long (for date) from: '%s'", source), e);
            }
        }

        // Last chance, attempt to parse as date-time string
        try
        {
            return DateUtils.parseDateStrictly(source, dateStringPatterns).getTime();
        }
        catch (ParseException e1)
        {
            throw new MarshalException(String.format("unable to coerce '%s' to a  formatted date (long)", source), e1);
        }
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

    public Class<Date> getType()
    {
        return Date.class;
    }
}
