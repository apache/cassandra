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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.apache.cassandra.utils.ByteBufferUtil;

// For byte-order comparability, we shift by Integer.MIN_VALUE and treat the data as an unsigned integer ranging from
// min date to max date w/epoch sitting in the center @ 2^31
public class SimpleDateSerializer implements TypeSerializer<Integer>
{
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);
    private static final long minSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
    private static final long maxSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);
    private static final long maxSupportedDays = (long)Math.pow(2,32) - 1;
    private static final long byteOrderShift = (long)Math.pow(2,31) * 2;

    private static final Pattern rawPattern = Pattern.compile("^-?\\d+$");
    public static final SimpleDateSerializer instance = new SimpleDateSerializer();

    public Integer deserialize(ByteBuffer bytes)
    {
        return bytes.remaining() == 0 ? null : ByteBufferUtil.toInt(bytes);
    }

    public ByteBuffer serialize(Integer value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
    }

    public static Integer dateStringToDays(String source) throws MarshalException
    {
        // Raw day value in unsigned int form, epoch @ 2^31
        if (rawPattern.matcher(source).matches())
        {
            try
            {
                Long result = Long.parseLong(source);

                if (result < 0 || result > maxSupportedDays)
                    throw new NumberFormatException("Input out of bounds: " + source);

                // Shift > epoch days into negative portion of Integer result for byte order comparability
                if (result >= Integer.MAX_VALUE)
                    result -= byteOrderShift;

                return result.intValue();
            }
            catch (NumberFormatException e)
            {
                throw new MarshalException(String.format("Unable to make unsigned int (for date) from: '%s'", source), e);
            }
        }

        // Attempt to parse as date string
        try
        {
            DateTime parsed = formatter.parseDateTime(source);
            long millis = parsed.getMillis();
            if (millis < minSupportedDateMillis)
                throw new MarshalException(String.format("Input date %s is less than min supported date %s", source, new LocalDate(minSupportedDateMillis).toString()));
            if (millis > maxSupportedDateMillis)
                throw new MarshalException(String.format("Input date %s is greater than max supported date %s", source, new LocalDate(maxSupportedDateMillis).toString()));

            Integer result = (int)TimeUnit.MILLISECONDS.toDays(millis);
            result -= Integer.MIN_VALUE;
            return result;
        }
        catch (IllegalArgumentException e1)
        {
            throw new MarshalException(String.format("Unable to coerce '%s' to a formatted date (long)", source), e1);
        }
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 4)
            throw new MarshalException(String.format("Expected 4 byte long for date (%d)", bytes.remaining()));
    }

    public String toString(Integer value)
    {
        if (value == null)
            return "";

        return formatter.print(new LocalDate(TimeUnit.DAYS.toMillis(value - Integer.MIN_VALUE), DateTimeZone.UTC));
    }

    public Class<Integer> getType()
    {
        return Integer.class;
    }
}
