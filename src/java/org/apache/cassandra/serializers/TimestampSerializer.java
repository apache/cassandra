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
import java.util.Date;

public class TimestampSerializer implements TypeSerializer<Date>
{
    public static final String[] iso8601Patterns = new String[] {
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mmZ",
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mmZ",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd",
            "yyyy-MM-ddZ"
    };

    static final String DEFAULT_FORMAT = iso8601Patterns[3];

    static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>()
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

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long for date (%d)", bytes.remaining()));
    }

    public String toString(Date value)
    {
        return FORMATTER.get().format(value);
    }

    public Class<Date> getType()
    {
        return Date.class;
    }
}
