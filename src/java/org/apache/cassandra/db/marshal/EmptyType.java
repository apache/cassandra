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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.EmptySerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * A type that only accept empty data.
 * It is only useful as a value validation type, not as a comparator since column names can't be empty.
 */
public class EmptyType extends AbstractType<Void>
{
    private enum NonEmptyWriteBehavior { FAIL, LOG_DATA_LOSS, SILENT_DATA_LOSS }

    private static final Logger logger = LoggerFactory.getLogger(EmptyType.class);
    private static final String KEY_EMPTYTYPE_NONEMPTY_BEHAVIOR = "cassandra.serialization.emptytype.nonempty_behavior";
    private static final NoSpamLogger NON_EMPTY_WRITE_LOGGER = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final NonEmptyWriteBehavior NON_EMPTY_WRITE_BEHAVIOR = parseNonEmptyWriteBehavior();

    private static NonEmptyWriteBehavior parseNonEmptyWriteBehavior()
    {
        String value = System.getProperty(KEY_EMPTYTYPE_NONEMPTY_BEHAVIOR);
        if (value == null)
            return NonEmptyWriteBehavior.FAIL;
        try
        {
            return NonEmptyWriteBehavior.valueOf(value.toUpperCase().trim());
        }
        catch (Exception e)
        {
            logger.warn("Unable to parse property " + KEY_EMPTYTYPE_NONEMPTY_BEHAVIOR + ", falling back to FAIL", e);
            return NonEmptyWriteBehavior.FAIL;
        }
    }

    public static final EmptyType instance = new EmptyType();

    private EmptyType() {super(ComparisonType.CUSTOM);} // singleton

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        return 0;
    }

    public String getString(ByteBuffer bytes)
    {
        return "";
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        if (!source.isEmpty())
            throw new MarshalException(String.format("'%s' is not empty", source));

        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7970
        if (!(parsed instanceof String))
            throw new MarshalException(String.format("Expected an empty string, but got: %s", parsed));
        if (!((String) parsed).isEmpty())
            throw new MarshalException(String.format("'%s' is not empty", parsed));

        return new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10365
        return CQL3Type.Native.EMPTY;
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13592
        return "\"\"";
    }

    public TypeSerializer<Void> getSerializer()
    {
        return EmptySerializer.instance;
    }

    @Override
    public int valueLengthIfFixed()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        return 0;
    }

    @Override
    public ByteBuffer readValue(DataInputPlus in)
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    @Override
    public ByteBuffer readValue(DataInputPlus in, int maxValueSize)
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    @Override
    public void writeValue(ByteBuffer value, DataOutputPlus out)
    {
        if (!value.hasRemaining())
            return;
        // In 3.0 writeValue was added which required EmptyType to write data, and relied on caller to never do that;
        // that behavior was unsafe so guard against it.  There are configurable behaviors, but the only allowed cases
        // should be *_DATA_LOSS (last resort... really should avoid this) and fail; fail should be preferred in nearly
        // all cases.
        // see CASSANDRA-15790
        switch (NON_EMPTY_WRITE_BEHAVIOR)
        {
            case LOG_DATA_LOSS:
                NON_EMPTY_WRITE_LOGGER.warn("Dropping data...", new NonEmptyWriteException("Attempted to write a non-empty value using EmptyType"));
            case SILENT_DATA_LOSS:
                return;
            case FAIL:
            default:
                throw new AssertionError("Attempted to write a non-empty value using EmptyType");
        }
    }

    private static final class NonEmptyWriteException extends RuntimeException
    {
        NonEmptyWriteException(String message)
        {
            super(message);
        }
    }
}
