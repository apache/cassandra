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
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.EmptySerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.config.CassandraRelevantProperties.SERIALIZATION_EMPTY_TYPE_NONEMPTY_BEHAVIOR;

/**
 * A type that only accept empty data.
 * It is only useful as a value validation type, not as a comparator since column names can't be empty.
 */
public class EmptyType extends AbstractType<Void>
{
    private enum NonEmptyWriteBehavior { FAIL, LOG_DATA_LOSS, SILENT_DATA_LOSS }

    private static final Logger logger = LoggerFactory.getLogger(EmptyType.class);
    private static final NoSpamLogger NON_EMPTY_WRITE_LOGGER = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final NonEmptyWriteBehavior NON_EMPTY_WRITE_BEHAVIOR = parseNonEmptyWriteBehavior();

    private static NonEmptyWriteBehavior parseNonEmptyWriteBehavior()
    {
        String value = SERIALIZATION_EMPTY_TYPE_NONEMPTY_BEHAVIOR.getString();
        if (value == null)
            return NonEmptyWriteBehavior.FAIL;
        try
        {
            return NonEmptyWriteBehavior.valueOf(value.toUpperCase().trim());
        }
        catch (Exception e)
        {
            logger.warn("Unable to parse property " + SERIALIZATION_EMPTY_TYPE_NONEMPTY_BEHAVIOR.getKey() + ", falling back to FAIL", e);
            return NonEmptyWriteBehavior.FAIL;
        }
    }

    public static final EmptyType instance = new EmptyType();

    private EmptyType() {super(ComparisonType.CUSTOM);} // singleton

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        return null;
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return accessor.empty();
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return 0;
    }

    public <V> String getString(V value, ValueAccessor<V> accessor)
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
        if (!(parsed instanceof String))
            throw new MarshalException(String.format("Expected an empty string, but got: %s", parsed));
        if (!((String) parsed).isEmpty())
            throw new MarshalException(String.format("'%s' is not empty", parsed));

        return new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.EMPTY;
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return "\"\"";
    }

    public TypeSerializer<Void> getSerializer()
    {
        return EmptySerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 0;
    }

    @Override
    public <V> long writtenLength(V value, ValueAccessor<V> accessor)
    {
        // default implemenation requires non-empty bytes but this always requires empty bytes, so special case
        validate(value, accessor);
        return 0;
    }

    public ByteBuffer readBuffer(DataInputPlus in)
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    @Override
    public ByteBuffer readBuffer(DataInputPlus in, int maxValueSize)
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

    @Override
    public ByteBuffer getMaskedValue()
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }
}
