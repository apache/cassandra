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
package org.apache.cassandra.cql3;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;

/**
 * Static helper methods and classes for constants.
 */
public abstract class Constants
{
    public enum Type
    {
        STRING
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                 if (StandardCharsets.US_ASCII.newEncoder().canEncode(text))
                 {
                     return AsciiType.instance;
                 }

                 return UTF8Type.instance;
            }
        },
        INTEGER
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                // We only try to determine the smallest possible type between int, long and BigInteger
                BigInteger b = new BigInteger(text);

                if (b.equals(BigInteger.valueOf(b.intValue())))
                    return Int32Type.instance;

                if (b.equals(BigInteger.valueOf(b.longValue())))
                    return LongType.instance;

                return IntegerType.instance;
            }
        },
        UUID
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                return java.util.UUID.fromString(text).version() == 1
                       ? TimeUUIDType.instance
                       : UUIDType.instance;
            }
        },
        FLOAT
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                if ("NaN".equals(text) || "-NaN".equals(text) || "Infinity".equals(text) || "-Infinity".equals(text))
                    return DoubleType.instance;

                // We only try to determine the smallest possible type between double and BigDecimal
                BigDecimal b = new BigDecimal(text);

                if (b.compareTo(BigDecimal.valueOf(b.doubleValue())) == 0)
                    return DoubleType.instance;

                return DecimalType.instance;
            }
        },
        BOOLEAN
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                return BooleanType.instance;
            }
        },
        HEX
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                return ByteType.instance;
            }
        },
        DURATION
        {
            @Override
            public AbstractType<?> getPreferedTypeFor(String text)
            {
                return DurationType.instance;
            }
        };

        /**
         * Returns the exact type for the specified text
         *
         * @param text the text for which the type must be determined
         * @return the exact type or {@code null} if it is not known.
         */
        public AbstractType<?> getPreferedTypeFor(String text)
        {
            return null;
        }
    }

    private static class UnsetLiteral extends Term.Raw
    {
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return UNSET_VALUE;
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }

        public String getText()
        {
            return "";
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }
    }

    // We don't have "unset" literal in the syntax, but it's used implicitely for JSON "DEFAULT UNSET" option
    public static final UnsetLiteral UNSET_LITERAL = new UnsetLiteral();

    public static final Value UNSET_VALUE = new Value(ByteBufferUtil.UNSET_BYTE_BUFFER);

    private static class NullLiteral extends Term.Raw
    {
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!testAssignment(keyspace, receiver).isAssignable())
                throw new InvalidRequestException("Invalid null value for counter increment/decrement");

            return NULL_VALUE;
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return receiver.type instanceof CounterColumnType
                 ? AssignmentTestable.TestResult.NOT_ASSIGNABLE
                 : AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }

        public String getText()
        {
            return "NULL";
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }
    }

    public static final NullLiteral NULL_LITERAL = new NullLiteral();

    public static final Term.Terminal NULL_VALUE = new Value(null)
    {
        @Override
        public Terminal bind(QueryOptions options)
        {
            // We return null because that makes life easier for collections
            return null;
        }

        @Override
        public String toString()
        {
            return "null";
        }
    };

    public static class Literal extends Term.Raw
    {
        private final Type type;
        private final String text;
        private final AbstractType<?> preferedType;

        private Literal(Type type, String text)
        {
            assert type != null && text != null;
            this.type = type;
            this.text = text;
            this.preferedType = type.getPreferedTypeFor(text);
        }

        public static Literal string(String text)
        {
            return new Literal(Type.STRING, text);
        }

        public static Literal integer(String text)
        {
            return new Literal(Type.INTEGER, text);
        }

        public static Literal floatingPoint(String text)
        {
            return new Literal(Type.FLOAT, text);
        }

        public static Literal uuid(String text)
        {
            return new Literal(Type.UUID, text);
        }

        public static Literal bool(String text)
        {
            return new Literal(Type.BOOLEAN, text);
        }

        public static Literal hex(String text)
        {
            return new Literal(Type.HEX, text);
        }

        public static Literal duration(String text)
        {
            return new Literal(Type.DURATION, text);
        }

        public Value prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!testAssignment(keyspace, receiver).isAssignable())
                throw new InvalidRequestException(String.format("Invalid %s constant (%s) for \"%s\" of type %s", type, text, receiver.name, receiver.type.asCQL3Type()));

            return new Value(parsedValue(receiver.type));
        }

        private ByteBuffer parsedValue(AbstractType<?> validator) throws InvalidRequestException
        {
            if (validator instanceof ReversedType<?>)
                validator = ((ReversedType<?>) validator).baseType;
            try
            {
                if (type == Type.HEX)
                    // Note that validator could be BytesType, but it could also be a custom type, so
                    // we hardcode BytesType (rather than using 'validator') in the call below.
                    // Further note that BytesType doesn't want it's input prefixed by '0x', hence the substring.
                    return BytesType.instance.fromString(text.substring(2));

                if (validator instanceof CounterColumnType)
                    return LongType.instance.fromString(text);
                return validator.fromString(text);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        @Override
        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            CQL3Type receiverType = receiver.type.asCQL3Type();
            if (receiverType.isCollection() || receiverType.isUDT() || receiverType.isVector())
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

            if (!(receiverType instanceof CQL3Type.Native))
                // Skip type validation for custom types. May or may not be a good idea
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

            CQL3Type.Native nt = (CQL3Type.Native)receiverType;

            // If the receiver type match the prefered type we can straight away return an exact match
            if (nt.getType().equals(preferedType))
                return AssignmentTestable.TestResult.EXACT_MATCH;

            switch (type)
            {
                case STRING:
                    switch (nt)
                    {
                        case ASCII:
                        case TEXT:
                        case INET:
                        case VARCHAR:
                        case DATE:
                        case TIME:
                        case TIMESTAMP:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
                case INTEGER:
                    switch (nt)
                    {
                        case BIGINT:
                        case COUNTER:
                        case DATE:
                        case DECIMAL:
                        case DOUBLE:
                        case DURATION:
                        case FLOAT:
                        case INT:
                        case SMALLINT:
                        case TIME:
                        case TIMESTAMP:
                        case TINYINT:
                        case VARINT:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
                case UUID:
                    switch (nt)
                    {
                        case UUID:
                        case TIMEUUID:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
                case FLOAT:
                    switch (nt)
                    {
                        case DECIMAL:
                        case DOUBLE:
                        case FLOAT:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
                case BOOLEAN:
                    switch (nt)
                    {
                        case BOOLEAN:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
                case HEX:
                    switch (nt)
                    {
                        case BLOB:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
                case DURATION:
                    switch (nt)
                    {
                        case DURATION:
                            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                    }
                    break;
            }
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            // Most constant are valid for more than one type (the extreme example being integer constants, which can
            // be use for any numerical type, including date, time, ...) so they don't have an exact type. And in fact,
            // for good or bad, any literal is valid for custom types, so we can never claim an exact type.
            // But really, the reason it's fine to return null here is that getExactTypeIfKnown is only used to
            // implement testAssignment() in Selectable and that method is overriden above.
            return null;
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return preferedType;
        }

        public String getRawText()
        {
            return text;
        }

        public String getText()
        {
            return type == Type.STRING ? String.format("'%s'", text) : text;
        }
    }

    /**
     * A constant value, i.e. a ByteBuffer.
     */
    public static class Value extends Term.Terminal
    {
        public final ByteBuffer bytes;

        public Value(ByteBuffer bytes)
        {
            this.bytes = bytes;
        }

        public ByteBuffer get(ProtocolVersion version)
        {
            return bytes;
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options)
        {
            return bytes;
        }

        @Override
        public String toString()
        {
            return ByteBufferUtil.bytesToHex(bytes);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert !receiver.type.isCollection();
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            try
            {
                ByteBuffer value = options.getValues().get(bindIndex);
                if (value != null && value != ByteBufferUtil.UNSET_BYTE_BUFFER)
                    receiver.type.validate(value);
                return value;
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer bytes = bindAndGet(options);
            if (bytes == null)
                return null;
            if (bytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return Constants.UNSET_VALUE;
            return new Constants.Value(bytes);
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnMetadata column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer value = t.bindAndGet(params.options);
            if (value == null)
                params.addTombstone(column);
            else if (value != ByteBufferUtil.UNSET_BYTE_BUFFER) // use reference equality and not object equality
                params.addCell(column, value);
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnMetadata column, Term t)
        {
            super(column, t);
        }

        public boolean requiresRead()
        {
            return !(column.type instanceof CounterColumnType);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            if (column.type instanceof CounterColumnType)
            {
                ByteBuffer bytes = t.bindAndGet(params.options);
                if (bytes == null)
                    throw new InvalidRequestException("Invalid null value for counter increment");
                if (bytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    return;

                long increment = ByteBufferUtil.toLong(bytes);
                params.addCounter(column, increment);
            }
            else if (column.type instanceof NumberType<?>)
            {
                @SuppressWarnings("unchecked") NumberType<Number> type = (NumberType<Number>) column.type;
                ByteBuffer increment = t.bindAndGet(params.options);
                ByteBuffer current = getCurrentCellBuffer(partitionKey, params);
                if (current == null)
                    return;
                ByteBuffer newValue = type.add(type.compose(current), type.compose(increment));
                params.addCell(column, newValue);
            }
            else if (column.type instanceof StringType)
            {
                ByteBuffer append = t.bindAndGet(params.options);
                ByteBuffer current = getCurrentCellBuffer(partitionKey, params);
                if (current == null)
                    return;
                ByteBuffer newValue = ByteBuffer.allocate(current.remaining() + append.remaining());
                FastByteOperations.copy(current, current.position(), newValue, newValue.position(), current.remaining());
                FastByteOperations.copy(append, append.position(), newValue, newValue.position() + current.remaining(), append.remaining());
                params.addCell(column, newValue);
            }
        }

        private ByteBuffer getCurrentCellBuffer(DecoratedKey key, UpdateParameters params)
        {
            Row currentRow = params.getPrefetchedRow(key, column.isStatic() ? Clustering.STATIC_CLUSTERING : params.currentClustering());
            Cell<?> currentCell = currentRow == null ? null : currentRow.getCell(column);
            return currentCell == null ? null : currentCell.buffer();
        }
    }

    public static class Substracter extends Operation
    {
        public Substracter(ColumnMetadata column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer bytes = t.bindAndGet(params.options);
            if (bytes == null)
                throw new InvalidRequestException("Invalid null value for counter increment");
            if (bytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return;

            long increment = ByteBufferUtil.toLong(bytes);
            if (increment == Long.MIN_VALUE)
                throw new InvalidRequestException("The negation of " + increment + " overflows supported counter precision (signed 8 bytes integer)");

            params.addCounter(column, -increment);
        }
    }

    // This happens to also handle collection because it doesn't felt worth
    // duplicating this further
    public static class Deleter extends Operation
    {
        public Deleter(ColumnMetadata column)
        {
            super(column, null);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            if (column.type.isMultiCell())
                params.setComplexDeletionTime(column);
            else
                params.addTombstone(column);
        }
    }
}
