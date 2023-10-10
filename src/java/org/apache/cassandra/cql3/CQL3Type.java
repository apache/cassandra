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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.CollectionType.Kind;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.stream.Collectors.toList;

public interface CQL3Type
{
    static final Logger logger = LoggerFactory.getLogger(CQL3Type.class);

    default boolean isCollection()
    {
        return false;
    }

    default boolean isUDT()
    {
        return false;
    }

    default boolean isVector()
    {
        return false;
    }

    public AbstractType<?> getType();

    /**
     * Generates CQL literal from a binary value of this type.
     *  @param bytes the value to convert to a CQL literal. This value must be
     * serialized with {@code version} of the native protocol.
     */
    String toCQLLiteral(ByteBuffer bytes);

    /**
     * Generates a binary value for the CQL literal of this type
     */
    default ByteBuffer fromCQLLiteral(String literal)
    {
        return fromCQLLiteral(SchemaConstants.DUMMY_KEYSPACE_OR_TABLE_NAME, literal);
    }

    /**
     * Generates a binary value for the CQL literal of this type
     */
    default ByteBuffer fromCQLLiteral(String keyspaceName, String literal)
    {
        return Terms.asBytes(keyspaceName, literal, getType());
    }

    public enum Native implements CQL3Type
    {
        ASCII       (AsciiType.instance),
        BIGINT      (LongType.instance),
        BLOB        (BytesType.instance),
        BOOLEAN     (BooleanType.instance),
        COUNTER     (CounterColumnType.instance),
        DATE        (SimpleDateType.instance),
        DECIMAL     (DecimalType.instance),
        DOUBLE      (DoubleType.instance),
        DURATION    (DurationType.instance),
        EMPTY       (EmptyType.instance),
        FLOAT       (FloatType.instance),
        INET        (InetAddressType.instance),
        INT         (Int32Type.instance),
        SMALLINT    (ShortType.instance),
        TEXT        (UTF8Type.instance),
        TIME        (TimeType.instance),
        TIMESTAMP   (TimestampType.instance),
        TIMEUUID    (TimeUUIDType.instance),
        TINYINT     (ByteType.instance),
        UUID        (UUIDType.instance),
        VARCHAR     (UTF8Type.instance),
        VARINT      (IntegerType.instance);

        private final AbstractType<?> type;

        Native(AbstractType<?> type)
        {
            this.type = type;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        /**
         * Delegate to
         * {@link org.apache.cassandra.serializers.TypeSerializer#toCQLLiteral(ByteBuffer)}
         * for native types as most CQL literal representations work fine with the default
         * {@link org.apache.cassandra.serializers.TypeSerializer#toString(Object)}
         * {@link org.apache.cassandra.serializers.TypeSerializer#deserialize(ByteBuffer)} implementations.
         */
        public String toCQLLiteral(ByteBuffer buffer)
        {
            return type.getSerializer().toCQLLiteral(buffer);
        }

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

    public static class Custom implements CQL3Type
    {
        private final AbstractType<?> type;

        public Custom(AbstractType<?> type)
        {
            this.type = type;
        }

        public Custom(String className) throws SyntaxException, ConfigurationException
        {
            this(TypeParser.parse(className));
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer)
        {
            // *always* use the 'blob' syntax to express custom types in CQL
            return Native.BLOB.toCQLLiteral(buffer);
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Custom))
                return false;

            Custom that = (Custom)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return "'" + type + '\'';
        }
    }

    public static class Collection implements CQL3Type
    {
        private final CollectionType<?> type;

        public Collection(CollectionType<?> type)
        {
            this.type = type;
        }

        public CollectionType<?> getType()
        {
            return type;
        }

        public boolean isCollection()
        {
            return true;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer)
        {
            if (buffer == null)
                return "null";

            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            int size = CollectionSerializer.readCollectionSize(buffer, ByteBufferAccessor.instance);
            buffer.position(buffer.position() + CollectionSerializer.sizeOfCollectionSize());

            switch (type.kind)
            {
                case LIST:
                    CQL3Type elements = ((ListType<?>) type).getElementsType().asCQL3Type();
                    target.append('[');
                    generateSetOrListCQLLiteral(buffer, target, size, elements);
                    target.append(']');
                    break;
                case SET:
                    elements = ((SetType<?>) type).getElementsType().asCQL3Type();
                    target.append('{');
                    generateSetOrListCQLLiteral(buffer, target, size, elements);
                    target.append('}');
                    break;
                case MAP:
                    target.append('{');
                    generateMapCQLLiteral(buffer, target, size);
                    target.append('}');
                    break;
            }
            return target.toString();
        }

        private void generateMapCQLLiteral(ByteBuffer buffer, StringBuilder target, int size)
        {
            CQL3Type keys = ((MapType<?, ?>) type).getKeysType().asCQL3Type();
            CQL3Type values = ((MapType<?, ?>) type).getValuesType().asCQL3Type();
            int offset = 0;
            for (int i = 0; i < size; i++)
            {
                if (i > 0)
                    target.append(", ");
                ByteBuffer element = CollectionSerializer.readValue(buffer, ByteBufferAccessor.instance, offset);
                offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance);
                target.append(keys.toCQLLiteral(element));
                target.append(": ");
                element = CollectionSerializer.readValue(buffer, ByteBufferAccessor.instance, offset);
                offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance);
                target.append(values.toCQLLiteral(element));
            }
        }

        private static void generateSetOrListCQLLiteral(ByteBuffer buffer, StringBuilder target, int size, CQL3Type elements)
        {
            int offset = 0;
            for (int i = 0; i < size; i++)
            {
                if (i > 0)
                    target.append(", ");
                ByteBuffer element = CollectionSerializer.readValue(buffer, ByteBufferAccessor.instance, offset);
                offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance);
                target.append(elements.toCQLLiteral(element));
            }
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Collection))
                return false;

            Collection that = (Collection)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            boolean isFrozen = !this.type.isMultiCell();
            StringBuilder sb = new StringBuilder(isFrozen ? "frozen<" : "");
            switch (type.kind)
            {
                case LIST:
                    AbstractType<?> listType = ((ListType<?>) type).getElementsType();
                    sb.append("list<").append(listType.asCQL3Type());
                    break;
                case SET:
                    AbstractType<?> setType = ((SetType<?>) type).getElementsType();
                    sb.append("set<").append(setType.asCQL3Type());
                    break;
                case MAP:
                    AbstractType<?> keysType = ((MapType<?, ?>) type).getKeysType();
                    AbstractType<?> valuesType = ((MapType<?, ?>) type).getValuesType();
                    sb.append("map<").append(keysType.asCQL3Type()).append(", ").append(valuesType.asCQL3Type());
                    break;
                default:
                    throw new AssertionError();
            }
            sb.append('>');
            if (isFrozen)
                sb.append('>');
            return sb.toString();
        }
    }

    public static class UserDefined implements CQL3Type
    {
        // Keeping this separatly from type just to simplify toString()
        private final String name;
        private final UserType type;

        private UserDefined(String name, UserType type)
        {
            this.name = name;
            this.type = type;
        }

        public static UserDefined create(UserType type)
        {
            return new UserDefined(UTF8Type.instance.compose(type.name), type);
        }

        public boolean isUDT()
        {
            return true;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer)
        {
            if (buffer == null)
                return "null";


            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            target.append('{');
            for (int i = 0; i < type.size(); i++)
            {
                // we allow the input to have less fields than declared so as to support field addition.
                if (!buffer.hasRemaining())
                    break;

                if (buffer.remaining() < 4)
                    throw new MarshalException(String.format("Not enough bytes to read size of %dth field %s", i, type.fieldName(i)));

                int size = buffer.getInt();

                if (i > 0)
                    target.append(", ");

                target.append(ColumnIdentifier.maybeQuote(type.fieldNameAsString(i)));
                target.append(": ");

                // size < 0 means null value
                if (size < 0)
                {
                    target.append("null");
                    continue;
                }

                if (buffer.remaining() < size)
                    throw new MarshalException(String.format("Not enough bytes to read %dth field %s", i, type.fieldName(i)));

                ByteBuffer field = ByteBufferUtil.readBytes(buffer, size);
                target.append(type.fieldType(i).asCQL3Type().toCQLLiteral(field));
            }
            target.append('}');
            return target.toString();
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof UserDefined))
                return false;

            UserDefined that = (UserDefined)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            if (type.isMultiCell())
                return ColumnIdentifier.maybeQuote(name);
            else
                return "frozen<" + ColumnIdentifier.maybeQuote(name) + '>';
        }
    }

    public static class Tuple implements CQL3Type
    {
        private final TupleType type;

        private Tuple(TupleType type)
        {
            this.type = type;
        }

        public static Tuple create(TupleType type)
        {
            return new Tuple(type);
        }

        public TupleType getType()
        {
            return type;
        }

        public String toCQLLiteral(ByteBuffer buffer)
        {
            if (buffer == null)
                return "null";

            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            target.append('(');
            boolean first = true;
            for (int i = 0; i < type.size(); i++)
            {
                // we allow the input to have less fields than declared so as to support field addition.
                if (!buffer.hasRemaining())
                    break;

                if (buffer.remaining() < 4)
                    throw new MarshalException(String.format("Not enough bytes to read size of %dth component", i));

                int size = buffer.getInt();

                if (first)
                    first = false;
                else
                    target.append(", ");

                // size < 0 means null value
                if (size < 0)
                {
                    target.append("null");
                    continue;
                }

                if (buffer.remaining() < size)
                    throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

                ByteBuffer field = ByteBufferUtil.readBytes(buffer, size);
                target.append(type.type(i).asCQL3Type().toCQLLiteral(field));
            }
            target.append(')');
            return target.toString();
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Tuple))
                return false;

            Tuple that = (Tuple)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return toString(true);
        }

        public String toString(boolean withFrozen)
        {
            StringBuilder sb = new StringBuilder();
            if (withFrozen)
                sb.append("frozen<");
            sb.append("tuple<");
            for (int i = 0; i < type.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(type.type(i).asCQL3Type());
            }
            sb.append('>');
            if (withFrozen)
                sb.append('>');

            return sb.toString();
        }
    }

    public static class Vector implements CQL3Type
    {
        private final VectorType<?> type;

        public Vector(VectorType<?> type)
        {
            this.type = type;
        }

        public Vector(AbstractType<?> elementType, int dimensions)
        {
            this.type = VectorType.getInstance(elementType, dimensions);
        }

        public boolean isVector()
        {
            return true;
        }

        @Override
        public VectorType<?> getType()
        {
            return type;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer)
        {
            if (type.isNull(buffer))
                return "null";
            buffer = buffer.duplicate();
            CQL3Type elementType = type.elementType.asCQL3Type();
            List<ByteBuffer> values = getType().split(buffer);
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = 0; i < values.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(elementType.toCQLLiteral(values.get(i)));
            }
            sb.append(']');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Vector vector = (Vector) o;
            return Objects.equals(type, vector.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("vector<").append(type.elementType.asCQL3Type()).append(", ").append(type.dimension).append('>');
            return sb.toString();
        }
    }

    // For UserTypes, we need to know the current keyspace to resolve the
    // actual type used, so Raw is a "not yet prepared" CQL3Type.
    public abstract class Raw
    {
        protected final boolean frozen;

        protected Raw(boolean frozen)
        {
            this.frozen = frozen;
        }

        public abstract boolean supportsFreezing();

        public boolean isFrozen()
        {
            return this.frozen;
        }

        public boolean isDuration()
        {
            return false;
        }

        public boolean isCounter()
        {
            return false;
        }

        public boolean isUDT()
        {
            return false;
        }

        public boolean isTuple()
        {
            return false;
        }

        public boolean isImplicitlyFrozen()
        {
            return isTuple() || isVector();
        }

        public boolean isVector()
        {
            return false;
        }

        public String keyspace()
        {
            return null;
        }

        public Raw freeze()
        {
            String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", this);
            throw new InvalidRequestException(message);
        }

        public abstract void validate(ClientState state, String name);

        public CQL3Type prepare(String keyspace)
        {
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
            if (ksm == null)
                throw new ConfigurationException(String.format("Keyspace %s doesn't exist", keyspace));
            return prepare(keyspace, ksm.types);
        }

        public abstract CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException;

        public CQL3Type prepareInternal(String keyspace, Types udts) throws InvalidRequestException
        {
            return prepare(keyspace, udts);
        }

        public boolean referencesUserType(String name)
        {
            return false;
        }

        public static Raw from(CQL3Type type)
        {
            return new RawType(type, false);
        }

        public static Raw userType(UTName name)
        {
            return new RawUT(name, false);
        }

        public static Raw map(CQL3Type.Raw t1, CQL3Type.Raw t2)
        {
            return new RawCollection(CollectionType.Kind.MAP, t1, t2, false);
        }

        public static Raw list(CQL3Type.Raw t)
        {
            return new RawCollection(CollectionType.Kind.LIST, null, t, false);
        }

        public static Raw set(CQL3Type.Raw t)
        {
            return new RawCollection(CollectionType.Kind.SET, null, t, false);
        }

        public static Raw tuple(List<CQL3Type.Raw> ts)
        {
            return new RawTuple(ts);
        }

        public static Raw vector(CQL3Type.Raw t, int dimension)
        {
            return new RawVector(t, dimension);
        }

        private static class RawType extends Raw
        {
            private final CQL3Type type;

            private RawType(CQL3Type type, boolean frozen)
            {
                super(frozen);
                this.type = type;
            }

            @Override
            public void validate(ClientState state, String name)
            {
                if (type.isVector())
                {
                    int dimensions = ((Vector) type).getType().dimension;
                    Guardrails.vectorDimensions.guard(dimensions, name, false, state);
                }
            }

            public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException
            {
                return type;
            }

            public boolean supportsFreezing()
            {
                return false;
            }

            public boolean isCounter()
            {
                return type == Native.COUNTER;
            }

            public boolean isDuration()
            {
                return type == Native.DURATION;
            }

            @Override
            public String toString()
            {
                return type.toString();
            }
        }

        private static class RawCollection extends Raw
        {
            private final CollectionType.Kind kind;
            private final CQL3Type.Raw keys;
            private final CQL3Type.Raw values;

            private RawCollection(CollectionType.Kind kind, CQL3Type.Raw keys, CQL3Type.Raw values, boolean frozen)
            {
                super(frozen);
                this.kind = kind;
                this.keys = keys;
                this.values = values;
            }

            @Override
            public RawCollection freeze()
            {
                CQL3Type.Raw frozenKeys =
                    null != keys && keys.supportsFreezing()
                  ? keys.freeze()
                  : keys;

                CQL3Type.Raw frozenValues =
                    null != values && values.supportsFreezing()
                  ? values.freeze()
                  : values;

                return new RawCollection(kind, frozenKeys, frozenValues, true);
            }

            public boolean supportsFreezing()
            {
                return true;
            }

            public boolean isCollection()
            {
                return true;
            }

            @Override
            public void validate(ClientState state, String name)
            {
                if (keys != null)
                    keys.validate(state, name);

                if (values != null)
                    values.validate(state, name);
            }

            public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException
            {
                return prepare(keyspace, udts, false);
            }

            public CQL3Type prepareInternal(String keyspace, Types udts)
            {
                return prepare(keyspace, udts, true);
            }

            public CQL3Type prepare(String keyspace, Types udts, boolean isInternal) throws InvalidRequestException
            {
                assert values != null : "Got null values type for a collection";

                if (!frozen && values.supportsFreezing() && !values.frozen)
                    throwNestedNonFrozenError(values);

                // we represent supercolumns as maps, internally, and we do allow counters in supercolumns. Thus,
                // for internal type parsing (think schema) we have to make an exception and allow counters as (map) values
                if (values.isCounter() && !isInternal)
                    throw new InvalidRequestException("Counters are not allowed inside collections: " + this);

                if (values.isDuration() && kind == Kind.SET)
                    throw new InvalidRequestException("Durations are not allowed inside sets: " + this);

                if (keys != null)
                {
                    if (keys.isCounter())
                        throw new InvalidRequestException("Counters are not allowed inside collections: " + this);
                    if (keys.isDuration())
                        throw new InvalidRequestException("Durations are not allowed as map keys: " + this);
                    if (!frozen && keys.supportsFreezing() && !keys.frozen)
                        throwNestedNonFrozenError(keys);
                }

                AbstractType<?> valueType = values.prepare(keyspace, udts).getType();
                switch (kind)
                {
                    case LIST:
                        return new Collection(ListType.getInstance(valueType, !frozen));
                    case SET:
                        return new Collection(SetType.getInstance(valueType, !frozen));
                    case MAP:
                        assert keys != null : "Got null keys type for a collection";
                        return new Collection(MapType.getInstance(keys.prepare(keyspace, udts).getType(), valueType, !frozen));
                }
                throw new AssertionError();
            }

            private void throwNestedNonFrozenError(Raw innerType)
            {
                if (innerType instanceof RawCollection)
                    throw new InvalidRequestException("Non-frozen collections are not allowed inside collections: " + this);
                else if (innerType.isUDT())
                    throw new InvalidRequestException("Non-frozen UDTs are not allowed inside collections: " + this);
            }

            public boolean referencesUserType(String name)
            {
                return (keys != null && keys.referencesUserType(name)) || values.referencesUserType(name);
            }

            @Override
            public String toString()
            {
                String start = frozen? "frozen<" : "";
                String end = frozen ? ">" : "";
                switch (kind)
                {
                    case LIST: return start + "list<" + values + '>' + end;
                    case SET:  return start + "set<" + values + '>' + end;
                    case MAP:  return start + "map<" + keys + ", " + values + '>' + end;
                }
                throw new AssertionError();
            }
        }

        private static class RawVector extends Raw
        {
            private final CQL3Type.Raw element;
            private final int dimension;

            private RawVector(Raw element, int dimension)
            {
                super(true);
                this.element = element;
                this.dimension = dimension;
            }

            @Override
            public boolean isVector()
            {
                return true;
            }

            @Override
            public boolean supportsFreezing()
            {
                return true;
            }

            @Override
            public Raw freeze()
            {
                return this;
            }

            @Override
            public void validate(ClientState state, String name)
            {
                Guardrails.vectorDimensions.guard(dimension, name, false, state);
            }

            @Override
            public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException
            {
                CQL3Type type = element.prepare(keyspace, udts);
                return new Vector(type.getType(), dimension);
            }

            @Override
            public String toString()
            {
                return "vector<" + element.toString() + ", " + dimension + '>';
            }
        }

        private static class RawUT extends Raw
        {
            private final UTName name;

            private RawUT(UTName name, boolean frozen)
            {
                super(frozen);
                this.name = name;
            }

            public String keyspace()
            {
                return name.getKeyspace();
            }

            @Override
            public RawUT freeze()
            {
                return new RawUT(name, true);
            }

            @Override
            public void validate(ClientState state, String name)
            {
                // nothing to do here
            }

            public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException
            {
                if (name.hasKeyspace())
                {
                    // The provided keyspace is the one of the current statement this is part of. If it's different from the keyspace of
                    // the UTName, we reject since we want to limit user types to their own keyspace (see #6643)
                    if (!keyspace.equals(name.getKeyspace()))
                        throw new InvalidRequestException(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; "
                                                                        + "user types can only be used in the keyspace they are defined in",
                                                                        keyspace, name.getKeyspace()));
                }
                else
                {
                    name.setKeyspace(keyspace);
                }

                UserType type = udts.getNullable(name.getUserTypeName());
                if (type == null)
                    throw new InvalidRequestException("Unknown type " + name);

                if (frozen)
                    type = type.freeze();
                return new UserDefined(name.toString(), type);
            }

            public boolean referencesUserType(String name)
            {
                return this.name.getStringTypeName().equals(name);
            }

            public boolean supportsFreezing()
            {
                return true;
            }

            public boolean isUDT()
            {
                return true;
            }

            @Override
            public String toString()
            {
                if (frozen)
                    return "frozen<" + name.toString() + '>';
                else
                    return name.toString();
            }
        }

        private static class RawTuple extends Raw
        {
            private final List<CQL3Type.Raw> types;

            private RawTuple(List<CQL3Type.Raw> types)
            {
                super(true);
                this.types = types.stream()
                                  .map(t -> t.supportsFreezing() ? t.freeze() : t)
                                  .collect(toList());
            }

            public boolean supportsFreezing()
            {
                return true;
            }

            @Override
            public RawTuple freeze()
            {
                return this;
            }

            @Override
            public void validate(ClientState state, String name)
            {
                for (CQL3Type.Raw t : types)
                    t.validate(state, name);
            }

            public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException
            {
                List<AbstractType<?>> ts = new ArrayList<>(types.size());
                for (CQL3Type.Raw t : types)
                {
                    if (t.isCounter())
                        throw new InvalidRequestException("Counters are not allowed inside tuples");

                    ts.add(t.prepare(keyspace, udts).getType());
                }
                return new Tuple(new TupleType(ts));
            }

            public boolean isTuple()
            {
                return true;
            }

            public boolean referencesUserType(String name)
            {
                return types.stream().anyMatch(t -> t.referencesUserType(name));
            }

            @Override
            public String toString()
            {
                StringBuilder sb = new StringBuilder();
                sb.append("tuple<");
                for (int i = 0; i < types.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(types.get(i));
                }
                sb.append('>');
                return sb.toString();
            }
        }
    }
}
