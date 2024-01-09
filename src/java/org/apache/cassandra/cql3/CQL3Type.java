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
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.stream.Collectors.toList;

public interface CQL3Type
{
    Logger logger = LoggerFactory.getLogger(CQL3Type.class);

    default boolean isCollection()
    {
        return false;
    }

    default boolean isUDT()
    {
        return false;
    }

    AbstractType<?> getType();

    /**
     * Generates CQL literal from a binary value of this type.
     *  @param bytes the value to convert to a CQL literal. This value must be
     * serialized with {@code version} of the native protocol.
     * @param version the native protocol version in which {@code buffer} is encoded.
     */
    String toCQLLiteral(ByteBuffer bytes, ProtocolVersion version);


    /**
     * Generates the string representation of this type, with options regarding the addition of frozen.
     * <p>
     * This exists only to provide proper {@code toString} and {@link #toSchemaString} implementations and would
     * be protected if this was an abstract class rather than an interface (but we cannot change it to an abstract
     * class easily since {@link Native} is an enum).
     *
     * @param alreadyFrozen whether the type is within an already frozen type. If {@code true}, and unless
     * {@code forceFrozen} is also {@code true} and this type is a complex frozen type itself, the returned
     * representation will omit the 'frozen<>' prefix. Otherwise, the 'frozen<>' prefix will be included if this type
     * is (complex and) frozen.
     * @param forceFrozen force the repetition of 'frozen<>' prefixes before complex frozen types, even if the
     * context is already frozen. See {@link #toSchemaString} as to why we need this.
     * @return the type string representation (with or without a 'frozen<>' based on {@code alreadyFrozen} and
     * {@code forceFrozen}).
     */
    String toString(boolean alreadyFrozen, boolean forceFrozen);

    /**
     * Generate the string representation of this type, like {@code toString}, but where the 'frozen<>' is
     * repeated for every frozen complex types, even if it is already present as at higher level (and is such
     * unnecessary). In other words, this will generate {@code frozen<list<frozen<set<int>>>>} when
     * {@code toString} only generates {@code frozen<list<set<int>>>}.
     * <p>
     * This generally shouldn't be used since it is a less readable, wasteful representation, but we use it for
     * column types in the schema for the following historical reason: before DB-3084, the default {@code toString}
     * representation was behaving like this method, and drivers have indirectly "relied" on it in the sense that while
     * types on the drivers side have a {@code isFrozen()} method, it only returns {@code true} if the type has an
     * explicit "frozen<>" prefix just before it. Meaning that reading {@code frozen<list<set<int>>>} in the schema,
     * the driver (at least the java one) will return that the list is frozen, but not the set.
     * <p>
     * So to avoid risks in breaking external consumers, we preserve the verbose, if wasteful, representation in the
     * schema. We may remove this when we've coordinated fixing the drivers side to always set their {@code isFrozen} to
     * {@code true} for subtypes of frozen types, no matter what the representation says.
     */
    default String toSchemaString()
    {
        return toString(false, true);
    }

    enum Native implements CQL3Type
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

        @Override
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
        public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version)
        {
            return type.getSerializer().toCQLLiteral(buffer);
        }

        @Override
        public String toString(boolean alreadyFrozen, boolean forceFrozen)
        {
            return super.toString().toLowerCase();
        }

        @Override
        public String toString()
        {
            return toString(false, false);
        }
    }

    class Custom implements CQL3Type
    {
        private final AbstractType<?> type;

        public Custom(AbstractType<?> type)
        {
            this.type = type;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version)
        {
            // *always* use the 'blob' syntax to express custom types in CQL
            return Native.BLOB.toCQLLiteral(buffer, version);
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof Custom))
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
        public String toString(boolean alreadyFrozen, boolean forceFrozen)
        {
            return "'" + type + '\'';
        }

        @Override
        public String toString()
        {
            return toString(false, false);
        }
    }

    class Collection implements CQL3Type
    {
        private final CollectionType<?> type;

        public Collection(CollectionType<?> type)
        {
            this.type = type;
        }

        @Override
        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public boolean isCollection()
        {
            return true;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version)
        {
            if (buffer == null)
                return "null";

            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            int size = CollectionSerializer.readCollectionSize(buffer, ByteBufferAccessor.instance, version);
            buffer.position(buffer.position() + CollectionSerializer.sizeOfCollectionSize(size, version));

            switch (type.kind)
            {
                case LIST:
                    CQL3Type listElements = ((ListType<?>) type).getElementsType().asCQL3Type();
                    target.append('[');
                    generateSetOrListCQLLiteral(buffer, version, target, size, listElements);
                    target.append(']');
                    break;
                case SET:
                    CQL3Type setElements = ((SetType<?>) type).getElementsType().asCQL3Type();
                    target.append('{');
                    generateSetOrListCQLLiteral(buffer, version, target, size, setElements);
                    target.append('}');
                    break;
                case MAP:
                    target.append('{');
                    generateMapCQLLiteral(buffer, version, target, size);
                    target.append('}');
                    break;
            }
            return target.toString();
        }

        private void generateMapCQLLiteral(ByteBuffer buffer, ProtocolVersion version, StringBuilder target, int size)
        {
            CQL3Type keys = ((MapType<?, ?>) type).getKeysType().asCQL3Type();
            CQL3Type values = ((MapType<?, ?>) type).getValuesType().asCQL3Type();
            int offset = 0;
            for (int i = 0; i < size; i++)
            {
                if (i > 0)
                    target.append(", ");
                ByteBuffer element = CollectionSerializer.readValue(buffer, ByteBufferAccessor.instance, offset, version);
                offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance, version);
                target.append(keys.toCQLLiteral(element, version));
                target.append(": ");
                element = CollectionSerializer.readValue(buffer, ByteBufferAccessor.instance, offset, version);
                offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance, version);
                target.append(values.toCQLLiteral(element, version));
            }
        }

        private static void generateSetOrListCQLLiteral(ByteBuffer buffer, ProtocolVersion version, StringBuilder target, int size, CQL3Type elements)
        {
            int offset = 0;
            for (int i = 0; i < size; i++)
            {
                if (i > 0)
                    target.append(", ");
                ByteBuffer element = CollectionSerializer.readValue(buffer, ByteBufferAccessor.instance, offset, version);
                offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance, version);
                target.append(elements.toCQLLiteral(element, version));
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
        public String toString(boolean alreadyFrozen, boolean forceFrozen)
        {
            boolean addFrozen = !this.type.isMultiCell() && (!alreadyFrozen || forceFrozen);
            alreadyFrozen = !this.type.isMultiCell() || alreadyFrozen;
            StringBuilder sb = new StringBuilder(addFrozen ? "frozen<" : "");
            switch (type.kind)
            {
                case LIST:
                    AbstractType<?> listType = ((ListType<?>) type).getElementsType();
                    sb.append("list<").append(listType.asCQL3Type().toString(alreadyFrozen, forceFrozen));
                    break;
                case SET:
                    AbstractType<?> setType = ((SetType<?>) type).getElementsType();
                    sb.append("set<").append(setType.asCQL3Type().toString(alreadyFrozen, forceFrozen));
                    break;
                case MAP:
                    AbstractType<?> keysType = ((MapType<?, ?>) type).getKeysType();
                    AbstractType<?> valuesType = ((MapType<?, ?>) type).getValuesType();
                    sb.append("map<").append(keysType.asCQL3Type().toString(alreadyFrozen, forceFrozen))
                      .append(", ").append(valuesType.asCQL3Type().toString(alreadyFrozen, forceFrozen));
                    break;
                default:
                    throw new AssertionError();
            }
            sb.append('>');
            if (addFrozen)
                sb.append('>');
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return toString(false, false);
        }
    }

    class UserDefined implements CQL3Type
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

        @Override
        public boolean isUDT()
        {
            return true;
        }

        @Override
        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version)
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
                target.append(type.fieldType(i).asCQL3Type().toCQLLiteral(field, version));
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
        public String toString(boolean alreadyFrozen, boolean forceFrozen)
        {
            if (type.isMultiCell() || (alreadyFrozen && !forceFrozen))
                return ColumnIdentifier.maybeQuote(name);
            else
                return "frozen<" + ColumnIdentifier.maybeQuote(name) + '>';
        }

        @Override
        public String toString()
        {
            return toString(false, false);
        }
    }

    class Tuple implements CQL3Type
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

        @Override
        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version)
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
                target.append(type.type(i).asCQL3Type().toCQLLiteral(field, version));
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
        public String toString(boolean alreadyFrozen, boolean forceFrozen)
        {
            // As tuples are frozen by default, we don't output the frozen<> prefix even when frozen, unless forceFrozen
            // is explicitly provided. Note that in practice, the type should never be non-frozen unless forceFrozen
            // is called: if that were to happen, either:
            //  1. we'd have built a non-frozen tuple for something other than a dropped column
            //  2. or we'd be calling toString() for a dropped column type, rather that toSchemaString()
            // Both of which being bugs.
            assert forceFrozen || !type.isMultiCell() : type + " should have been frozen/non-multi-cell";
            boolean addFrozen = !type.isMultiCell() && forceFrozen;
            alreadyFrozen = !type.isMultiCell() || alreadyFrozen;
            StringBuilder sb = new StringBuilder(addFrozen ? "frozen<" : "");
            sb.append("tuple<");
            for (int i = 0; i < type.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(type.type(i).asCQL3Type().toString(alreadyFrozen, forceFrozen));
            }
            sb.append('>');
            if (addFrozen)
                sb.append('>');
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return toString(false, false);
        }
    }

    // For UserTypes, we need to know the current keyspace to resolve the
    // actual type used, so Raw is a "not yet prepared" CQL3Type.
    abstract class Raw
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

        public String keyspace()
        {
            return null;
        }

        public abstract void forEachUserType(Consumer<UTName> userTypeNameConsumer);

        public Raw freeze()
        {
            String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", this);
            throw new InvalidRequestException(message);
        }

        /**
         * Prepare this raw type given the current keyspace to obtain a proper {@link CQL3Type}.
         *
         * @param keyspace the keyspace the type is part of.
         * @return the prepared type.
         */
        public CQL3Type prepare(String keyspace)
        {
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
            if (ksm == null)
                throw new ConfigurationException(String.format("Keyspace %s doesn't exist", keyspace));
            return prepare(keyspace, ksm.types);
        }

        /**
         * Prepare this raw type given the current keyspace and its defined user types to obtain a proper
         * {@link CQL3Type}.
         *
         * @param keyspace the keyspace the type is part of.
         * @param udts user types defined in {@code keyspace}, which this type may rely on.
         * @return the prepared type.
         */
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

        public static Raw custom(String className)
        {
            return new RawCustom(className, false);
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

        private static class RawType extends Raw
        {
            private final CQL3Type type;

            private RawType(CQL3Type type, boolean frozen)
            {
                super(frozen);
                this.type = type;
            }

            @Override
            public CQL3Type prepare(String keyspace, Types udts)
            {
                return type;
            }

            @Override
            public boolean supportsFreezing()
            {
                return false;
            }

            @Override
            public boolean isCounter()
            {
                return type == Native.COUNTER;
            }

            @Override
            public boolean isDuration()
            {
                return type == Native.DURATION;
            }

            @Override
            public void forEachUserType(Consumer<UTName> userTypeNameConsumer)
            {
                // no-op
            }

            @Override
            public String toString()
            {
                return type.toString();
            }
        }

        private static class RawCustom extends Raw
        {
            private final String className;

            private RawCustom(String className, boolean frozen)
            {
                super(frozen);
                this.className = className;
            }

            @Override
            public boolean supportsFreezing()
            {
                // Technically, we cannot know if the resulting type may be frozen or not, so allow the frozen keyword,
                // but it might well basically do nothing when used.
                return true;
            }

            @Override
            public RawCustom freeze() throws InvalidRequestException
            {
                return new RawCustom(className, true);
            }

            @Override
            public CQL3Type prepare(String keyspace, Types udts)
            {
                AbstractType<?> type = TypeParser.parse(className);
                if (frozen)
                    type = type.freeze();
                return new Custom(type);
            }

            @Override
            public void forEachUserType(Consumer<UTName> userTypeNameConsumer)
            {
                // no-op
            }

            @Override
            public String toString()
            {
                return '"' + className + '"';
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

            @Override
            public boolean supportsFreezing()
            {
                return true;
            }

            @Override
            public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException
            {
                return prepare(keyspace, udts, false);
            }

            @Override
            public CQL3Type prepareInternal(String keyspace, Types udts)
            {
                return prepare(keyspace, udts, true);
            }

            public CQL3Type prepare(String keyspace, Types udts, boolean isInternal) throws InvalidRequestException
            {
                assert values != null : "Got null values type for a collection";

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

            @Override
            public boolean referencesUserType(String name)
            {
                return (keys != null && keys.referencesUserType(name)) || values.referencesUserType(name);
            }

            @Override
            public void forEachUserType(Consumer<UTName> userTypeNameConsumer)
            {
                if (keys != null)
                    keys.forEachUserType(userTypeNameConsumer);
                if (values != null)
                    values.forEachUserType(userTypeNameConsumer);
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

        private static class RawUT extends Raw
        {
            private final UTName name;

            private RawUT(UTName name, boolean frozen)
            {
                super(frozen);
                this.name = name;
            }

            @Override
            public String keyspace()
            {
                return name.getKeyspace();
            }

            @Override
            public void forEachUserType(Consumer<UTName> userTypeNameConsumer)
            {
                userTypeNameConsumer.accept(name);
            }

            @Override
            public RawUT freeze()
            {
                return new RawUT(name, true);
            }

            @Override
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

            @Override
            public boolean referencesUserType(String name)
            {
                return this.name.getStringTypeName().equals(name);
            }

            @Override
            public boolean supportsFreezing()
            {
                return true;
            }

            @Override
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

            @Override
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

            @Override
            public boolean isTuple()
            {
                return true;
            }

            @Override
            public boolean referencesUserType(String name)
            {
                return types.stream().anyMatch(t -> t.referencesUserType(name));
            }

            @Override
            public void forEachUserType(Consumer<UTName> userTypeNameConsumer)
            {
                types.forEach(t -> t.forEachUserType(userTypeNameConsumer));
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
