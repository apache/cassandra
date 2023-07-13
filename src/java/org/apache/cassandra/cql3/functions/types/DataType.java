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
package org.apache.cassandra.cql3.functions.types;

import java.util.*;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Data types supported by cassandra.
 */
public abstract class DataType
{

    /**
     * The CQL type name.
     */
    public enum Name
    {
        CUSTOM(0),
        ASCII(1),
        BIGINT(2),
        BLOB(3),
        BOOLEAN(4),
        COUNTER(5),
        DECIMAL(6),
        DOUBLE(7),
        FLOAT(8),
        INT(9),
        TEXT(10)
        {
            @Override
            public boolean isCompatibleWith(Name that)
            {
                return this == that || that == VARCHAR;
            }
        },
        TIMESTAMP(11),
        UUID(12),
        VARCHAR(13)
        {
            @Override
            public boolean isCompatibleWith(Name that)
            {
                return this == that || that == TEXT;
            }
        },
        VARINT(14),
        TIMEUUID(15),
        INET(16),
        DATE(17, ProtocolVersion.V4),
        TIME(18, ProtocolVersion.V4),
        SMALLINT(19, ProtocolVersion.V4),
        TINYINT(20, ProtocolVersion.V4),
        DURATION(21, ProtocolVersion.V5),
        LIST(32),
        MAP(33),
        SET(34),
        UDT(48, ProtocolVersion.V3),
        TUPLE(49, ProtocolVersion.V3),
        VECTOR(50, ProtocolVersion.V5);

        final int protocolId;

        final ProtocolVersion minProtocolVersion;

        private static final Name[] nameToIds;

        static
        {
            int maxCode = -1;
            for (Name name : Name.values()) maxCode = Math.max(maxCode, name.protocolId);
            nameToIds = new Name[maxCode + 1];
            for (Name name : Name.values())
            {
                if (nameToIds[name.protocolId] != null) throw new IllegalStateException("Duplicate Id");
                nameToIds[name.protocolId] = name;
            }
        }

        Name(int protocolId)
        {
            this(protocolId, ProtocolVersion.V1);
        }

        Name(int protocolId, ProtocolVersion minProtocolVersion)
        {
            this.protocolId = protocolId;
            this.minProtocolVersion = minProtocolVersion;
        }

        /**
         * Return {@code true} if the provided Name is equal to this one, or if they are aliases for
         * each other, and {@code false} otherwise.
         *
         * @param that the Name to compare with the current one.
         * @return {@code true} if the provided Name is equal to this one, or if they are aliases for
         * each other, and {@code false} otherwise.
         */
        public boolean isCompatibleWith(Name that)
        {
            return this == that;
        }

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

    private static final Map<Name, DataType> primitiveTypeMap =
    new EnumMap<>(Name.class);

    static
    {
        primitiveTypeMap.put(Name.ASCII, new DataType.NativeType(Name.ASCII));
        primitiveTypeMap.put(Name.BIGINT, new DataType.NativeType(Name.BIGINT));
        primitiveTypeMap.put(Name.BLOB, new DataType.NativeType(Name.BLOB));
        primitiveTypeMap.put(Name.BOOLEAN, new DataType.NativeType(Name.BOOLEAN));
        primitiveTypeMap.put(Name.COUNTER, new DataType.NativeType(Name.COUNTER));
        primitiveTypeMap.put(Name.DECIMAL, new DataType.NativeType(Name.DECIMAL));
        primitiveTypeMap.put(Name.DOUBLE, new DataType.NativeType(Name.DOUBLE));
        primitiveTypeMap.put(Name.FLOAT, new DataType.NativeType(Name.FLOAT));
        primitiveTypeMap.put(Name.INET, new DataType.NativeType(Name.INET));
        primitiveTypeMap.put(Name.INT, new DataType.NativeType(Name.INT));
        primitiveTypeMap.put(Name.TEXT, new DataType.NativeType(Name.TEXT));
        primitiveTypeMap.put(Name.TIMESTAMP, new DataType.NativeType(Name.TIMESTAMP));
        primitiveTypeMap.put(Name.UUID, new DataType.NativeType(Name.UUID));
        primitiveTypeMap.put(Name.VARCHAR, new DataType.NativeType(Name.VARCHAR));
        primitiveTypeMap.put(Name.VARINT, new DataType.NativeType(Name.VARINT));
        primitiveTypeMap.put(Name.TIMEUUID, new DataType.NativeType(Name.TIMEUUID));
        primitiveTypeMap.put(Name.SMALLINT, new DataType.NativeType(Name.SMALLINT));
        primitiveTypeMap.put(Name.TINYINT, new DataType.NativeType(Name.TINYINT));
        primitiveTypeMap.put(Name.DATE, new DataType.NativeType(Name.DATE));
        primitiveTypeMap.put(Name.TIME, new DataType.NativeType(Name.TIME));
        primitiveTypeMap.put(Name.DURATION, new DataType.NativeType(Name.DURATION));
    }

    protected final DataType.Name name;

    protected DataType(DataType.Name name)
    {
        this.name = name;
    }

    /**
     * Returns the ASCII type.
     *
     * @return The ASCII type.
     */
    public static DataType ascii()
    {
        return primitiveTypeMap.get(Name.ASCII);
    }

    /**
     * Returns the BIGINT type.
     *
     * @return The BIGINT type.
     */
    public static DataType bigint()
    {
        return primitiveTypeMap.get(Name.BIGINT);
    }

    /**
     * Returns the BLOB type.
     *
     * @return The BLOB type.
     */
    public static DataType blob()
    {
        return primitiveTypeMap.get(Name.BLOB);
    }

    /**
     * Returns the BOOLEAN type.
     *
     * @return The BOOLEAN type.
     */
    public static DataType cboolean()
    {
        return primitiveTypeMap.get(Name.BOOLEAN);
    }

    /**
     * Returns the COUNTER type.
     *
     * @return The COUNTER type.
     */
    public static DataType counter()
    {
        return primitiveTypeMap.get(Name.COUNTER);
    }

    /**
     * Returns the DECIMAL type.
     *
     * @return The DECIMAL type.
     */
    public static DataType decimal()
    {
        return primitiveTypeMap.get(Name.DECIMAL);
    }

    /**
     * Returns the DOUBLE type.
     *
     * @return The DOUBLE type.
     */
    public static DataType cdouble()
    {
        return primitiveTypeMap.get(Name.DOUBLE);
    }

    /**
     * Returns the FLOAT type.
     *
     * @return The FLOAT type.
     */
    public static DataType cfloat()
    {
        return primitiveTypeMap.get(Name.FLOAT);
    }

    /**
     * Returns the INET type.
     *
     * @return The INET type.
     */
    public static DataType inet()
    {
        return primitiveTypeMap.get(Name.INET);
    }

    /**
     * Returns the TINYINT type.
     *
     * @return The TINYINT type.
     */
    public static DataType tinyint()
    {
        return primitiveTypeMap.get(Name.TINYINT);
    }

    /**
     * Returns the SMALLINT type.
     *
     * @return The SMALLINT type.
     */
    public static DataType smallint()
    {
        return primitiveTypeMap.get(Name.SMALLINT);
    }

    /**
     * Returns the INT type.
     *
     * @return The INT type.
     */
    public static DataType cint()
    {
        return primitiveTypeMap.get(Name.INT);
    }

    /**
     * Returns the TEXT type.
     *
     * @return The TEXT type.
     */
    public static DataType text()
    {
        return primitiveTypeMap.get(Name.TEXT);
    }

    /**
     * Returns the TIMESTAMP type.
     *
     * @return The TIMESTAMP type.
     */
    public static DataType timestamp()
    {
        return primitiveTypeMap.get(Name.TIMESTAMP);
    }

    /**
     * Returns the DATE type.
     *
     * @return The DATE type.
     */
    public static DataType date()
    {
        return primitiveTypeMap.get(Name.DATE);
    }

    /**
     * Returns the TIME type.
     *
     * @return The TIME type.
     */
    public static DataType time()
    {
        return primitiveTypeMap.get(Name.TIME);
    }

    /**
     * Returns the UUID type.
     *
     * @return The UUID type.
     */
    public static DataType uuid()
    {
        return primitiveTypeMap.get(Name.UUID);
    }

    /**
     * Returns the VARCHAR type.
     *
     * @return The VARCHAR type.
     */
    public static DataType varchar()
    {
        return primitiveTypeMap.get(Name.VARCHAR);
    }

    /**
     * Returns the VARINT type.
     *
     * @return The VARINT type.
     */
    public static DataType varint()
    {
        return primitiveTypeMap.get(Name.VARINT);
    }

    /**
     * Returns the TIMEUUID type.
     *
     * @return The TIMEUUID type.
     */
    public static DataType timeuuid()
    {
        return primitiveTypeMap.get(Name.TIMEUUID);
    }

    /**
     * Returns the type of lists of {@code elementType} elements.
     *
     * @param elementType the type of the list elements.
     * @param frozen      whether the list is frozen.
     * @return the type of lists of {@code elementType} elements.
     */
    public static CollectionType list(DataType elementType, boolean frozen)
    {
        return new DataType.CollectionType(Name.LIST, ImmutableList.of(elementType), frozen);
    }

    /**
     * Returns the type of "not frozen" lists of {@code elementType} elements.
     *
     * <p>This is a shorthand for {@code list(elementType, false);}.
     *
     * @param elementType the type of the list elements.
     * @return the type of "not frozen" lists of {@code elementType} elements.
     */
    public static CollectionType list(DataType elementType)
    {
        return list(elementType, false);
    }

    /**
     * Returns the type of sets of {@code elementType} elements.
     *
     * @param elementType the type of the set elements.
     * @param frozen      whether the set is frozen.
     * @return the type of sets of {@code elementType} elements.
     */
    public static CollectionType set(DataType elementType, boolean frozen)
    {
        return new DataType.CollectionType(Name.SET, ImmutableList.of(elementType), frozen);
    }

    /**
     * Returns the type of "not frozen" sets of {@code elementType} elements.
     *
     * <p>This is a shorthand for {@code set(elementType, false);}.
     *
     * @param elementType the type of the set elements.
     * @return the type of "not frozen" sets of {@code elementType} elements.
     */
    public static CollectionType set(DataType elementType)
    {
        return set(elementType, false);
    }

    /**
     * Returns the type of maps of {@code keyType} to {@code valueType} elements.
     *
     * @param keyType   the type of the map keys.
     * @param valueType the type of the map values.
     * @param frozen    whether the map is frozen.
     * @return the type of maps of {@code keyType} to {@code valueType} elements.
     */
    public static CollectionType map(DataType keyType, DataType valueType, boolean frozen)
    {
        return new DataType.CollectionType(Name.MAP, ImmutableList.of(keyType, valueType), frozen);
    }

    /**
     * Returns the type of "not frozen" maps of {@code keyType} to {@code valueType} elements.
     *
     * <p>This is a shorthand for {@code map(keyType, valueType, false);}.
     *
     * @param keyType   the type of the map keys.
     * @param valueType the type of the map values.
     * @return the type of "not frozen" maps of {@code keyType} to {@code valueType} elements.
     */
    public static CollectionType map(DataType keyType, DataType valueType)
    {
        return map(keyType, valueType, false);
    }

    /**
     * Returns the type of vector of {@code elementType} elements with {@code dimensions} dimensions.
     *
     * @param elementType the type of the vector elements.
     * @param dimensions  the number of dimensions of the vector.
     * @return the type of vectors of {@code elementType} elements and {@code dimensions} dimensions.
     */
    public static VectorType vector(DataType elementType, int dimensions)
    {
        return new VectorType(elementType, dimensions);
    }

    /**
     * Returns a Custom type.
     *
     * <p>A custom type is defined by the name of the class used on the Cassandra side to implement
     * it. Note that the support for custom types by the driver is limited.
     *
     * <p>The use of custom types is rarely useful and is thus not encouraged.
     *
     * @param typeClassName the server-side fully qualified class name for the type.
     * @return the custom type for {@code typeClassName}.
     */
    public static DataType.CustomType custom(String typeClassName)
    {
        if (typeClassName == null) throw new NullPointerException();
        return new DataType.CustomType(Name.CUSTOM, typeClassName);
    }

    /**
     * Returns the Duration type, introduced in Cassandra 3.10.
     *
     * <p>Note that a Duration type does not have a native representation in CQL, and technically, is
     * merely a special {@link DataType#custom(String) custom type} from the driver's point of view.
     *
     * @return the Duration type. The returned instance is a singleton.
     */
    public static DataType duration()
    {
        return primitiveTypeMap.get(Name.DURATION);
    }

    /**
     * Returns the name of that type.
     *
     * @return the name of that type.
     */
    public Name getName()
    {
        return name;
    }

    /**
     * Returns whether this data type is frozen.
     *
     * <p>This applies to User Defined Types, tuples and nested collections. Frozen types are
     * serialized as a single value in Cassandra's storage engine, whereas non-frozen types are stored
     * in a form that allows updates to individual subfields.
     *
     * @return whether this data type is frozen.
     */
    public abstract boolean isFrozen();

    /**
     * Returns whether this data type represent a CQL {@link
     * DataType.CollectionType collection type}, that is, a list, set or map.
     *
     * @return whether this data type name represent the name of a collection type.
     */
    public boolean isCollection()
    {
        return this instanceof CollectionType;
    }

    /**
     * Returns the type arguments of this type.
     *
     * <p>Note that only the collection types (LIST, MAP, SET) have type arguments. For the other
     * types, this will return an empty list.
     *
     * <p>For the collection types:
     *
     * <ul>
     * <li>For lists and sets, this method returns one argument, the type of the elements.
     * <li>For maps, this method returns two arguments, the first one is the type of the map keys,
     * the second one is the type of the map values.
     * </ul>
     *
     * @return an immutable list containing the type arguments of this type.
     */
    public List<DataType> getTypeArguments()
    {
        return Collections.emptyList();
    }

    /**
     * Returns a String representation of this data type suitable for inclusion as a parameter type in
     * a function or aggregate signature.
     *
     * <p>In such places, the String representation might vary from the canonical one as returned by
     * {@link #toString()}; e.g. the {@code frozen} keyword is not accepted.
     *
     * @return a String representation of this data type suitable for inclusion as a parameter type in
     * a function or aggregate signature.
     */
    public String asFunctionParameterString()
    {
        return toString();
    }

    /**
     * Instances of this class represent CQL native types, also known as CQL primitive types.
     */
    public static class NativeType extends DataType
    {

        private NativeType(DataType.Name name)
        {
            super(name);
        }

        @Override
        public boolean isFrozen()
        {
            return false;
        }

        @Override
        public final int hashCode()
        {
            return (name == Name.TEXT) ? Name.VARCHAR.hashCode() : name.hashCode();
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof DataType.NativeType)) return false;

            NativeType that = (DataType.NativeType) o;
            return this.name.isCompatibleWith(that.name);
        }

        @Override
        public String toString()
        {
            return name.toString();
        }
    }

    /**
     * Instances of this class represent collection types, that is, lists, sets or maps.
     */
    public static class CollectionType extends DataType
    {

        private final List<DataType> typeArguments;
        private final boolean frozen;

        private CollectionType(DataType.Name name, List<DataType> typeArguments, boolean frozen)
        {
            super(name);
            this.typeArguments = typeArguments;
            this.frozen = frozen;
        }

        @Override
        public boolean isFrozen()
        {
            return frozen;
        }

        @Override
        public List<DataType> getTypeArguments()
        {
            return typeArguments;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(name, typeArguments);
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof DataType.CollectionType)) return false;

            DataType.CollectionType d = (DataType.CollectionType) o;
            return name == d.name && typeArguments.equals(d.typeArguments);
        }

        @Override
        public String toString()
        {
            if (name == Name.MAP)
            {
                String template = frozen ? "frozen<%s<%s, %s>>" : "%s<%s, %s>";
                return String.format(template, name, typeArguments.get(0), typeArguments.get(1));
            }
            else
            {
                String template = frozen ? "frozen<%s<%s>>" : "%s<%s>";
                return String.format(template, name, typeArguments.get(0));
            }
        }

        @Override
        public String asFunctionParameterString()
        {
            if (name == Name.MAP)
            {
                String template = "%s<%s, %s>";
                return String.format(
                template,
                name,
                typeArguments.get(0).asFunctionParameterString(),
                typeArguments.get(1).asFunctionParameterString());
            }
            else
            {
                String template = "%s<%s>";
                return String.format(template, name, typeArguments.get(0).asFunctionParameterString());
            }
        }
    }

    /**
     * A "custom" type is a type that cannot be expressed as a CQL type.
     *
     * <p>Each custom type is merely identified by the fully qualified {@code
     * #getCustomTypeClassName() class name} that represents this type server-side.
     *
     * <p>The driver provides a minimal support for such types through instances of this class.
     *
     * <p>A codec for custom types can be obtained via {@link TypeCodec#custom(DataType.CustomType)}.
     */
    public static class CustomType extends DataType
    {

        private final String customClassName;

        private CustomType(DataType.Name name, String className)
        {
            super(name);
            this.customClassName = className;
        }

        @Override
        public boolean isFrozen()
        {
            return false;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(name, customClassName);
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof DataType.CustomType)) return false;

            DataType.CustomType d = (DataType.CustomType) o;
            return name == d.name && Objects.equals(customClassName, d.customClassName);
        }

        @Override
        public String toString()
        {
            return String.format("'%s'", customClassName);
        }
    }
}
