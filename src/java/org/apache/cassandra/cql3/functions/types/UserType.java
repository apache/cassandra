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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A User Defined Type (UDT).
 *
 * <p>A UDT is a essentially a named collection of fields (with a name and a type).
 */
public class UserType extends DataType implements Iterable<UserType.Field>
{

    private final String keyspace;
    private final String typeName;
    private final boolean frozen;
    private final ProtocolVersion protocolVersion;

    // can be null, if this object is being constructed from a response message
    // see Responses.Result.Rows.Metadata.decode()
    private final CodecRegistry codecRegistry;

    // Note that we don't expose the order of fields, from an API perspective this is a map
    // of String->Field, but internally we care about the order because the serialization format
    // of UDT expects a particular order.
    final Field[] byIdx;
    // For a given name, we can only have one field with that name, so we don't need a int[] in
    // practice. However, storing one element arrays save allocations in UDTValue.getAllIndexesOf
    // implementation.
    final Map<String, int[]> byName;

    private UserType(
    Name name,
    String keyspace,
    String typeName,
    boolean frozen,
    ProtocolVersion protocolVersion,
    CodecRegistry codecRegistry,
    Field[] byIdx,
    Map<String, int[]> byName)
    {
        super(name);
        this.keyspace = keyspace;
        this.typeName = typeName;
        this.frozen = frozen;
        this.protocolVersion = protocolVersion;
        this.codecRegistry = codecRegistry;
        this.byIdx = byIdx;
        this.byName = byName;
    }

    UserType(
    String keyspace,
    String typeName,
    boolean frozen,
    Collection<Field> fields,
    ProtocolVersion protocolVersion,
    CodecRegistry codecRegistry)
    {
        this(
        DataType.Name.UDT,
        keyspace,
        typeName,
        frozen,
        protocolVersion,
        codecRegistry,
        fields.toArray(new Field[fields.size()]),
        mapByName(fields));
    }

    private static ImmutableMap<String, int[]> mapByName(Collection<Field> fields)
    {
        ImmutableMap.Builder<String, int[]> builder = new ImmutableMap.Builder<>();
        int i = 0;
        for (Field field : fields)
        {
            builder.put(field.getName(), new int[]{ i });
            i += 1;
        }
        return builder.build();
    }

    /**
     * Returns a new empty value for this user type definition.
     *
     * @return an empty value for this user type definition.
     */
    public UDTValue newValue()
    {
        return new UDTValue(this);
    }

    /**
     * The name of the keyspace this UDT is part of.
     *
     * @return the name of the keyspace this UDT is part of.
     */
    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * The name of this user type.
     *
     * @return the name of this user type.
     */
    public String getTypeName()
    {
        return typeName;
    }

    /**
     * Returns the number of fields in this UDT.
     *
     * @return the number of fields in this UDT.
     */
    public int size()
    {
        return byIdx.length;
    }

    /**
     * Returns whether this UDT contains a given field.
     *
     * @param name the name to check. Note that {@code name} obey the usual CQL identifier rules: it
     *             should be quoted if it denotes a case sensitive identifier (you can use {@link
     *             Metadata#quote} for the quoting).
     * @return {@code true} if this UDT contains a field named {@code name}, {@code false} otherwise.
     */
    public boolean contains(String name)
    {
        return byName.containsKey(Metadata.handleId(name));
    }

    /**
     * Returns an iterator over the fields of this UDT.
     *
     * @return an iterator over the fields of this UDT.
     */
    @Override
    public Iterator<Field> iterator()
    {
        return Iterators.forArray(byIdx);
    }

    /**
     * Returns the type of a given field.
     *
     * @param name the name of the field. Note that {@code name} obey the usual CQL identifier rules:
     *             it should be quoted if it denotes a case sensitive identifier (you can use {@link
     *             Metadata#quote} for the quoting).
     * @return the type of field {@code name} if this UDT has a field of this name, {@code null}
     * otherwise.
     * @throws IllegalArgumentException if {@code name} is not a field of this UDT definition.
     */
    DataType getFieldType(String name)
    {
        int[] idx = byName.get(Metadata.handleId(name));
        if (idx == null)
            throw new IllegalArgumentException(name + " is not a field defined in this definition");

        return byIdx[idx[0]].getType();
    }

    @Override
    public boolean isFrozen()
    {
        return frozen;
    }

    public UserType copy(boolean newFrozen)
    {
        if (newFrozen == frozen)
        {
            return this;
        }
        else
        {
            return new UserType(
            name, keyspace, typeName, newFrozen, protocolVersion, codecRegistry, byIdx, byName);
        }
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + keyspace.hashCode();
        result = 31 * result + typeName.hashCode();
        result = 31 * result + Arrays.hashCode(byIdx);
        return result;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UserType)) return false;

        UserType other = (UserType) o;

        // Note: we don't test byName because it's redundant with byIdx in practice,
        // but also because the map holds 'int[]' which don't have proper equal.
        return name.equals(other.name)
               && keyspace.equals(other.keyspace)
               && typeName.equals(other.typeName)
               && Arrays.equals(byIdx, other.byIdx);
    }

    /**
     * Return the protocol version that has been used to deserialize this UDT, or that will be used to
     * serialize it. In most cases this should be the version currently in use by the cluster instance
     * that this UDT belongs to, as reported by {@code ProtocolOptions#getProtocolVersion()}.
     *
     * @return the protocol version that has been used to deserialize this UDT, or that will be used
     * to serialize it.
     */
    ProtocolVersion getProtocolVersion()
    {
        return protocolVersion;
    }

    CodecRegistry getCodecRegistry()
    {
        return codecRegistry;
    }

    @Override
    public String toString()
    {
        String str =
        Metadata.quoteIfNecessary(getKeyspace()) + '.' + Metadata.quoteIfNecessary(getTypeName());
        return isFrozen() ? "frozen<" + str + '>' : str;
    }

    @Override
    public String asFunctionParameterString()
    {
        return Metadata.quoteIfNecessary(getTypeName());
    }

    /**
     * A UDT field.
     */
    public static class Field
    {
        private final String name;
        private final DataType type;

        Field(String name, DataType type)
        {
            this.name = name;
            this.type = type;
        }

        /**
         * Returns the name of the field.
         *
         * @return the name of the field.
         */
        public String getName()
        {
            return name;
        }

        /**
         * Returns the type of the field.
         *
         * @return the type of the field.
         */
        public DataType getType()
        {
            return type;
        }

        @Override
        public final int hashCode()
        {
            return Arrays.hashCode(new Object[]{ name, type });
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof Field)) return false;

            Field other = (Field) o;
            return name.equals(other.name) && type.equals(other.type);
        }

        @Override
        public String toString()
        {
            return Metadata.quoteIfNecessary(name) + ' ' + type;
        }
    }
}
