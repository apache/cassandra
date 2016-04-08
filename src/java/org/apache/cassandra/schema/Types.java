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
package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.*;

import javax.annotation.Nullable;

import com.google.common.collect.*;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
import static com.google.common.collect.Iterables.filter;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * An immutable container for a keyspace's UDTs.
 */
public final class Types implements Iterable<UserType>
{
    private static final Types NONE = new Types(ImmutableMap.of());

    private final Map<ByteBuffer, UserType> types;

    private Types(Builder builder)
    {
        types = builder.types.build();
    }

    /*
     * For use in RawBuilder::build only.
     */
    private Types(Map<ByteBuffer, UserType> types)
    {
        this.types = types;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static RawBuilder rawBuilder(String keyspace)
    {
        return new RawBuilder(keyspace);
    }

    public static Types none()
    {
        return NONE;
    }

    public static Types of(UserType... types)
    {
        return builder().add(types).build();
    }

    public Iterator<UserType> iterator()
    {
        return types.values().iterator();
    }

    /**
     * Get the type with the specified name
     *
     * @param name a non-qualified type name
     * @return an empty {@link Optional} if the type name is not found; a non-empty optional of {@link UserType} otherwise
     */
    public Optional<UserType> get(ByteBuffer name)
    {
        return Optional.ofNullable(types.get(name));
    }

    /**
     * Get the type with the specified name
     *
     * @param name a non-qualified type name
     * @return null if the type name is not found; the found {@link UserType} otherwise
     */
    @Nullable
    public UserType getNullable(ByteBuffer name)
    {
        return types.get(name);
    }

    /**
     * Create a Types instance with the provided type added
     */
    public Types with(UserType type)
    {
        if (get(type.name).isPresent())
            throw new IllegalStateException(format("Type %s already exists", type.name));

        return builder().add(this).add(type).build();
    }

    /**
     * Creates a Types instance with the type with the provided name removed
     */
    public Types without(ByteBuffer name)
    {
        UserType type =
            get(name).orElseThrow(() -> new IllegalStateException(format("Type %s doesn't exists", name)));

        return builder().add(filter(this, t -> t != type)).build();
    }

    MapDifference<ByteBuffer, UserType> diff(Types other)
    {
        return Maps.difference(types, other.types);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof Types))
            return false;

        Types other = (Types) o;

        if (types.size() != other.types.size())
            return false;

        Iterator<Map.Entry<ByteBuffer, UserType>> thisIter = this.types.entrySet().iterator();
        Iterator<Map.Entry<ByteBuffer, UserType>> otherIter = other.types.entrySet().iterator();
        while (thisIter.hasNext())
        {
            Map.Entry<ByteBuffer, UserType> thisNext = thisIter.next();
            Map.Entry<ByteBuffer, UserType> otherNext = otherIter.next();
            if (!thisNext.getKey().equals(otherNext.getKey()))
                return false;

            if (!thisNext.getValue().equals(otherNext.getValue(), true))  // ignore freezing
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        return types.hashCode();
    }

    @Override
    public String toString()
    {
        return types.values().toString();
    }

    public static final class Builder
    {
        final ImmutableSortedMap.Builder<ByteBuffer, UserType> types = ImmutableSortedMap.naturalOrder();

        private Builder()
        {
        }

        public Types build()
        {
            return new Types(this);
        }

        public Builder add(UserType type)
        {
            assert type.isMultiCell();
            types.put(type.name, type);
            return this;
        }

        public Builder add(UserType... types)
        {
            for (UserType type : types)
                add(type);
            return this;
        }

        public Builder add(Iterable<UserType> types)
        {
            types.forEach(this::add);
            return this;
        }
    }

    public static final class RawBuilder
    {
        final String keyspace;
        final List<RawUDT> definitions;

        private RawBuilder(String keyspace)
        {
            this.keyspace = keyspace;
            this.definitions = new ArrayList<>();
        }

        /**
         * Build a Types instance from Raw definitions.
         *
         * Constructs a DAG of graph dependencies and resolves them 1 by 1 in topological order.
         */
        public Types build()
        {
            if (definitions.isEmpty())
                return Types.none();

            /*
             * build a DAG of UDT dependencies
             */
            Map<RawUDT, Integer> vertices = new HashMap<>(); // map values are numbers of referenced types
            for (RawUDT udt : definitions)
                vertices.put(udt, 0);

            Multimap<RawUDT, RawUDT> adjacencyList = HashMultimap.create();
            for (RawUDT udt1 : definitions)
                for (RawUDT udt2 : definitions)
                    if (udt1 != udt2 && udt1.referencesUserType(udt2))
                        adjacencyList.put(udt2, udt1);

            /*
             * resolve dependencies in topological order, using Kahn's algorithm
             */
            adjacencyList.values().forEach(vertex -> vertices.put(vertex, vertices.get(vertex) + 1));

            Queue<RawUDT> resolvableTypes = new LinkedList<>(); // UDTs with 0 dependencies
            for (Map.Entry<RawUDT, Integer> entry : vertices.entrySet())
                if (entry.getValue() == 0)
                    resolvableTypes.add(entry.getKey());

            Types types = new Types(new HashMap<>());
            while (!resolvableTypes.isEmpty())
            {
                RawUDT vertex = resolvableTypes.remove();

                for (RawUDT dependentType : adjacencyList.get(vertex))
                    if (vertices.replace(dependentType, vertices.get(dependentType) - 1) == 1)
                        resolvableTypes.add(dependentType);

                UserType udt = vertex.prepare(keyspace, types);
                types.types.put(udt.name, udt);
            }

            if (types.types.size() != definitions.size())
                throw new ConfigurationException(format("Cannot resolve UDTs for keyspace %s: some types are missing", keyspace));

            /*
             * return an immutable copy
             */
            return Types.builder().add(types).build();
        }

        public void add(String name, List<String> fieldNames, List<String> fieldTypes)
        {
            List<CQL3Type.Raw> rawFieldTypes =
                fieldTypes.stream()
                          .map(CQLTypeParser::parseRaw)
                          .collect(toList());

            definitions.add(new RawUDT(name, fieldNames, rawFieldTypes));
        }

        private static final class RawUDT
        {
            final String name;
            final List<String> fieldNames;
            final List<CQL3Type.Raw> fieldTypes;

            RawUDT(String name, List<String> fieldNames, List<CQL3Type.Raw> fieldTypes)
            {
                this.name = name;
                this.fieldNames = fieldNames;
                this.fieldTypes = fieldTypes;
            }

            boolean referencesUserType(RawUDT other)
            {
                return fieldTypes.stream().anyMatch(t -> t.referencesUserType(other.name));
            }

            UserType prepare(String keyspace, Types types)
            {
                List<ByteBuffer> preparedFieldNames =
                    fieldNames.stream()
                              .map(ByteBufferUtil::bytes)
                              .collect(toList());

                List<AbstractType<?>> preparedFieldTypes =
                    fieldTypes.stream()
                              .map(t -> t.prepareInternal(keyspace, types).getType())
                              .collect(toList());

                return new UserType(keyspace, bytes(name), preparedFieldNames, preparedFieldTypes, true);
            }

            @Override
            public int hashCode()
            {
                return name.hashCode();
            }

            @Override
            public boolean equals(Object other)
            {
                return name.equals(((RawUDT) other).name);
            }
        }
    }
}
