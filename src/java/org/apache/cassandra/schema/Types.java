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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

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
            throw new IllegalStateException(String.format("Type %s already exists", type.name));

        return builder().add(this).add(type).build();
    }

    /**
     * Creates a Types instance with the type with the provided name removed
     */
    public Types without(ByteBuffer name)
    {
        UserType type =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Type %s doesn't exists", name)));

        return builder().add(filter(this, t -> t != type)).build();
    }

    MapDifference<ByteBuffer, UserType> diff(Types other)
    {
        return Maps.difference(types, other.types);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Types && types.equals(((Types) o).types));
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
        final ImmutableMap.Builder<ByteBuffer, UserType> types = ImmutableMap.builder();

        private Builder()
        {
        }

        public Types build()
        {
            return new Types(this);
        }

        public Builder add(UserType type)
        {
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
            DefaultDirectedGraph<RawUDT, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);

            definitions.forEach(graph::addVertex);

            for (RawUDT udt1: definitions)
                for (RawUDT udt2 : definitions)
                    if (udt1 != udt2 && udt1.referencesUserType(udt2.name))
                        graph.addEdge(udt2, udt1);

            /*
             * iterate in topological order,
             */
            Types types = new Types(new HashMap<>());

            TopologicalOrderIterator<RawUDT, DefaultEdge> iterator = new TopologicalOrderIterator<>(graph);
            while (iterator.hasNext())
            {
                UserType udt = iterator.next().prepare(keyspace, types); // will throw InvalidRequestException if meets an unknown type
                types.types.put(udt.name, udt);
            }

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

            boolean referencesUserType(String typeName)
            {
                return fieldTypes.stream().anyMatch(t -> t.referencesUserType(typeName));
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

                return new UserType(keyspace, bytes(name), preparedFieldNames, preparedFieldTypes);
            }
        }
    }
}
