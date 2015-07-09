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
import java.util.Iterator;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.marshal.UserType;

import static com.google.common.collect.Iterables.filter;

/**
 * An immutable container for a keyspace's UDTs.
 */
public final class Types implements Iterable<UserType>
{
    private final ImmutableMap<ByteBuffer, UserType> types;

    private Types(Builder builder)
    {
        types = builder.types.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Types none()
    {
        return builder().build();
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
        final ImmutableMap.Builder<ByteBuffer, UserType> types = new ImmutableMap.Builder<>();

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
}
