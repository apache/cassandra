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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMultimap;

import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;

import static com.google.common.collect.Iterables.filter;

/**
 * An immutable container for a keyspace's UDAs and UDFs (and, in case of {@link org.apache.cassandra.db.SystemKeyspace},
 * native functions and aggregates).
 */
public final class Functions implements Iterable<Function>
{
    private final ImmutableMultimap<FunctionName, Function> functions;

    private Functions(Builder builder)
    {
        functions = builder.functions.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Functions none()
    {
        return builder().build();
    }

    public static Functions of(Function... funs)
    {
        return builder().add(funs).build();
    }

    public Iterator<Function> iterator()
    {
        return functions.values().iterator();
    }

    public Stream<Function> stream()
    {
        return functions.values().stream();
    }

    /**
     * @return a stream of keyspace's UDFs
     */
    public Stream<UDFunction> udfs()
    {
        return stream().filter(f -> f instanceof UDFunction).map(f -> (UDFunction) f);
    }

    /**
     * @return a stream of keyspace's UDAs
     */
    public Stream<UDAggregate> udas()
    {
        return stream().filter(f -> f instanceof UDAggregate).map(f -> (UDAggregate) f);
    }

    /**
     * @return a collection of aggregates that use the provided function as either a state or a final function
     * @param function the referree function
     */
    public Collection<UDAggregate> aggregatesUsingFunction(Function function)
    {
        return udas().filter(uda -> uda.hasReferenceTo(function)).collect(Collectors.toList());
    }

    /**
     * Get all function overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the function name is not found; a non-empty collection of {@link Function} otherwise
     */
    public Collection<Function> get(FunctionName name)
    {
        return functions.get(name);
    }

    /**
     * Find the function with the specified name
     *
     * @param name fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the function name is not found; a non-empty optional of {@link Function} otherwise
     */
    public Optional<Function> find(FunctionName name, List<AbstractType<?>> argTypes)
    {
        return get(name).stream()
                        .filter(fun -> typesMatch(fun.argTypes(), argTypes))
                        .findAny();
    }

    /*
     * We need to compare the CQL3 representation of the type because comparing
     * the AbstractType will fail for example if a UDT has been changed.
     * Reason is that UserType.equals() takes the field names and types into account.
     * Example CQL sequence that would fail when comparing AbstractType:
     *    CREATE TYPE foo ...
     *    CREATE FUNCTION bar ( par foo ) RETURNS foo ...
     *    ALTER TYPE foo ADD ...
     * or
     *    ALTER TYPE foo ALTER ...
     * or
     *    ALTER TYPE foo RENAME ...
     */
    public static boolean typesMatch(AbstractType<?> t1, AbstractType<?> t2)
    {
        return t1.freeze().asCQL3Type().toString().equals(t2.freeze().asCQL3Type().toString());
    }

    public static boolean typesMatch(List<AbstractType<?>> t1, List<AbstractType<?>> t2)
    {
        if (t1.size() != t2.size())
            return false;

        for (int i = 0; i < t1.size(); i++)
            if (!typesMatch(t1.get(i), t2.get(i)))
                return false;

        return true;
    }

    public static int typeHashCode(AbstractType<?> t)
    {
        return t.asCQL3Type().toString().hashCode();
    }

    public static int typeHashCode(List<AbstractType<?>> types)
    {
        int h = 0;
        for (AbstractType<?> type : types)
            h = h * 31 + typeHashCode(type);
        return h;
    }

    /**
     * Create a Functions instance with the provided function added
     */
    public Functions with(Function fun)
    {
        if (find(fun.name(), fun.argTypes()).isPresent())
            throw new IllegalStateException(String.format("Function %s already exists", fun.name()));

        return builder().add(this).add(fun).build();
    }

    /**
     * Creates a Functions instance with the function with the provided name and argument types removed
     */
    public Functions without(FunctionName name, List<AbstractType<?>> argTypes)
    {
        Function fun =
            find(name, argTypes).orElseThrow(() -> new IllegalStateException(String.format("Function %s doesn't exists", name)));

        return builder().add(filter(this, f -> f != fun)).build();
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Functions && functions.equals(((Functions) o).functions));
    }

    @Override
    public int hashCode()
    {
        return functions.hashCode();
    }

    @Override
    public String toString()
    {
        return functions.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMultimap.Builder<FunctionName, Function> functions = new ImmutableMultimap.Builder<>();

        private Builder()
        {
            // we need deterministic iteration order; otherwise Functions.equals() breaks down
            functions.orderValuesBy((f1, f2) -> Integer.compare(f1.hashCode(), f2.hashCode()));
        }

        public Functions build()
        {
            return new Functions(this);
        }

        public Builder add(Function fun)
        {
            functions.put(fun.name(), fun);
            return this;
        }

        public Builder add(Function... funs)
        {
            for (Function fun : funs)
                add(fun);
            return this;
        }

        public  Builder add(Iterable<? extends Function> funs)
        {
            funs.forEach(this::add);
            return this;
        }
    }
}
