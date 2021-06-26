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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.*;

import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;

import static java.util.stream.Collectors.toList;

import static com.google.common.collect.Iterables.any;

/**
 * An immutable container for a keyspace's UDAs and UDFs (and, in case of {@link org.apache.cassandra.db.SystemKeyspace},
 * native functions and aggregates).
 */
public final class Functions implements Iterable<Function>
{
    public enum Filter implements Predicate<Function>
    {
        ALL, UDF, UDA;

        public boolean test(Function function)
        {
            switch (this)
            {
                case UDF: return function instanceof UDFunction;
                case UDA: return function instanceof UDAggregate;
                default:  return true;
            }
        }
    }

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

    public int size()
    {
        return functions.size();
    }

    /**
     * @return a stream of keyspace's UDFs
     */
    public Stream<UDFunction> udfs()
    {
        return stream().filter(Filter.UDF).map(f -> (UDFunction) f);
    }

    /**
     * @return a stream of keyspace's UDAs
     */
    public Stream<UDAggregate> udas()
    {
        return stream().filter(Filter.UDA).map(f -> (UDAggregate) f);
    }

    public Iterable<Function> referencingUserType(ByteBuffer name)
    {
        return Iterables.filter(this, f -> f.referencesUserType(name));
    }

    public Functions withUpdatedUserType(UserType udt)
    {
        if (!any(this, f -> f.referencesUserType(udt.name)))
            return this;

        Collection<UDFunction>  udfs = udfs().map(f -> f.withUpdatedUserType(udt)).collect(toList());
        Collection<UDAggregate> udas = udas().map(f -> f.withUpdatedUserType(udfs, udt)).collect(toList());

        return builder().add(udfs).add(udas).build();
    }

    /**
     * @return a stream of aggregates that use the provided function as either a state or a final function
     * @param function the referree function
     */
    public Stream<UDAggregate> aggregatesUsingFunction(Function function)
    {
        return udas().filter(uda -> uda.hasReferenceTo(function));
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
     * Get all UDFs overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the function name is not found; a non-empty collection of {@link UDFunction} otherwise
     */
    public Collection<UDFunction> getUdfs(FunctionName name)
    {
        return functions.get(name)
                        .stream()
                        .filter(Filter.UDF)
                        .map(f -> (UDFunction) f)
                        .collect(Collectors.toList());
    }

    /**
     * Get all UDAs overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the function name is not found; a non-empty collection of {@link UDAggregate} otherwise
     */
    public Collection<UDAggregate> getUdas(FunctionName name)
    {
        return functions.get(name)
                        .stream()
                        .filter(Filter.UDA)
                        .map(f -> (UDAggregate) f)
                        .collect(Collectors.toList());
    }

    public Optional<Function> find(FunctionName name, List<AbstractType<?>> argTypes)
    {
        return find(name, argTypes, Filter.ALL);
    }

    /**
     * Find the function with the specified name
     *
     * @param name fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the function name is not found; a non-empty optional of {@link Function} otherwise
     */
    public Optional<Function> find(FunctionName name, List<AbstractType<?>> argTypes, Filter filter)
    {
        return get(name).stream()
                        .filter(filter.and(fun -> typesMatch(fun.argTypes(), argTypes)))
                        .findAny();
    }

    public boolean isEmpty()
    {
        return functions.isEmpty();
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
    private static boolean typesMatch(AbstractType<?> t1, AbstractType<?> t2)
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

    public Functions filter(Predicate<Function> predicate)
    {
        Builder builder = builder();
        stream().filter(predicate).forEach(builder::add);
        return builder.build();
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

        return without(fun);
    }

    public Functions without(Function function)
    {
        return builder().add(Iterables.filter(this, f -> f != function)).build();
    }

    public Functions withAddedOrUpdated(Function function)
    {
        return builder().add(Iterables.filter(this, f -> !(f.name().equals(function.name()) && Functions.typesMatch(f.argTypes(), function.argTypes()))))
                        .add(function)
                        .build();
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
            functions.orderValuesBy(Comparator.comparingInt(Object::hashCode));
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

        public Builder add(Iterable<? extends Function> funs)
        {
            funs.forEach(this::add);
            return this;
        }
    }

    @SuppressWarnings("unchecked")
    static FunctionsDiff<UDFunction> udfsDiff(Functions before, Functions after)
    {
        return (FunctionsDiff<UDFunction>) FunctionsDiff.diff(before, after, Filter.UDF);
    }

    @SuppressWarnings("unchecked")
    static FunctionsDiff<UDAggregate> udasDiff(Functions before, Functions after)
    {
        return (FunctionsDiff<UDAggregate>) FunctionsDiff.diff(before, after, Filter.UDA);
    }

    public static final class FunctionsDiff<T extends Function> extends Diff<Functions, T>
    {
        static final FunctionsDiff NONE = new FunctionsDiff<>(Functions.none(), Functions.none(), ImmutableList.of());

        private FunctionsDiff(Functions created, Functions dropped, ImmutableCollection<Altered<T>> altered)
        {
            super(created, dropped, altered);
        }

        private static FunctionsDiff diff(Functions before, Functions after, Filter filter)
        {
            if (before == after)
                return NONE;

            Functions created = after.filter(filter.and(k -> !before.find(k.name(), k.argTypes(), filter).isPresent()));
            Functions dropped = before.filter(filter.and(k -> !after.find(k.name(), k.argTypes(), filter).isPresent()));

            ImmutableList.Builder<Altered<Function>> altered = ImmutableList.builder();
            before.stream().filter(filter).forEach(functionBefore ->
            {
                after.find(functionBefore.name(), functionBefore.argTypes(), filter).ifPresent(functionAfter ->
                {
                    functionBefore.compare(functionAfter).ifPresent(kind -> altered.add(new Altered<>(functionBefore, functionAfter, kind)));
                });
            });

            return new FunctionsDiff<>(created, dropped, altered.build());
        }
    }
}
