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
 * An immutable container for a keyspace's UDAs and UDFs.
 */
public final class UserFunctions implements Iterable<UserFunction>
{
    public enum Filter implements Predicate<UserFunction>
    {
        ALL, UDF, UDA;

        public boolean test(UserFunction function)
        {
            switch (this)
            {
                case UDF: return function instanceof UDFunction;
                case UDA: return function instanceof UDAggregate;
                default:  return true;
            }
        }
    }

    private final ImmutableMultimap<FunctionName, UserFunction> functions;

    private UserFunctions(Builder builder)
    {
        functions = builder.functions.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static UserFunctions none()
    {
        return builder().build();
    }

    public Iterator<UserFunction> iterator()
    {
        return functions.values().iterator();
    }

    public Stream<UserFunction> stream()
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

    public Iterable<UserFunction> referencingUserType(ByteBuffer name)
    {
        return Iterables.filter(this, f -> f.referencesUserType(name));
    }

    public UserFunctions withUpdatedUserType(UserType udt)
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
    public Collection<UserFunction> get(FunctionName name)
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

    public Optional<UserFunction> find(FunctionName name, List<AbstractType<?>> argTypes)
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
    public Optional<UserFunction> find(FunctionName name, List<AbstractType<?>> argTypes, Filter filter)
    {
        return get(name).stream()
                        .filter(filter.and(fun -> fun.typesMatch(argTypes)))
                        .findAny();
    }

    public boolean isEmpty()
    {
        return functions.isEmpty();
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

    public UserFunctions filter(Predicate<UserFunction> predicate)
    {
        Builder builder = builder();
        stream().filter(predicate).forEach(builder::add);
        return builder.build();
    }

    /**
     * Create a Functions instance with the provided function added
     */
    public UserFunctions with(UserFunction fun)
    {
        if (find(fun.name(), fun.argTypes()).isPresent())
            throw new IllegalStateException(String.format("Function %s already exists", fun.name()));

        return builder().add(this).add(fun).build();
    }

    /**
     * Creates a Functions instance with the function with the provided name and argument types removed
     */
    public UserFunctions without(FunctionName name, List<AbstractType<?>> argTypes)
    {
        Function fun =
            find(name, argTypes).orElseThrow(() -> new IllegalStateException(String.format("Function %s doesn't exists", name)));

        return without(fun);
    }

    public UserFunctions without(Function function)
    {
        return builder().add(Iterables.filter(this, f -> f != function)).build();
    }

    public UserFunctions withAddedOrUpdated(UserFunction function)
    {
        return builder().add(Iterables.filter(this, f -> !(f.name().equals(function.name()) && f.typesMatch(function.argTypes()))))
                        .add(function)
                        .build();
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof UserFunctions && functions.equals(((UserFunctions) o).functions));
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
        final ImmutableMultimap.Builder<FunctionName, UserFunction> functions = new ImmutableMultimap.Builder<>();

        private Builder()
        {
            // we need deterministic iteration order; otherwise Functions.equals() breaks down
            functions.orderValuesBy(Comparator.comparingInt(Object::hashCode));
        }

        public UserFunctions build()
        {
            return new UserFunctions(this);
        }

        public Builder add(UserFunction fun)
        {
            functions.put(fun.name(), fun);
            return this;
        }

        public Builder add(UserFunction... funs)
        {
            for (UserFunction fun : funs)
                add(fun);
            return this;
        }

        public Builder add(Iterable<? extends UserFunction> funs)
        {
            funs.forEach(this::add);
            return this;
        }
    }

    @SuppressWarnings("unchecked")
    static FunctionsDiff<UDFunction> udfsDiff(UserFunctions before, UserFunctions after)
    {
        return (FunctionsDiff<UDFunction>) FunctionsDiff.diff(before, after, Filter.UDF);
    }

    @SuppressWarnings("unchecked")
    static FunctionsDiff<UDAggregate> udasDiff(UserFunctions before, UserFunctions after)
    {
        return (FunctionsDiff<UDAggregate>) FunctionsDiff.diff(before, after, Filter.UDA);
    }

    public static final class FunctionsDiff<T extends Function> extends Diff<UserFunctions, T>
    {
        static final FunctionsDiff NONE = new FunctionsDiff<>(UserFunctions.none(), UserFunctions.none(), ImmutableList.of());

        private FunctionsDiff(UserFunctions created, UserFunctions dropped, ImmutableCollection<Altered<T>> altered)
        {
            super(created, dropped, altered);
        }

        private static FunctionsDiff diff(UserFunctions before, UserFunctions after, Filter filter)
        {
            if (before == after)
                return NONE;

            UserFunctions created = after.filter(filter.and(k -> !before.find(k.name(), k.argTypes(), filter).isPresent()));
            UserFunctions dropped = before.filter(filter.and(k -> !after.find(k.name(), k.argTypes(), filter).isPresent()));

            ImmutableList.Builder<Altered<UserFunction>> altered = ImmutableList.builder();
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
