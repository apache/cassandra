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

package org.apache.cassandra.cql3.functions;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.cql3.functions.masking.MaskingFcts;

/**
 * A container of native functions. It stores both pre-built function overloads ({@link NativeFunction}) and
 * dynamic generators of functions ({@link FunctionFactory}).
 */
public class NativeFunctions
{
    public static NativeFunctions instance = new NativeFunctions()
    {
        {
            TokenFct.addFunctionsTo(this);
            CastFcts.addFunctionsTo(this);
            UuidFcts.addFunctionsTo(this);
            TimeFcts.addFunctionsTo(this);
            ToJsonFct.addFunctionsTo(this);
            FromJsonFct.addFunctionsTo(this);
            OperationFcts.addFunctionsTo(this);
            AggregateFcts.addFunctionsTo(this);
            CollectionFcts.addFunctionsTo(this);
            BytesConversionFcts.addFunctionsTo(this);
            MathFcts.addFunctionsTo(this);
            MaskingFcts.addFunctionsTo(this);
            VectorFcts.addFunctionsTo(this);
        }
    };

    /** Pre-built function overloads. */
    private final Multimap<FunctionName, NativeFunction> functions = HashMultimap.create();

    /** Dynamic function factories. */
    private final Multimap<FunctionName, FunctionFactory> factories = HashMultimap.create();

    public void add(NativeFunction function)
    {
        functions.put(function.name(), function);
        NativeFunction legacyFunction = function.withLegacyName();
        if (legacyFunction != null)
            functions.put(legacyFunction.name(), legacyFunction);
    }

    public void addAll(NativeFunction... functions)
    {
        for (NativeFunction function : functions)
            add(function);
    }

    public void add(FunctionFactory factory)
    {
        factories.put(factory.name(), factory);
    }

    /**
     * Returns all the registered pre-built functions overloads with the specified name.
     *
     * @param name a function name
     * @return the pre-built functions with the specified name
     */
    public Collection<NativeFunction> getFunctions(FunctionName name)
    {
        return functions.get(name);
    }

    /**
     * @return all the registered pre-built functions.
     */
    public Collection<NativeFunction> getFunctions()
    {
        return functions.values();
    }

    /**
     * Returns all the registered functions factories with the specified name.
     *
     * @param name a function name
     * @return the function factories with the specified name
     */
    public Collection<FunctionFactory> getFactories(FunctionName name)
    {
        return factories.get(name);
    }

    /**
     * @return all the registered functions factories.
     */
    public Collection<FunctionFactory> getFactories()
    {
        return factories.values();
    }

    /**
     * Returns whether there is a function factory with the specified name.
     *
     * @param name a function name
     * @return {@code true} if there is a factory with the specified name, {@code false} otherwise
     */
    public boolean hasFactory(FunctionName name)
    {
        return factories.containsKey(name);
    }
}
