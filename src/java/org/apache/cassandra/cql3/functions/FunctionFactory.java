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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;

/**
 * Class for dynamically building different overloads of a CQL {@link Function} according to specific function calls.
 * <p>
 * For example, the factory for the {@code max} function will return a {@code (text) -> text} function if it's called
 * with an {@code text} argument, like in {@code max('abc')}. It however will return a {@code (list<int>) -> list<int>}
 * function if it's called with {@code max([1,2,3])}, etc.
 * <p>
 * This is meant to be used to create functions that require too many overloads to have them pre-created in memory. Note
 * that in the case of functions accepting collections, tuples or UDTs the number of overloads is potentially infinite.
 */
public abstract class FunctionFactory
{
    /** The name of the built functions. */
    protected final FunctionName name;

    /** The accepted parameters. */
    protected final List<FunctionParameter> parameters;

    /**
     * @param name the name of the built functions
     * @param parameters the accepted parameters
     */
    public FunctionFactory(String name, FunctionParameter... parameters)
    {
        this.name = FunctionName.nativeFunction(name);
        this.parameters = Arrays.asList(parameters);
    }

    public FunctionName name()
    {
        return name;
    }

    /**
     * Returns a function with a signature compatible with the specified function call.
     *
     * @param args the arguments in the function call for which the function is going to be built
     * @param receiverType the expected return type of the function call for which the function is going to be built
     * @param receiverKs the name of the recevier keyspace
     * @param receiverCf the name of the recevier table
     * @return a function with a signature compatible with the specified function call
     */
    public NativeFunction getOrCreateFunction(List<? extends AssignmentTestable> args,
                                              AbstractType<?> receiverType,
                                              String receiverKs,
                                              String receiverCf)
    {
        // validate the number of arguments
        if (args.size() != parameters.size())
            throw new InvalidRequestException("Invalid number of arguments for function " + this);

        // try to infer the types of the arguments
        List<AbstractType<?>> types = new ArrayList<>(args.size());
        for (int i = 0; i < args.size(); i++)
        {
            AssignmentTestable arg = args.get(i);
            FunctionParameter parameter = parameters.get(i);
            AbstractType<?> type = parameter.inferType(SchemaConstants.SYSTEM_KEYSPACE_NAME, arg, receiverType);
            if (type == null)
                throw new InvalidRequestException(String.format("Cannot infer type of argument %s in call to " +
                                                                "function %s: use type casts to disambiguate",
                                                                arg, this));
            parameter.validateType(name, arg, type);
            type = type.udfType();
            types.add(type);
        }

        return doGetOrCreateFunction(types, receiverType);
    }

    /**
     * Returns a function compatible with the specified signature.
     *
     * @param argTypes the types of the function arguments
     * @param receiverType the expected return type of the function
     * @return a function compatible with the specified signature
     */
    protected abstract NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType);

    @Override
    public String toString()
    {
        return String.format("%s(%s)", name, parameters.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }
}
