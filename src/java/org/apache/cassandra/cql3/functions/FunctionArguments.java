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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.FunctionContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Default {@link Arguments} implementation.
 */
public final class FunctionArguments implements Arguments
{
    public static final ArgumentDeserializer[] NO_DESERIALIZERS = new ArgumentDeserializer[0];

    /**
     * The deserializer used to deserialize the columns.
     */
    private final ArgumentDeserializer[] deserializers;

    /**
     * The protocol version.
     */
    private final ProtocolVersion version;

    private final FunctionContext context;

    /**
     * The deserialized arguments.
     */
    private final Object[] arguments;

    /**
     * Creates a new {@link FunctionArguments} for the specified types.
     *
     * @param version the protocol version
     * @param argTypes the argument types
     * @return a new {@link FunctionArguments} for the specified types.
     */
    public static Arguments newInstanceForUdf(ProtocolVersion version, FunctionContext context, List<UDFDataType> argTypes)
    {
        int size = argTypes.size();

        if (size == 0)
            return context.noArguments(version);

        ArgumentDeserializer[] deserializers = new ArgumentDeserializer[size];

        for (int i = 0; i < size; i++)
            deserializers[i] = argTypes.get(i).getArgumentDeserializer();

        return new FunctionArguments(version, context, deserializers);
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return version;
    }

    @Override
    public FunctionContext context()
    {
        return context;
    }

    /**
     * Creates a new {@link FunctionArguments} that does not deserialize the arguments.
     *
     * @param version the protocol version
     * @param numberOfArguments the number of argument
     * @return a new {@link FunctionArguments} for the specified types.
     */
    public static FunctionArguments newNoopInstance(ProtocolVersion version, FunctionContext context, int numberOfArguments)
    {
        ArgumentDeserializer[] deserializers = new ArgumentDeserializer[numberOfArguments];
        Arrays.fill(deserializers, ArgumentDeserializer.NOOP_DESERIALIZER);

        return new FunctionArguments(version, context, deserializers);
    }

    /**
     * Creates a new {@link FunctionArguments} for a native function.
     * <p>Native functions can use different {@link ArgumentDeserializer} to avoid instanciating primitive wrappers.</p>
     *
     * @param version the protocol version
     * @param argTypes the argument types
     * @return a new {@link FunctionArguments} for the specified types.
     */
    public static Arguments newInstanceForNativeFunction(ProtocolVersion version, FunctionContext context, List<AbstractType<?>> argTypes)
    {
        int size = argTypes.size();

        if (size == 0)
            return context.noArguments(version);

        ArgumentDeserializer[] deserializers = new ArgumentDeserializer[size];

        for (int i = 0; i < size; i++)
            deserializers[i] = argTypes.get(i).getArgumentDeserializer();

        return new FunctionArguments(version, context, deserializers);
    }

    public FunctionArguments(ProtocolVersion version, FunctionContext context, ArgumentDeserializer... deserializers)
    {
        this.version = version;
        this.context = context;
        this.deserializers = deserializers;
        this.arguments = new Object[deserializers.length];
    }

    public void set(int i, ByteBuffer buffer)
    {
        arguments[i] = deserializers[i].deserialize(version, buffer);
    }

    @Override
    public boolean containsNulls()
    {
        for (int i = 0; i < arguments.length; i++)
        {
            if (arguments[i] == null)
                return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int i)
    {
        return (T) arguments[i];
    }

    @Override
    public int size()
    {
        return arguments.length;
    }
}
