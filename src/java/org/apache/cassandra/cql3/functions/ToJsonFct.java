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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class ToJsonFct extends NativeScalarFunction
{
    private static final Map<AbstractType<?>, ToJsonFct> instances = new ConcurrentHashMap<>();

    public static ToJsonFct getInstance(String name, List<AbstractType<?>> argTypes) throws InvalidRequestException
    {
        if (argTypes.size() != 1)
            throw new InvalidRequestException(format("%s() only accepts one argument (got %d)", name, argTypes.size()));

        AbstractType<?> fromType = argTypes.get(0);
        ToJsonFct func = instances.get(fromType);
        if (func == null)
        {
            func = new ToJsonFct(name, fromType);
            instances.put(fromType, func);
        }
        return func;
    }

    private ToJsonFct(String name, AbstractType<?> argType)
    {
        super(name, UTF8Type.instance, argType);
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return new FunctionArguments(version, (protocolVersion, buffer) -> {
            AbstractType<?> argType = argTypes.get(0);

            if (buffer == null || (!buffer.hasRemaining() && argType.isEmptyValueMeaningless()))
                return null;

            return argTypes.get(0).toJSONString(buffer, protocolVersion);
        });
    }

    @Override
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        if (arguments.containsNulls())
            return ByteBufferUtil.bytes("null");

        return ByteBufferUtil.bytes(arguments.<String>get(0));
    }

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new Factory("to_json"));
        functions.add(new Factory("tojson")); // deprecated pre-5.0 name
    }

    private static class Factory extends FunctionFactory
    {
        public Factory(String name)
        {
            super(name, FunctionParameter.anyType(false));
        }

        @Override
        protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
        {
            return ToJsonFct.getInstance(name.name, argTypes);
        }
    }
}
