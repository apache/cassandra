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
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ToJsonFct extends NativeScalarFunction
{
    public static final FunctionName NAME = FunctionName.nativeFunction("tojson");

    private static final Map<AbstractType<?>, ToJsonFct> instances = new ConcurrentHashMap<>();

    public static ToJsonFct getInstance(List<AbstractType<?>> argTypes) throws InvalidRequestException
    {
        if (argTypes.size() != 1)
            throw new InvalidRequestException(String.format("toJson() only accepts one argument (got %d)", argTypes.size()));

        AbstractType<?> fromType = argTypes.get(0);
        ToJsonFct func = instances.get(fromType);
        if (func == null)
        {
            func = new ToJsonFct(fromType);
            instances.put(fromType, func);
        }
        return func;
    }

    private ToJsonFct(AbstractType<?> argType)
    {
        super("tojson", UTF8Type.instance, argType);
    }

    public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
    {
        assert parameters.size() == 1 : "Expected 1 argument for toJson(), but got " + parameters.size();
        ByteBuffer parameter = parameters.get(0);
        if (parameter == null)
            return ByteBufferUtil.bytes("null");

        return ByteBufferUtil.bytes(argTypes.get(0).toJSONString(parameter, protocolVersion));
    }
}
