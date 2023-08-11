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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cql3.CQL3Type;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.JsonUtils;

import static java.lang.String.format;

public class FromJsonFct extends NativeScalarFunction
{
    private static final Map<AbstractType<?>, FromJsonFct> instances = new ConcurrentHashMap<>();

    public static FromJsonFct getInstance(FunctionName name, AbstractType<?> returnType)
    {
        FromJsonFct func = instances.get(returnType);
        if (func == null)
        {
            func = new FromJsonFct(name, returnType);
            instances.put(returnType, func);
        }
        return func;
    }

    private FromJsonFct(FunctionName name, AbstractType<?> returnType)
    {
        super(name.name, returnType, UTF8Type.instance);
    }

    @Override
    public ByteBuffer execute(Arguments arguments)
    {
        assert arguments.size() == 1 : format("Unexpectedly got %d arguments for %s()", arguments.size(), name.name);

        if (arguments.containsNulls())
            return null;

        String jsonArg = arguments.get(0);
        try
        {
            Object object = JsonUtils.JSON_OBJECT_MAPPER.readValue(jsonArg, Object.class);
            if (object == null)
                return null;

            return returnType.fromJSONObject(object)
                             .bindAndGet(QueryOptions.forProtocolVersion(arguments.getProtocolVersion()));
        }
        catch (IOException exc)
        {
            throw FunctionExecutionException.create(name(), 
                                                    Collections.singletonList("text"),
                                                    format("Could not decode JSON string '%s': %s", jsonArg, exc));
        }
        catch (MarshalException exc)
        {
            throw FunctionExecutionException.create(this, exc);
        }
    }

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new Factory("from_json"));
        functions.add(new Factory("fromjson"));
    }
    
    private static class Factory extends FunctionFactory
    {
        private Factory(String name)
        {
            super(name, FunctionParameter.fixed(CQL3Type.Native.TEXT));
        }

        @Override
        protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
        {
            if (receiverType == null)
                throw new InvalidRequestException(format("%s() cannot be used in the selection clause of a SELECT statement", name.name));

            return FromJsonFct.getInstance(name, receiverType);
        }
    }
}
