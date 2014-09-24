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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * User-defined function using a method in a class loaded on the classpath by
 * reflection.
 *
 * This is used when the LANGUAGE of the UDF definition is "class".
 */
final class ReflectionBasedUDF extends UDFunction
{
    private final MethodHandle method;

    ReflectionBasedUDF(FunctionName name,
                       List<ColumnIdentifier> argNames,
                       List<AbstractType<?>> argTypes,
                       AbstractType<?> returnType,
                       String language,
                       String body,
                       boolean deterministic)
    throws InvalidRequestException
    {
        super(name, argNames, argTypes, returnType, language, body, deterministic);
        assert language.equals("class");
        this.method = resolveMethod();
    }

    private MethodHandle resolveMethod() throws InvalidRequestException
    {
        Class<?> jReturnType = javaType(returnType);
        Class<?>[] paramTypes = javaParamTypes(argTypes);

        String className;
        String methodName;
        int i = body.indexOf('#');
        if (i != -1)
        {
            methodName = body.substring(i + 1);
            className = body.substring(0, i);
        }
        else
        {
            methodName = name.name;
            className = body;
        }
        try
        {
            Class<?> cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader());

            MethodHandles.Lookup handles = MethodHandles.lookup();
            MethodType methodType = MethodType.methodType(jReturnType, paramTypes);
            MethodHandle handle = handles.findStatic(cls, methodName, methodType);

            return handle;
        }
        catch (ClassNotFoundException e)
        {
            throw new InvalidRequestException("Class " + className + " does not exist");
        }
        catch (NoSuchMethodException e)
        {
            throw new InvalidRequestException("Method 'public static " + jReturnType.getName() + ' ' +
                                              className + '.' + methodName + '(' + Arrays.toString(paramTypes) +
                                              ")' does not exist - check for static, argument types and return type");
        }
        catch (IllegalAccessException e)
        {
            throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") is not accessible");
        }
    }

    public ByteBuffer execute(List<ByteBuffer> parameters) throws InvalidRequestException
    {
        Object[] parms = new Object[argTypes.size()];
        for (int i = 0; i < parms.length; i++)
        {
            ByteBuffer bb = parameters.get(i);
            if (bb != null)
                parms[i] = argTypes.get(i).compose(bb);
        }

        Object result;
        try
        {
            result = method.invokeWithArguments(parms);
            @SuppressWarnings("unchecked") ByteBuffer r = result != null ? ((AbstractType) returnType).decompose(result) : null;
            return r;
        }
        catch (VirtualMachineError e)
        {
            // handle OutOfMemoryError and other fatals not here!
            throw e;
        }
        catch (Throwable e)
        {
            logger.error("Invocation of function '{}' failed", this, e);
            throw new InvalidRequestException("Invocation of function '" + this + "' failed: " + e);
        }
    }
}
