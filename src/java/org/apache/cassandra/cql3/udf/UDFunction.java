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

package org.apache.cassandra.cql3.udf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.UFMetaData;
import org.apache.cassandra.cql3.AssignementTestable;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * UDFunction contains the <i>invokable</i> instance of a user defined function.
 * Currently (as of CASSANDRA-7395) only {@code public static} methods in a {@link public} class
 * can be invoked.
 * CASSANDRA-7562 will introduce Java source code UDFs and CASSANDRA-7526 will introduce JSR-223 scripting languages.
 * Invocations of UDFs are routed via this class.
 */
public class UDFunction
{
    private static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    public final UFMetaData meta;

    public final Method method;

    UDFunction(UFMetaData meta) throws InvalidRequestException
    {
        this.meta = meta;

        Method m;
        switch (meta.language)
        {
            case "class":
                m = resolveClassMethod();
                break;
            default:
                throw new InvalidRequestException("Invalid UDF language " + meta.language + " for '" + meta.qualifiedName + '\'');
        }
        this.method = m;
    }

    private Method resolveClassMethod() throws InvalidRequestException
    {
        Class<?> jReturnType = meta.cqlReturnType.getType().getSerializer().getType();
        Class<?> paramTypes[] = new Class[meta.cqlArgumentTypes.size()];
        for (int i = 0; i < paramTypes.length; i++)
            paramTypes[i] = meta.cqlArgumentTypes.get(i).getType().getSerializer().getType();

        String className;
        String methodName;
        int i = meta.body.indexOf('#');
        if (i != -1)
        {
            methodName = meta.body.substring(i + 1);
            className = meta.body.substring(0, i);
        }
        else
        {
            methodName = meta.functionName;
            className = meta.body;
        }
        try
        {
            Class<?> cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader());

            Method method = cls.getMethod(methodName, paramTypes);

            if (!Modifier.isStatic(method.getModifiers()))
                throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") is not static");

            if (!jReturnType.isAssignableFrom(method.getReturnType()))
            {
                throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") " +
                                                  "has incompatible return type " + method.getReturnType() + " (not assignable to " + jReturnType + ')');
            }

            return method;
        }
        catch (ClassNotFoundException e)
        {
            throw new InvalidRequestException("Class " + className + " does not exist");
        }
        catch (NoSuchMethodException e)
        {
            throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") does not exist");
        }
    }

    public Function create(List<? extends AssignementTestable> providedArgs)
    {
        final int argCount = providedArgs.size();
        final List<AbstractType<?>> argsType = new ArrayList<>(argCount);
        final AbstractType<?> returnType = meta.cqlReturnType.getType();
        for (int i = 0; i < argCount; i++)
        {
            AbstractType<?> argType = meta.cqlArgumentTypes.get(i).getType();
            argsType.add(argType);
        }

        return new Function()
        {
            public String name()
            {
                return meta.qualifiedName;
            }

            public List<AbstractType<?>> argsType()
            {
                return argsType;
            }

            public AbstractType<?> returnType()
            {
                return returnType;
            }

            public ByteBuffer execute(List<ByteBuffer> parameters) throws InvalidRequestException
            {
                Object[] parms = new Object[argCount];
                for (int i = 0; i < parms.length; i++)
                {
                    ByteBuffer bb = parameters.get(i);
                    if (bb != null)
                    {
                        AbstractType<?> argType = argsType.get(i);
                        parms[i] = argType.compose(bb);
                    }
                }

                Object result;
                try
                {
                    result = method.invoke(null, parms);
                    @SuppressWarnings("unchecked") ByteBuffer r = result != null ? ((AbstractType) returnType).decompose(result) : null;
                    return r;
                }
                catch (InvocationTargetException e)
                {
                    Throwable c = e.getCause();
                    logger.error("Invocation of UDF {} failed", meta.qualifiedName, c);
                    throw new InvalidRequestException("Invocation of UDF " + meta.qualifiedName + " failed: " + c);
                }
                catch (IllegalAccessException e)
                {
                    throw new InvalidRequestException("UDF " + meta.qualifiedName + " invocation failed: " + e);
                }
            }

            public boolean isPure()
            {
                return meta.deterministic;
            }
        };
    }
}
