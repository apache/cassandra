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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * UDF implementation base class for reflection and Java source UDFs.
 */
abstract class AbstractJavaUDF extends UDFunction
{
    private final Method method;

    AbstractJavaUDF(FunctionName name,
                    List<ColumnIdentifier> argNames,
                    List<AbstractType<?>> argTypes,
                    AbstractType<?> returnType,
                    String language,
                    String body,
                    boolean deterministic)
    throws InvalidRequestException
    {
        super(name, argNames, argTypes, returnType, language, body, deterministic);
        assert language.equals(requiredLanguage());
        this.method = resolveMethod();
    }

    abstract String requiredLanguage();

    abstract Method resolveMethod() throws InvalidRequestException;

    protected Class<?>[] javaParamTypes()
    {
        Class<?> paramTypes[] = new Class[argTypes.size()];
        for (int i = 0; i < paramTypes.length; i++)
            paramTypes[i] = argTypes.get(i).getSerializer().getType();
        return paramTypes;
    }

    protected Class<?> javaReturnType()
    {
        return returnType.getSerializer().getType();
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
            result = method.invoke(null, parms);
            @SuppressWarnings("unchecked") ByteBuffer r = result != null ? ((AbstractType) returnType).decompose(result) : null;
            return r;
        }
        catch (InvocationTargetException | IllegalAccessException e)
        {
            Throwable c = e.getCause();
            logger.error("Invocation of function '{}' failed", this, c);
            throw new InvalidRequestException("Invocation of function '" + this + "' failed: " + c);
        }
    }
}
