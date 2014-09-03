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

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * User-defined function using Java source code in UDF body.
 * <p/>
 * This is used when the LANGUAGE of the UDF definition is "java".
 */
final class JavaSourceBasedUDF extends AbstractJavaUDF
{
    static final AtomicInteger clsIdgen = new AtomicInteger();

    JavaSourceBasedUDF(FunctionName name,
                       List<ColumnIdentifier> argNames,
                       List<AbstractType<?>> argTypes,
                       AbstractType<?> returnType,
                       String language,
                       String body,
                       boolean deterministic)
    throws InvalidRequestException
    {
        super(name, argNames, argTypes, returnType, language, body, deterministic);
    }

    String requiredLanguage()
    {
        return "java";
    }

    Method resolveMethod() throws InvalidRequestException
    {
        Class<?> jReturnType = javaReturnType();
        Class<?>[] paramTypes = javaParamTypes();

        StringBuilder code = new StringBuilder();
        code.append("public static ").
             append(jReturnType.getName()).append(' ').
             append(name.name).append('(');
        for (int i = 0; i < paramTypes.length; i++)
        {
            if (i > 0)
                code.append(", ");
            code.append(paramTypes[i].getName()).
                 append(' ').
                 append(argNames.get(i));
        }
        code.append(") { ");
        code.append(body);
        code.append('}');

        ClassPool classPool = ClassPool.getDefault();
        CtClass cc = classPool.makeClass("org.apache.cassandra.cql3.udf.gen.C" + javaIdentifierPart(name.toString()) + '_' + clsIdgen.incrementAndGet());
        try
        {
            cc.addMethod(CtNewMethod.make(code.toString(), cc));
            Class<?> clazz = cc.toClass();
            return clazz.getMethod(name.name, paramTypes);
        }
        catch (LinkageError e)
        {
            throw new InvalidRequestException("Could not compile function '" + name + "' from Java source: " + e.getMessage());
        }
        catch (CannotCompileException e)
        {
            throw new InvalidRequestException("Could not compile function '" + name + "' from Java source: " + e.getReason());
        }
        catch (NoSuchMethodException e)
        {
            throw new InvalidRequestException("Could not build function '" + name + "' from Java source");
        }
    }

    private static String javaIdentifierPart(String qualifiedName)
    {
        StringBuilder sb = new StringBuilder(qualifiedName.length());
        for (int i = 0; i < qualifiedName.length(); i++)
        {
            char c = qualifiedName.charAt(i);
            if (Character.isJavaIdentifierPart(c))
                sb.append(c);
        }
        return sb.toString();
    }
}
