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
package org.apache.cassandra.inject;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Refer to <a href="https://github.com/bytemanproject/byteman/blob/master/docs/asciidoc/src/main/asciidoc/chapters/Byteman-Rule-Language.adoc"/>
 * and injections.md files in the root directory.
 */
public class InvokePointBuilder
{
    private String invokePoint = "AT ENTRY";
    private String targetClassOrInterface;
    private boolean targetInterface;
    private String targetMethod;

    public static InvokePointBuilder newInvokePoint()
    {
        return new InvokePointBuilder();
    }

    public InvokePointBuilder onClass(Class<?> targetClass)
    {
        if (targetClass.isInterface())
        {
            return onInterface(targetClass.getName());
        }
        else
        {
            return onClass(targetClass.getName());
        }
    }

    public InvokePointBuilder onClass(String targetClass)
    {
        this.targetClassOrInterface = targetClass;
        this.targetInterface = false;
        return this;
    }

    public InvokePointBuilder onInterface(String targetInterface)
    {
        this.targetClassOrInterface = targetInterface;
        this.targetInterface = true;
        return this;
    }

    public InvokePointBuilder onClass(Class<?> enclosingClass, String targetClass)
    {
        this.targetClassOrInterface = String.format("%s$%s", enclosingClass.getName(), targetClass);
        this.targetInterface = false;
        return this;
    }

    public InvokePointBuilder onMethod(String targetMethod, Object... methodArgs)
    {
        if (methodArgs.length > 0)
        {
            targetMethod = targetMethod + Arrays.stream(methodArgs)
                                                .map(arg -> (arg instanceof Class<?>) ? ((Class<?>) arg).getName() : String.valueOf(arg))
                                                .collect(Collectors.joining(",", "(", ")"));
        }
        this.targetMethod = targetMethod;
        return this;
    }

    public InvokePointBuilder atEntry()
    {
        invokePoint = "AT ENTRY";
        return this;
    }

    public InvokePointBuilder atExit()
    {
        invokePoint = "AT EXIT";
        return this;
    }

    public InvokePointBuilder at(String atExpression)
    {
        invokePoint = String.format("AT %s", atExpression);
        return this;
    }

    public InvokePointBuilder after(String afterExpression)
    {
        invokePoint = String.format("AFTER %s", afterExpression);
        return this;
    }

    public InvokePointBuilder atInvoke(String method)
    {
        invokePoint = String.format("AT INVOKE %s", method);
        return this;
    }

    public InvokePointBuilder atExceptionExit()
    {
        invokePoint = "AT EXCEPTION EXIT";
        return this;
    }

    public String getTargetClassOrInterface()
    {
        return targetClassOrInterface;
    }

    String buildInternal()
    {
        return String.format("%s %s\nMETHOD %s\n%s",
                targetInterface ? "INTERFACE" : "CLASS",
                             targetClassOrInterface,
                             targetMethod,
                             invokePoint);
    }
}
