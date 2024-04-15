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

package org.apache.cassandra.tools.nodetool;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;


import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
name = "setguardrailsconfig",
description = "Modify guardrails configurations. " +
              "Please use --list to find available setters. " +
              "For Threshold setter please provide <warn> <fail> thresholds. " +
              "For Properties setter please use the setter ends with CSV and pass in Comma-separated list. " +
              "To disable the guardrail, put 'null' as the argument.")
public class SetGuardrailsConfig extends NodeTool.NodeToolCmd
{
    @Option(title = "list_guardrails_setters",
    name = "--list",
    description = "List all available guardrails setters")
    private boolean listSetters = false;

    @Arguments(usage = "[<setter> <value1> ...]", description = "Call setter to modify guardrail configuration")
    private final List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        // Get all available setters for Guardrails
        GuardrailsMBean mbean = probe.getGuardrailsMBean();
        Method[] methods = mbean.getClass().getDeclaredMethods();
        List<Method> allSetters = Arrays.stream(methods)
                                        .filter(method -> method.getName().startsWith("set"))
                                        .sorted(Comparator.comparing(Method::getName))
                                        .collect(Collectors.toList());
        if (listSetters)
        {
            StringBuilder sb = new StringBuilder();
            for (Method setter : allSetters)
            {
                sb.append(setter.getName()).append('\t');
                if (setter.getParameterTypes().length == 0)
                {
                    sb.append("No argument");
                }
                else if (setter.getParameterTypes().length == 1)
                {
                    sb.append(setter.getParameterTypes()[0].getName());
                }
                else
                {
                    sb.append(Arrays.stream(setter.getParameterTypes()).map(Class::getName).collect(Collectors.toList()));
                }
                sb.append('\n');
            }
            probe.output().out.println(sb);
            return;
        }

        // verify setter name
        String setterName = args.get(0);
        Method setter = allSetters.stream()
                                  .filter(method -> method.getName().equals(setterName))
                                  .findFirst()
                                  .orElseThrow(() -> new RuntimeException(String.format("Setter method %s not found. " +
                                                                              "Run nodetool setguardrailsconfig --list " +
                                                                              "to see available setters",
                                                                              setterName)));
        // verify args count
        if (args.size() != setter.getParameterCount() + 1)
        {
            throw new RuntimeException(String.format("%s is expecting %d args. Getting %d instead.",
                                                     setterName,
                                                     setter.getParameterCount(),
                                                     args.size() - 1));
        }
        // invoke method with args
        List<String> methodArgs = args.subList(1, args.size());
        try
        {
            setter.invoke(mbean, prepareArguments(methodArgs, setter));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error occured when setting the config", e);
        }
    }

    private Object[] prepareArguments(List<String> args, Method method)
    {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] arguments = new Object[args.size()];
        for (int i = 0; i < args.size(); i++)
        {
            arguments[i] = castType(parameterTypes[i], args.get(i));
        }
        return arguments;
    }

    private Object castType(Class<?> targetType, String value) throws IllegalArgumentException
    {
        if (targetType == String.class)
        {
            if (value.equals("null"))
            {
                return "";
            }
            return value;
        }
        else if (targetType == int.class || targetType == Integer.class)
        {
            if (value.equals("null"))
            {
                return -1;
            }
            return Integer.parseInt(value);
        }
        else if (targetType == long.class || targetType == Long.class)
        {
            if (value.equals("null"))
            {
                return -1;
            }
            return Long.parseLong(value);
        }
        else if (targetType == boolean.class || targetType == Boolean.class)
        {
            return Boolean.parseBoolean(value);
        }
        else
        {
            // unhandled type
            throw new IllegalArgumentException("unsupported type: " + targetType + '.'+
                                               "Please use the setter ends with CSV and pass in comma-separated list");
        }
    }
}
