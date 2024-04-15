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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getguardrailsconfig", description = "Print current guardrails configurations")
public class GetGuardrailsConfig extends NodeTool.NodeToolCmd
{
    @Option(title = "list_all_guardrails_config",
    name = { "--all" },
    description = "List all guardrails configuration (including disabled)")
    private boolean showFullConfig = false;

    private final StringBuilder sb = new StringBuilder();

    @Override
    public void execute(NodeProbe probe)
    {
        GuardrailsMBean mbean = probe.getGuardrailsMBean();
        sb.append("Guardrails Configuration:\n");
        printConfig(mbean);
        probe.output().out.println(sb);
    }

    private void printConfig(GuardrailsMBean mbean)
    {
        // Get all available getters for Guardrails
        Method[] methods = mbean.getClass().getDeclaredMethods();
        List<Method> allGetters = Arrays.stream(methods)
                                        .filter(method -> method.getName().startsWith("get"))
                                        .sorted(Comparator.comparing(Method::getName))
                                        .collect(Collectors.toList());
        try
        {
            for (Method getter : allGetters)
            {
                String guardrailName = getter.getName().substring(3);
                Class<?> type = getter.getReturnType();
                Object res = getter.invoke(mbean);
                printResObject(res, guardrailName, type);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error occured when getting the guardrails config", e);
        }
    }

    private void printResObject(Object res, String name, Class<?> returnType)
    {
        String strVal = "";
        boolean isGuardrailEnabled;
        if (returnType.equals(int.class) || returnType.equals(Integer.class))
        {
            isGuardrailEnabled = (Integer)res > 0;
            strVal = res.toString();
        }
        else if (returnType.equals(long.class) || returnType.equals(Long.class))
        {
            isGuardrailEnabled = (Long)res > 0;
            strVal = res.toString();
        }
        else if (returnType.equals(boolean.class) || returnType.equals(Boolean.class))
        {
            isGuardrailEnabled = !(Boolean)res;
            strVal = res.toString();
        }
        else if (returnType.equals(String.class))
        {
            if (res == null || res.toString().isEmpty())
            {
                strVal = "null";
            }
            else
            {
                strVal = res.toString();
            }
            isGuardrailEnabled = !strVal.equals("null") && !strVal.isEmpty();
        }
        else if (returnType.equals(Set.class))
        {
            // skip Set<String> return type, we should have an equivalent CSV method that
            // returns comma-separated string list.
            return;
        }
        else
        {
            // unhandled type
            throw new RuntimeException("unhandled return type: " + returnType.getTypeName());
        }
        if (showFullConfig || isGuardrailEnabled)
        {
            // print only if ask for all config or the guardrail is enabled
            sb.append(name).append(": ").append(strVal).append('\n');
        }
    }
}
