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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.db.guardrails.GuardrailsProxy;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import org.apache.cassandra.utils.LocalizeString;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.guardrails.GuardrailsProxy.CAMEL_PATTERN;
import static org.apache.cassandra.db.guardrails.GuardrailsProxy.toSnakeCaseTranslationMap;

public abstract class GuardrailsConfigCommand extends AbstractCommand
{
    @Command(name = "getguardrailsconfig", description = "Print runtime configuration of guardrails.")
    public static class GetGuardrailsConfig extends GuardrailsConfigCommand
    {
        @Option(names = { "--category", "-c" },
                description = "Category of guardrails to filter, can be one of 'values', 'thresholds', 'flags', 'others'.")
        private GuardrailCategory guardrailCategory;

        @Option(names = { "--expand" },
        description = "Expand all guardrail names so they reflect their counterparts in cassandra.yaml")
        private boolean expand = false;

        @Parameters(index = "0", arity = "0..1", description = "Specific name of a guardrail to get configuration of or all guardrails if not specified.")
        private String guardrailName;

        @Override
        public void execute(NodeProbe probe)
        {
            if (guardrailName != null && guardrailCategory != null)
                throw new IllegalStateException("Do not specify additional arguments when --category/-c is set.");

            guardrailsMBean = probe.getGuardrailsMBean();

            GuardrailsProxy.instance.clientInitialisation(guardrailsMBean, guardrailName, false, true);

            Map<String, List<Method>> allGetters = GuardrailsProxy.instance.getAllGetters();

            if (guardrailName != null)
            {
                List<Method> methods = allGetters.get(guardrailName);
                allGetters = new HashMap<>();
                if (methods != null)
                    allGetters.put(guardrailName, methods);
            }

            if (allGetters.isEmpty())
            {
                assert guardrailName != null;
                throw new IllegalStateException(format("Guardrail %s not found.", guardrailName));
            }

            display(probe, allGetters, guardrailCategory, expand);
        }

        @Override
        public void addRow(List<InternalRow> bucket, List<Method> methods, String guardrailName) throws Throwable
        {
            List<String> values = new ArrayList<>();
            for (Method method : methods)
            {
                Class<?> returnType = method.getReturnType();
                Object value = method.invoke(guardrailsMBean);

                if (returnType.equals(int.class) || returnType.equals(Integer.class)
                    || returnType.equals(long.class) || returnType.equals(Long.class)
                    || returnType.equals(boolean.class) || returnType.equals(Boolean.class)
                    || returnType.equals(Set.class))
                {
                    values.add(value.toString());
                }
                else if (returnType.equals(String.class))
                {
                    if (value == null || value.toString().isEmpty())
                        values.add("null");
                    else
                        values.add(value.toString());
                }
                else
                {
                    throw new RuntimeException("Unhandled return type: " + returnType.getTypeName());
                }
            }

            constructRow(bucket, guardrailName, values.size() == 1 ? values.get(0) : values.toString());
        }
    }

    @Command(name = "setguardrailsconfig", description = "Modify runtime configuration of guardrails.")
    public static class SetGuardrailsConfig extends GuardrailsConfigCommand
    {
        private static final Pattern SETTER_PATTERN = Pattern.compile("^set");

        @CassandraUsage(usage = "[<setter> <value1> ...]",
                description = "For flags, possible values are 'true' or 'false'. " +
                        "For thresholds, two values are expected, first for failure, second for warning. " +
                        "For values, enumeration of values expected or one value where multiple items are separated by comma. " +
                        "Setting for thresholds accepting strings and value guardrails are reset by specifying 'null' or '[]' value. " +
                        "For thresholds accepting integers, the reset value is -1.")
        private List<String> args = new ArrayList<>();

        @Parameters(index = "0", arity = "0..1")
        private String setterName;

        @Parameters(index = "1..*", arity = "0..*", description = "Arguments for the setter. For flags, possible values are 'true' or 'false'. " +
                "For thresholds, two values are expected, first for failure, second for warning. " +
                "For values, enumeration of values expected or one value where multiple items are separated by comma. " +
                "Setting for thresholds accepting strings and value guardrails are reset by specifying 'null' or '[]' value. " +
                "For thresholds accepting integers, the reset value is -1.")
        private List<String> setterArgs = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            args = CommandUtils.concatArgs(setterName, setterArgs);
            if (args.isEmpty())
                throw new IllegalStateException("No arguments.");

            guardrailsMBean = probe.getGuardrailsMBean();

            String snakeCaseName = args.get(0);
            GuardrailsProxy.instance.clientInitialisation(guardrailsMBean, snakeCaseName, true, false);

            Method setter = GuardrailsProxy.instance.getSetter(snakeCaseName);
            if (setter == null)
                throw new IllegalStateException(format("Guardrail %s not found.", snakeCaseName));

            sanitizeArguments(setter, args);
            validateArguments(setter, snakeCaseName, args);

            List<String> methodArgs = args.subList(1, args.size());
            try
            {
                GuardrailsProxy.instance.invoke(setter, GuardrailsProxy.instance.prepareArguments(methodArgs.toArray(new String[0]), setter));
            }
            catch (Exception ex)
            {
                String reason;
                if (ex.getCause() != null && ex.getCause().getMessage() != null)
                    reason = ex.getCause().getMessage();
                else
                    reason = ex.getMessage();

                throw new IllegalStateException(format("Error occured when setting the config for setter %s with arguments %s: %s",
                                                       snakeCaseName, methodArgs, reason));
            }
        }

        @Override
        public void addRow(List<InternalRow> bucket, List<Method> methods, String guardrailName) throws Throwable
        {
            if (methods.size() == 1)
            {
                Method method = methods.get(0);
                if (method.getParameterTypes().length == 1)
                    constructRow(bucket, sanitizeSetterName(method), method.getParameterTypes()[0].getName());
                else
                    constructRow(bucket, sanitizeSetterName(method), stream(method.getParameterTypes()).map(Class::getName).collect(toList()).toString());
            }
        }

        private String sanitizeSetterName(Method setter)
        {
            return toSnakeCase(SETTER_PATTERN.matcher(setter.getName()).replaceAll(""));
        }

        private void sanitizeArguments(Method setter, List<String> args)
        {
            Class<?>[] parameterTypes = setter.getParameterTypes();
            if (parameterTypes.length == 1 && parameterTypes[0] == Set.class)
            {
                if (args.size() > 2)
                {
                    String guardrail = args.get(0);
                    // replace multiple arguments with one which is separated by a single comma
                    String collectedArguments = String.join(",", args.subList(1, args.size()));
                    args.clear();
                    args.add(guardrail);
                    args.add(collectedArguments);
                }
            }
        }

        private void validateArguments(Method setter, String setterName, List<String> args)
        {
            if (args.size() != setter.getParameterCount() + 1)
            {
                throw new IllegalStateException(format("%s is expecting %d argument values. Getting %d instead.",
                                                       setterName,
                                                       setter.getParameterCount(),
                                                       args.size() - 1));
            }
        }
    }

    /**
     * Set of guardrails which are flags, even though their suffix would suggest they are part of "values" which have warned, ignored, and disallowed sub-categories
     */
    private static final Set<String> specialFlags = Set.of("intersect_filtering_query_warned", "zero_ttl_on_twcs_warned");

    GuardrailsMBean guardrailsMBean;

    @VisibleForTesting
    public enum GuardrailCategory
    {
        values,
        thresholds,
        flags,
        others;
    }

    void display(NodeProbe probe, Map<String, List<Method>> methods, GuardrailCategory userCategory, boolean verbose)
    {
        try
        {
            List<InternalRow> flags = new ArrayList<>();
            List<InternalRow> thresholds = new ArrayList<>();
            List<InternalRow> values = new ArrayList<>();
            List<InternalRow> others = new ArrayList<>();

            for (Map.Entry<String, List<Method>> entry : methods.entrySet())
            {
                String key = entry.getKey();

                if (GuardrailsProxy.instance.isFlag(key))
                    addRow(flags, entry.getValue().get(0), key);
                else if (GuardrailsProxy.instance.isValue(key))
                    addRow(values, entry.getValue().get(0), key);
                else if (GuardrailsProxy.instance.isThreshold(key))
                {
                    if (!verbose)
                    {
                        addRow(thresholds, entry.getValue(), key);
                    }
                    else
                    {
                        for (Method method : entry.getValue())
                        {
                            String guardrailName = toSnakeCase(method.getName().substring(3));
                            addRow(thresholds, method, guardrailName);
                        }
                    }
                }
                else
                    addRow(others, entry.getValue().get(0), key);
            }

            TableBuilder tb = new TableBuilder();
            Map<GuardrailCategory, List<InternalRow>> holder = new LinkedHashMap<>();

            holder.put(GuardrailCategory.flags, flags);
            holder.put(GuardrailCategory.thresholds, thresholds);
            holder.put(GuardrailCategory.values, values);
            holder.put(GuardrailCategory.others, others);

            if (userCategory != null)
            {
                populateTable(tb, holder.get(userCategory));
            }
            else
            {
                if (holder.values().stream().flatMap(list -> Stream.of(list.toArray(new InternalRow[0]))).count() == 1)
                {
                    for (Map.Entry<GuardrailCategory, List<InternalRow>> entry : holder.entrySet())
                        populateOne(tb, entry.getValue());
                }
                else
                {
                    for (Map.Entry<GuardrailCategory, List<InternalRow>> entry : holder.entrySet())
                        populateTable(tb, entry.getValue());
                }
            }

            tb.printTo(probe.output().out);
        }
        catch (Throwable e)
        {
            throw new RuntimeException("Error occured when getting the guardrails config", e);
        }
    }

    private void populateTable(TableBuilder tableBuilder, List<InternalRow> bucket)
    {
        for (InternalRow row : bucket)
            tableBuilder.add(row.name, row.value);
    }

    private void populateOne(TableBuilder tableBuilder, List<InternalRow> bucket)
    {
        if (bucket.size() == 1)
            tableBuilder.add(bucket.get(0).value);
    }

    void constructRow(List<InternalRow> bucket, String guardrailName, String value)
    {
        bucket.add(new InternalRow(guardrailName, value));
    }

    void addRow(List<InternalRow> bucket, Method method, String guardrailName) throws Throwable
    {
        List<Method> methods = new ArrayList<>();
        methods.add(method);
        addRow(bucket, methods, guardrailName);
    }

    abstract void addRow(List<InternalRow> bucket, List<Method> method, String guardrailName) throws Throwable;

    public static class InternalRow
    {
        final String name;
        final String value;

        public InternalRow(String name, String value)
        {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InternalRow that = (InternalRow) o;
            return Objects.equals(name, that.name) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }

        @Override
        public String toString()
        {
            return "InternalRow{" +
                   "name='" + name + '\'' +
                   ", value='" + value + '\'' +
                   '}';
        }
    }

    @VisibleForTesting
    public static String toSnakeCase(String camelCase)
    {
        if (camelCase == null || camelCase.isEmpty())
            return camelCase;
        else
        {
            String maybeSnakeCase = toSnakeCaseTranslationMap.get(camelCase);
            if (maybeSnakeCase != null)
                return maybeSnakeCase;

            return LocalizeString.toLowerCaseLocalized(CAMEL_PATTERN.matcher(camelCase).replaceAll("$1_$2"));
        }
    }
}
