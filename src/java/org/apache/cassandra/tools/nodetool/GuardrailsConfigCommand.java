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

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public abstract class GuardrailsConfigCommand extends NodeTool.NodeToolCmd
{
    @Command(name = "getguardrailsconfig", description = "Print runtime configuration of guardrails.")
    public static class GetGuardrailsConfig extends GuardrailsConfigCommand
    {
        @Option(name = { "--category", "-c" },
        description = "Category of guardrails to filter, can be one of 'values', 'thresholds', 'flags', 'others'.",
        allowedValues = { "values", "thresholds", "flags", "others" })
        private String guardrailCategory;

        @Option(name = { "--expand" },
        description = "Expand all guardrail names so they reflect their counterparts in cassandra.yaml")
        private boolean expand = false;

        @Arguments(description = "Specific name of a guardrail to get configuration of.")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            GuardrailCategory categoryEnum = GuardrailCategory.parseCategory(guardrailCategory, probe.output().out);

            if (args.size() > 1)
                throw new IllegalStateException("Specify only one guardrail name to get the configuration of or no name to get the configuration of all of them.");

            String guardrailName = !args.isEmpty() ? args.get(0) : null;

            if (guardrailName != null && categoryEnum != null)
                throw new IllegalStateException("Do not specify additional arguments when --category/-c is set.");

            Map<String, List<Method>> allGetters = parseGuardrailNames(probe.getGuardrailsMBean().getClass().getDeclaredMethods(), guardrailName);

            if (allGetters.isEmpty())
            {
                assert guardrailName != null;
                throw new IllegalStateException(format("Guardrail %s not found.", guardrailName));
            }

            display(probe, allGetters, categoryEnum, expand);
        }

        @VisibleForTesting
        public static Map<String, List<Method>> parseGuardrailNames(Method[] guardrailsMethods, String guardrailName)
        {
            Map<String, List<Method>> allGetters = stream(guardrailsMethods)
                                                   .filter(method -> method.getName().startsWith("get")
                                                                     && !method.getName().endsWith("CSV")
                                                                     && !(method.getName().endsWith("WarnThreshold") || method.getName().endsWith("FailThreshold")))
                                                   .filter(method -> guardrailName == null || guardrailName.equals(toSnakeCase(method.getName().substring(3))))
                                                   .collect(Collectors.groupingBy(method -> toSnakeCase(method.getName().substring(3))));

            Map<String, List<Method>> thresholds = stream(guardrailsMethods)
                                                   .filter(method -> method.getName().startsWith("get")
                                                                     && !method.getName().endsWith("CSV")
                                                                     && (method.getName().endsWith("WarnThreshold") || method.getName().endsWith("FailThreshold")))
                                                   .filter(method -> {
                                                       if (guardrailName == null)
                                                           return true;

                                                       String snakeCase = toSnakeCase(method.getName().substring(3));
                                                       String snakeCaseSuccinct = snakeCase.replace("_warn_", "_")
                                                                                           .replace("_fail_", "_");

                                                       return guardrailName.equals(snakeCase) || guardrailName.equals(snakeCaseSuccinct);
                                                   })
                                                   .sorted(comparing(Method::getName))
                                                   .collect(Collectors.groupingBy(method -> {
                                                       String methodName = method.getName().substring(3);
                                                       String snakeCase = toSnakeCase(methodName);
                                                       if (snakeCase.endsWith("warn_threshold"))
                                                           return snakeCase.replaceAll("_warn_", "_");
                                                       else
                                                           return snakeCase.replaceAll("_fail_", "_");
                                                   }));

            allGetters.putAll(thresholds);

            return allGetters.entrySet()
                                   .stream()
                                   .sorted(Map.Entry.comparingByKey())
                                   .collect(Collectors.toMap(Map.Entry::getKey,
                                                             Map.Entry::getValue,
                                                             (e1, e2) -> e1,
                                                             LinkedHashMap::new));
        }

        @Override
        public void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, List<Method> methods, String guardrailName) throws Throwable
        {
            List<String> values = new ArrayList<>();
            for (Method method : methods)
            {
                Class<?> returnType = method.getReturnType();
                Object value = method.invoke(mBean);

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

        @Arguments(usage = "[<setter> <value1> ...]",
        description = "For flags, possible values are 'true' or 'false'. " +
                      "For thresholds, two values are expected, first for failure, second for warning. " +
                      "For values, enumeration of values expected or one value where multiple items are separated by comma. " +
                      "Setting for thresholds accepting strings and value guardrails are reset by specifying 'null' or '[]' value. " +
                      "For thresholds accepting integers, the reset value is -1.")
        private final List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            if (args.isEmpty())
                throw new IllegalStateException("No arguments.");

            String snakeCaseName = args.get(0);

            Method setter = getAllSetters(probe).entrySet().stream()
                                                    .findFirst()
                                                    .map(o -> o.getValue().get(0))
                                                    .orElseThrow(() -> new IllegalStateException(format("Guardrail %s not found.", snakeCaseName)));

            sanitizeArguments(setter, args);
            validateArguments(setter, snakeCaseName, args);

            List<String> methodArgs = args.subList(1, args.size());
            try
            {
                setter.invoke(probe.getGuardrailsMBean(), prepareArguments(methodArgs, setter));
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
        public void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, List<Method> methods, String guardrailName) throws Throwable
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

        private Map<String, List<Method>> getAllSetters(NodeProbe probe)
        {
            return stream(probe.getGuardrailsMBean().getClass().getDeclaredMethods())
                   .filter(method -> method.getName().startsWith("set") && !method.getName().endsWith("CSV"))
                   .filter(method -> args.isEmpty() || args.contains(toSnakeCase(method.getName().substring(3))))
                   .sorted(comparing(Method::getName))
                   .collect(Collectors.groupingBy(method -> toSnakeCase(method.getName().substring(3))))
                   .entrySet()
                   .stream()
                   .sorted(Map.Entry.comparingByKey())
                   .collect(Collectors.toMap(Map.Entry::getKey,
                                             Map.Entry::getValue,
                                             (e1, e2) -> e1,
                                             LinkedHashMap::new));
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

        private Object[] prepareArguments(List<String> args, Method method)
        {
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object[] arguments = new Object[args.size()];

            for (int i = 0; i < args.size(); i++)
                arguments[i] = castType(parameterTypes[i], args.get(i));

            if (method.getName().endsWith("Threshold"))
            {
                List<Object> thresholdArgs = Arrays.asList(arguments);
                Collections.reverse(thresholdArgs);
                arguments = thresholdArgs.toArray();
            }

            return arguments;
        }

        private Object castType(Class<?> targetType, String value) throws IllegalArgumentException
        {
            if (targetType == String.class)
                return value.equals("null") ? "" : value;
            else if (targetType == int.class || targetType == Integer.class)
                return getNumber(value, Integer::parseInt, -1);
            else if (targetType == long.class || targetType == Long.class)
                return getNumber(value, Long::parseLong, -1);
            else if (targetType == boolean.class || targetType == Boolean.class)
            {
                return getNumber(value, (v) -> {
                    if (!v.equals("true") && !v.equals("false"))
                        throw new IllegalStateException("Use 'true' or 'false' values for booleans");

                    return Boolean.parseBoolean(v);
                }, false);
            }
            else if (targetType == Set.class)
            {
                if (value == null || value.equals("null") || value.equals("[]"))
                    return new HashSet<>();
                else
                    return new LinkedHashSet<>(Arrays.asList(value.split(",")));
            }
            else
            {
                throw new IllegalArgumentException(format("unsupported type: %s", targetType));
            }
        }

        private <T> T getNumber(String value, Function<String, T> transformer, T defaultValue)
        {
            if (value == null || value.equals("null"))
                return defaultValue;

            try
            {
                return transformer.apply(value);
            }
            catch (NumberFormatException ex)
            {
                throw new IllegalStateException(format("Unable to parse value %s", value), ex);
            }
        }
    }

    private static final Pattern CAMEL_PATTERN = Pattern.compile("([a-z])([A-Z])");

    /**
     * Special map for methods which do not adhere to camel-case convention precisely.
     * These will be translated manually.
     */
    private static final Map<String, String> toSnakeCaseTranslationMap = new HashMap<String, String>()
    {{
        put("FieldsPerUDTFailThreshold", "fields_per_udt_fail_threshold");
        put("FieldsPerUDTWarnThreshold", "fields_per_udt_warn_threshold");
        put("FieldsPerUDTThreshold", "fields_per_udt_threshold");
    }};

    @VisibleForTesting
    public enum GuardrailCategory
    {
        values,
        thresholds,
        flags,
        others;

        public static GuardrailCategory parseCategory(String category, PrintStream out)
        {
            if (category == null)
                return null;

            try
            {
                return GuardrailCategory.valueOf(category.toLowerCase());
            }
            catch (IllegalArgumentException ex)
            {
                String enabledValues = Arrays.stream(GuardrailCategory.values())
                                             .map(GuardrailCategory::name)
                                             .collect(Collectors.joining(","));
                out.printf("%nError: Illegal value for -c/--category used: '"
                           + category + "'. Supported values are " + enabledValues + ".%n");
                System.exit(1);
                return null;
            }
        }
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
                List<InternalRow> bucket;

                if (key.endsWith("_enabled"))
                    bucket = flags;
                else if (key.endsWith("_threshold"))
                {
                    if (!verbose)
                    {
                        addRow(thresholds, probe.getGuardrailsMBean(), entry.getValue(), entry.getKey());
                    }
                    else
                    {
                        for (Method method : entry.getValue())
                        {
                            String guardrailName = toSnakeCase(method.getName().substring(3));
                            addRow(thresholds, probe.getGuardrailsMBean(), method, guardrailName);
                        }
                    }
                    continue;
                }
                else if (key.endsWith("_disallowed") ||
                         key.endsWith("_ignored") ||
                         key.endsWith("_warned"))
                    bucket = values;
                else
                    bucket = others;

                addRow(bucket, probe.getGuardrailsMBean(), entry.getValue().get(0), key);
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

    void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, Method method, String guardrailName) throws Throwable
    {
        List<Method> methods = new ArrayList<>();
        methods.add(method);
        addRow(bucket, mBean, methods, guardrailName);
    }

    abstract void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, List<Method> method, String guardrailName) throws Throwable;

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

            return CAMEL_PATTERN.matcher(camelCase).replaceAll("$1_$2").toLowerCase();
        }
    }
}
