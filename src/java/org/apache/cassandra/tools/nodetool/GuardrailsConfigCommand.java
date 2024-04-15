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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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

        @Arguments(description = "Specific names or guardrails to get configuration of.")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            GuardrailCategory categoryEnum = GuardrailCategory.parseCategory(guardrailCategory, probe.output().out);

            if (!args.isEmpty() && categoryEnum != null)
                throw new IllegalStateException("Do not specify additional arguments when --category/-c is set.");

            List<Method> allGetters = stream(probe.getGuardrailsMBean().getClass().getDeclaredMethods())
                                      .filter(method -> method.getName().startsWith("get")
                                                        && !method.getName().endsWith("CSV"))
                                      .filter(method -> args.isEmpty() || args.contains(toSnakeCase(method.getName().substring(3))))
                                      .sorted(comparing(Method::getName))
                                      .collect(toList());

            display(probe, allGetters, categoryEnum);
        }

        @Override
        public void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, Method method, String guardrailName) throws Throwable
        {
            Class<?> returnType = method.getReturnType();
            Object value = method.invoke(mBean);

            if (returnType.equals(int.class) || returnType.equals(Integer.class)
                || returnType.equals(long.class) || returnType.equals(Long.class)
                || returnType.equals(boolean.class) || returnType.equals(Boolean.class)
                || returnType.equals(Set.class))
            {
                constructRow(bucket, guardrailName, value.toString());
            }
            else if (returnType.equals(String.class))
            {
                if (value == null || value.toString().isEmpty())
                    constructRow(bucket, guardrailName, "null");
                else
                    constructRow(bucket, guardrailName, value.toString());
            }
            else
            {
                throw new RuntimeException("unhandled return type: " + returnType.getTypeName());
            }
        }
    }

    @Command(name = "setguardrailsconfig", description = "Modify runtime configuration of guardrails.")
    public static class SetGuardrailsConfig extends GuardrailsConfigCommand
    {
        private static final Pattern SETTER_PATTERN = Pattern.compile("^set");

        @Option(name = { "--list", "-l" },
        description = "List all available guardrails setters")
        private boolean list;

        @Option(name = { "--category", "-c" },
        description = "Category of guardrails to filter, can be one of 'values', 'thresholds', 'flags', 'others'.",
        allowedValues = { "values", "thresholds", "flags", "others" })
        private String guardrailCategory;

        @Arguments(usage = "[<setter> <value1> ...]",
        description = "For flags, possible values are 'true' or 'false'. " +
                      "For thresholds, two values are expected, first for warning, second for failure. " +
                      "For values, one value is expected, multiple values separated by comma.")
        private final List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            if (!list && guardrailCategory != null)
                throw new IllegalStateException("--category/-c can be specified only together with --list/-l");

            GuardrailCategory categoryEnum = GuardrailCategory.parseCategory(guardrailCategory, probe.output().out);

            if (args.isEmpty() && !list)
                throw new IllegalStateException("No arguments.");

            if (list)
                display(probe, getAllSetters(probe), categoryEnum);
            else
                executeSetter(probe);
        }

        @Override
        public void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, Method method, String guardrailName) throws Throwable
        {
            if (method.getParameterTypes().length == 1)
                constructRow(bucket, sanitizeSetterName(method), method.getParameterTypes()[0].getName());
            else
                constructRow(bucket, sanitizeSetterName(method), stream(method.getParameterTypes()).map(Class::getName).collect(toList()).toString());
        }

        private List<Method> getAllSetters(NodeProbe probe)
        {
            return stream(probe.getGuardrailsMBean().getClass().getDeclaredMethods())
                   .filter(method -> method.getName().startsWith("set") && !method.getName().endsWith("CSV"))
                   .filter(method -> args.isEmpty() || args.contains(toSnakeCase(method.getName().substring(3))))
                   .sorted(comparing(Method::getName))
                   .collect(toList());
        }

        private String sanitizeSetterName(Method setter)
        {
            return toSnakeCase(SETTER_PATTERN.matcher(setter.getName()).replaceAll(""));
        }

        private void executeSetter(NodeProbe nodeProbe)
        {
            String snakeCaseName = args.get(0);
            String setterName = toCamelCase(args.get(0).startsWith("set_") ? args.get(0) : "set_" + args.get(0));

            Method setter = getAllSetters(nodeProbe).stream()
                                                    .findFirst()
                                                    .orElseThrow(() -> new IllegalStateException(format("Setter method %s not found. " +
                                                                                                        "Run nodetool setguardrailsconfig --list " +
                                                                                                        "to see available setters", setterName)));

            validateArguments(setter, snakeCaseName, args);

            List<String> methodArgs = args.subList(1, args.size());
            try
            {
                setter.invoke(nodeProbe.getGuardrailsMBean(), prepareArguments(methodArgs, setter));
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
                if (value == null || value.equals("null"))
                    return new HashSet<>();
                else
                {
                    return new LinkedHashSet<>(Arrays.asList(value.split(",")));
                }
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

    private static final Map<String, String> toCamelCaseTranslationMap = new HashMap<String, String>()
    {{
        put("set_fields_per_udt_threshold", "setFieldsPerUDTThreshold");
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

    void display(NodeProbe probe, List<Method> methods, GuardrailCategory userCategory)
    {
        try
        {
            List<InternalRow> flags = new ArrayList<>();
            List<InternalRow> thresholds = new ArrayList<>();
            List<InternalRow> values = new ArrayList<>();
            List<InternalRow> others = new ArrayList<>();

            for (Method method : methods)
            {
                String guardrailName = toSnakeCase(method.getName().substring(3));

                List<InternalRow> bucket;

                if (guardrailName.endsWith("_enabled"))
                    bucket = flags;
                else if (guardrailName.endsWith("_threshold"))
                    bucket = thresholds;
                else if (guardrailName.endsWith("_disallowed") ||
                         guardrailName.endsWith("_ignored") ||
                         guardrailName.endsWith("_warned"))
                    bucket = values;
                else
                    bucket = others;

                addRow(bucket, probe.getGuardrailsMBean(), method, guardrailName);
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

    abstract void addRow(List<InternalRow> bucket, GuardrailsMBean mBean, Method method, String guardrailName) throws Throwable;

    public static class InternalRow
    {
        final String name;
        final String value;

        public InternalRow(String name, String value)
        {
            this.name = name;
            this.value = value;
        }
    }

    private static String toSnakeCase(String camelCase)
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

    private static String toCamelCase(String snakeCase)
    {
        if (snakeCase == null || snakeCase.isEmpty())
            return snakeCase;

        String maybeCamelCase = toCamelCaseTranslationMap.get(snakeCase);
        if (maybeCamelCase != null)
            return maybeCamelCase;

        StringBuilder result = new StringBuilder();
        boolean toUpper = false;

        for (int i = 0; i < snakeCase.length(); i++)
        {
            char c = snakeCase.charAt(i);
            if (c == '_')
            {
                toUpper = true;
            }
            else
            {
                result.append(toUpper ? Character.toUpperCase(c) : c);
                toUpper = false;
            }
        }

        return result.toString();
    }
}
