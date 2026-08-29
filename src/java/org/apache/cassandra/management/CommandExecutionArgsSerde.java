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

package org.apache.cassandra.management;

import java.lang.reflect.Array;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.management.api.ArgumentMetadata;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;
import org.apache.cassandra.management.picocli.PicocliCommandMetadata;
import org.apache.cassandra.management.picocli.PicocliOptionMetadata;
import org.apache.cassandra.management.picocli.PicocliParameterMetadata;
import org.apache.cassandra.management.picocli.TypeConverterRegistry;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.management.ManagementUtils.normalizeOptionName;
import static org.apache.cassandra.management.api.ParameterMetadata.COMMAND_POSITIONAL_PARAM_PREFIX;

public class CommandExecutionArgsSerde
{
    public static String toJson(CommandExecutionArgs args)
    {
        Map<String, Object> params = new LinkedHashMap<>();

        for (Map.Entry<OptionMetadata, Object> entry : args.options().entrySet())
        {
            OptionMetadata optionMetadata = entry.getKey();
            Object value = entry.getValue();

            if (value == null)
                continue;

            String primaryName = normalizeOptionName(optionMetadata.paramLabel());
            params.put(primaryName, normalizeJsonValue(value));
        }

        for (Map.Entry<ParameterMetadata, Object> entry : args.parameters().entrySet())
        {
            ParameterMetadata paramMetadata = entry.getKey();
            Object value = entry.getValue();

            if (value == null)
                continue;

            int index = paramMetadata.index();
            String indexKey = COMMAND_POSITIONAL_PARAM_PREFIX + index;
            params.put(indexKey, normalizeJsonValue(value));
        }

        return JsonUtils.writeAsJsonString(params);
    }

    public static CommandExecutionArgs fromJson(String json, CommandMetadata metadata)
    {
        if (!(metadata instanceof PicocliCommandMetadata))
            throw new IllegalArgumentException("CommandMetadata must be PicocliCommandMetadata for picocli commands");

        Map<String, Object> jsonMap = JsonUtils.fromJsonMap(json);
        Map<OptionMetadata, Object> options = new LinkedHashMap<>();
        Map<ParameterMetadata, Object> parameters = new LinkedHashMap<>();

        convertFromMap(jsonMap, metadata, options, parameters);
        return new SimpleCommandExecutionArgs(options, parameters);
    }

    public static CommandExecutionArgs fromMap(Map<String, Object> format, CommandMetadata metadata)
    {
        if (!(metadata instanceof PicocliCommandMetadata))
            throw new IllegalArgumentException("CommandMetadata must be PicocliCommandMetadata for picocli commands");

        Map<OptionMetadata, Object> options = new LinkedHashMap<>();
        Map<ParameterMetadata, Object> parameters = new LinkedHashMap<>();

        convertFromMap(format, metadata, options, parameters);
        return new SimpleCommandExecutionArgs(options, parameters);
    }

    private static void convertFromMap(Map<String, Object> source,
                                       CommandMetadata metadata,
                                       Map<OptionMetadata, Object> options,
                                       Map<ParameterMetadata, Object> parameters)
    {
        for (Map.Entry<String, Object> entry : source.entrySet())
        {
            String paramName = entry.getKey();
            Object value = entry.getValue();

            if (value == null)
                continue;

            OptionMetadata option = findOptionByNameIgnoreCase(metadata, paramName);
            if (option != null)
            {
                Object convertedValue = convertValue(value, option);
                options.put(option, convertedValue);
                continue;
            }

            ParameterMetadata param = findParameterByName(metadata, paramName);
            if (param != null)
            {
                Object convertedValue = convertValue(value, param);
                parameters.put(param, convertedValue);
            }
            else
            {
                throw new IllegalArgumentException("Unknown parameter: " + paramName);
            }
        }
    }

    /** Convert value using custom converter if provided, otherwise use basic conversion. */
    private static Object convertValue(Object value, ArgumentMetadata argSpec)
    {
        try
        {
            // Custom type converters are only supported for picocli-based metadata.
            if (argSpec instanceof PicocliParameterMetadata)
                return ((PicocliParameterMetadata) argSpec).convertValue(value);
            else if (argSpec instanceof PicocliOptionMetadata)
                return ((PicocliOptionMetadata) argSpec).convertValue(value);

            return TypeConverterRegistry.convertValueBasic(value, argSpec.type());
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(String.format("Failed to convert value '%s' to type %s: %s",
                                                             value,
                                                             argSpec.type().getName(),
                                                             e.getMessage()), e);
        }
    }

    /**
     * Converts a value from CommandExecutionArgs into a JSON-serializable form before it goes into the
     * Map(String, Object) that is serialized to JSON. Collection implementations (e.g. Set, LinkedHashSet)
     * are normalized to List so the JSON output stays the same shape.
     * <p>
     * The conversion rules are:
     * - Converts primitive arrays (String[], int[], etc.) -> Object[] (JSON-serializable)
     * - Converts any Collection -> List (JSON-serializable)
     * - Leaves other types unchanged
     *
     * @param value the value to convert.
     * @return the converted value.
     */
    private static Object normalizeJsonValue(Object value)
    {
        if (value == null)
            return null;

        if (value.getClass().isArray())
        {
            int length = Array.getLength(value);
            Object[] array = new Object[length];
            for (int i = 0; i < length; i++)
                array[i] = normalizeJsonValue(Array.get(value, i));
            return array;
        }

        if (value instanceof java.util.Collection)
        {
            return ((java.util.Collection<?>) value).stream()
                                                    .map(CommandExecutionArgsSerde::normalizeJsonValue)
                                                    .collect(Collectors.toList());
        }

        return value;
    }

    /**
     * Find ParameterMetadata by name (supports both an index format and paramLabel).
     */
    private static ParameterMetadata findParameterByName(CommandMetadata metadata, String name)
    {
        if (name.startsWith(COMMAND_POSITIONAL_PARAM_PREFIX))
        {
            try
            {
                int index = Integer.parseInt(name.substring(COMMAND_POSITIONAL_PARAM_PREFIX.length()));
                for (ParameterMetadata param : metadata.parameters())
                {
                    if (param.index() == index)
                        return param;
                }
            }
            catch (NumberFormatException e)
            {
                // Not a valid index format, continue to check paramLabel
            }
        }

        for (ParameterMetadata param : metadata.parameters())
        {
            String paramLabel = param.paramLabel();
            if (paramLabel != null && paramLabel.equalsIgnoreCase(name))
                return param;
        }

        return null;
    }

    /**
     * Find OptionMetadata by name (case-insensitive, normalized).
     * Matches against paramLabel and all names/aliases.
     */
    private static OptionMetadata findOptionByNameIgnoreCase(CommandMetadata metadata, String name)
    {
        String normalizedName = normalizeOptionName(name);

        for (OptionMetadata option : metadata.options())
        {
            String paramLabel = normalizeOptionName(option.paramLabel());
            if (paramLabel.equalsIgnoreCase(normalizedName))
                return option;

            for (String alias : option.names())
            {
                String normalizedAlias = normalizeOptionName(alias);
                if (normalizedAlias.equalsIgnoreCase(normalizedName))
                    return option;
            }
        }

        return null;
    }
}
