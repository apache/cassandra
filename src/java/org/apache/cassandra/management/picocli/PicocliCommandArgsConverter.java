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

package org.apache.cassandra.management.picocli;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.management.SimpleCommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

public class PicocliCommandArgsConverter
{
    /**
     * Extract command arguments from an AbstractCommand instance using picocli reflection.
     *
     * @param command the command instance to extract arguments from
     * @return CommandExecutionArgs containing all options and parameters from the command
     */
    public static <T> CommandExecutionArgs fromCommand(T command)
    {
        CommandMetadata metadata = PicocliCommandMetadata.from(command);
        PicocliCommandMetadata picocliMetadata = (PicocliCommandMetadata) metadata;
        CommandSpec spec = picocliMetadata.getCommandSpec();

        Map<OptionMetadata, Object> options = new LinkedHashMap<>();
        Map<ParameterMetadata, Object> parameters = new LinkedHashMap<>();

        for (CommandLine.Model.OptionSpec optionSpec : spec.options())
        {
            if (!optionSpec.isOption())
                continue;

            Object value = optionSpec.getValue();
            if (value == null)
                continue;

            if (optionSpec.type() == boolean.class || optionSpec.type() == Boolean.class)
            {
                if (Boolean.TRUE.equals(value))
                    options.put(new PicocliOptionMetadata(optionSpec), Boolean.TRUE);
            }
            else
            {
                options.put(new PicocliOptionMetadata(optionSpec), value);
            }
        }

        for (CommandLine.Model.PositionalParamSpec paramSpec : spec.positionalParameters())
        {
            if (!paramSpec.isPositional())
                continue;

            Object value = paramSpec.getValue();
            if (value != null)
                parameters.put(new PicocliParameterMetadata(paramSpec), value);
        }

        return new SimpleCommandExecutionArgs(options, parameters);
    }

    /**
     * Populate an AbstractCommand instance with values from CommandExecutionArgs using picocli setters.
     *
     * @param args the CommandExecutionArgs containing values to set
     * @param command the command instance to populate
     */
    public static <T> void toCommand(CommandExecutionArgs args, T command)
    {
        CommandMetadata metadata = PicocliCommandMetadata.from(command);
        PicocliCommandMetadata picocliMetadata = (PicocliCommandMetadata) metadata;
        CommandSpec spec = picocliMetadata.getCommandSpec();
        Map<OptionMetadata, CommandLine.Model.OptionSpec> optionMap = buildOptionSpecMap(spec);
        Map<ParameterMetadata, CommandLine.Model.PositionalParamSpec> paramMap = buildParameterSpecMap(spec);

        for (Map.Entry<OptionMetadata, Object> entry : args.options().entrySet())
        {
            OptionMetadata optionMetadata = entry.getKey();
            // This value is already converted to the correct type in CommandExecutionArgs.
            Object convertedValue = entry.getValue();

            if (convertedValue == null)
                continue;

            CommandLine.Model.OptionSpec optionSpec = optionMap.get(optionMetadata);
            if (optionSpec == null)
            {
                optionSpec = findOptionSpecByName(spec, optionMetadata.names());
                if (optionSpec == null)
                    throw new IllegalArgumentException("Option not found in command spec: " + Arrays.toString(optionMetadata.names()));
            }

            try
            {
                optionSpec.setter().set(convertedValue);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to set option " + optionMetadata.names()[0] +
                                           " with value " + convertedValue, e);
            }
        }

        for (Map.Entry<ParameterMetadata, Object> entry : args.parameters().entrySet())
        {
            ParameterMetadata paramMetadata = entry.getKey();
            Object value = entry.getValue();

            if (value == null)
                continue;

            CommandLine.Model.PositionalParamSpec paramSpec = paramMap.get(paramMetadata);
            if (paramSpec == null)
            {
                paramSpec = findParameterSpecByIndex(spec, paramMetadata.index());
                if (paramSpec == null)
                    throw new IllegalArgumentException("Parameter not found in command spec at index: " + paramMetadata.index());
            }

            try
            {
                paramSpec.setter().set(value);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to set parameter at index " + paramMetadata.index() +
                                           " with value " + value, e);
            }
        }
    }

    private static Map<OptionMetadata, CommandLine.Model.OptionSpec> buildOptionSpecMap(CommandSpec spec)
    {
        Map<OptionMetadata, CommandLine.Model.OptionSpec> map = new LinkedHashMap<>();
        for (CommandLine.Model.OptionSpec optionSpec : spec.options())
        {
            if (optionSpec.isOption())
                map.put(new PicocliOptionMetadata(optionSpec), optionSpec);
        }
        return map;
    }

    private static Map<ParameterMetadata, CommandLine.Model.PositionalParamSpec> buildParameterSpecMap(CommandSpec spec)
    {
        Map<ParameterMetadata, CommandLine.Model.PositionalParamSpec> map = new LinkedHashMap<>();
        for (CommandLine.Model.PositionalParamSpec paramSpec : spec.positionalParameters())
        {
            if (paramSpec.isPositional())
                map.put(new PicocliParameterMetadata(paramSpec), paramSpec);
        }
        return map;
    }

    private static CommandLine.Model.OptionSpec findOptionSpecByName(CommandSpec spec, String[] names)
    {
        for (CommandLine.Model.OptionSpec optionSpec : spec.options())
        {
            for (String name : names)
            {
                if (java.util.Arrays.asList(optionSpec.names()).contains(name))
                    return optionSpec;
            }
        }
        return null;
    }

    private static CommandLine.Model.PositionalParamSpec findParameterSpecByIndex(CommandSpec spec, int index)
    {
        for (CommandLine.Model.PositionalParamSpec paramSpec : spec.positionalParameters())
        {
            CommandLine.Range indexRange = paramSpec.index();
            if (index >= indexRange.min() && index <= indexRange.max())
                return paramSpec;
        }
        return null;
    }
}
