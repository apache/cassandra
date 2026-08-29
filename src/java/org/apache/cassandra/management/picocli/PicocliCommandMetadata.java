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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

/**
 * Implementation of CommandMetadata that extracts metadata from picocli CommandSpec.
 */
public class PicocliCommandMetadata implements CommandMetadata
{
    private final CommandSpec commandSpec;

    public PicocliCommandMetadata(CommandSpec commandSpec)
    {
        this.commandSpec = commandSpec;
    }

    /**
     * Create CommandMetadata from a command class.
     */
    public static CommandMetadata from(Class<?> commandClass)
    {
        CommandSpec spec = CommandSpec.forAnnotatedObject(commandClass);
        return new PicocliCommandMetadata(spec);
    }

    /**
     * Create CommandMetadata from a command instance.
     */
    public static CommandMetadata from(Object commandInstance)
    {
        CommandSpec spec = CommandSpec.forAnnotatedObject(commandInstance);
        return new PicocliCommandMetadata(spec);
    }

    @Override
    public String name()
    {
        return commandSpec.name();
    }

    @Override
    public String description()
    {
        String[] description = commandSpec.usageMessage().description();
        if (description == null || description.length == 0)
            return "";
        return String.join("\n", description);
    }

    @Override
    public List<OptionMetadata> options()
    {
        List<OptionMetadata> options = new ArrayList<>();
        for (CommandLine.Model.OptionSpec option : commandSpec.options())
        {
            if (option.isOption())
                options.add(new PicocliOptionMetadata(option));
        }
        return options;
    }

    @Override
    public List<ParameterMetadata> parameters()
    {
        List<ParameterMetadata> parameters = new ArrayList<>();
        for (CommandLine.Model.PositionalParamSpec positional : commandSpec.positionalParameters())
        {
            if (positional.isPositional())
                parameters.add(new PicocliParameterMetadata(positional));
        }
        return parameters;
    }

    @Override
    public List<CommandMetadata> subcommands()
    {
        return commandSpec.subcommands().values().stream()
                .map(subcommand -> new PicocliCommandMetadata(subcommand.getCommandSpec()))
                .collect(Collectors.toList());
    }

    /**
     * Returns the underlying CommandSpec.
     */
    public CommandSpec getCommandSpec()
    {
        return commandSpec;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PicocliCommandMetadata)) return false;
        PicocliCommandMetadata that = (PicocliCommandMetadata) o;
        return Objects.equals(commandSpec, that.commandSpec);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(commandSpec);
    }
}

