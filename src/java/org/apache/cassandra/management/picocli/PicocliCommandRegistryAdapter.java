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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionContext;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.CommandRegistry;
import org.apache.cassandra.tools.nodetool.AbstractCommand;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

/**
 * Adapter that wraps a picocli command with subcommands and implements CommandRegistry
 * using the Composite pattern. This allows hierarchical picocli commands to be used
 * as CommandRegistry instances.
 * <p>
 * For example, {@code CompressionDictionaryCommandGroup} has subcommands (train, list, export, import),
 * so it would be wrapped by this adapter to provide a CommandRegistry interface.
 * <p>
 * The adapter recursively adapts subcommands:
 * <ul>
 *   <li>If a subcommand has subcommands -> creates another {@code PicocliCommandRegistryAdapter}</li>
 *   <li>If a subcommand is a leaf -> creates a {@code PicocliCommandAdapter}</li>
 * </ul>
 */
public class PicocliCommandRegistryAdapter implements CommandRegistry
{
    private final CommandSpec commandSpec;
    private final CommandMetadata commandMetadata;
    private final Map<String, Command<?>> subcommandMap = new ConcurrentHashMap<>();

    /**
     * Create an adapter for a picocli command class that has subcommands.
     * @param commandClass the picocli command class (must have subcommands)
     */
    public PicocliCommandRegistryAdapter(Class<? extends AbstractCommand> commandClass)
    {
        this.commandSpec = CommandSpec.forAnnotatedObject(commandClass);
        this.commandMetadata = new PicocliCommandMetadata(commandSpec);
        
        if (commandSpec.subcommands().isEmpty())
        {
            throw new IllegalArgumentException(
                String.format("Command class %s does not have subcommands. " +
                             "Use PicocliCommandAdapter for leaf commands.",
                             commandClass.getName()));
        }

        for (Map.Entry<String, CommandLine> e : commandSpec.subcommands().entrySet())
            adaptSubcommands(e.getKey(), e.getValue());
    }

    /**
     * Recursively adapt all subcommands from the picocli command. Uses a Composite pattern:
     * subcommands with subcommands become registries, leaf subcommands become command adapters.
     */
    private void adaptSubcommands(String commandName, CommandLine command)
    {
        CommandSpec subcommandSpec = command.getCommandSpec();
        Command<?> adaptedCommand;

        if (!subcommandSpec.subcommands().isEmpty())
        {
            adaptedCommand = new PicocliCommandRegistryAdapter(command.getCommand());
        }
        else
        {
            Class<?> subcommandClass = command.getCommand().getClass();
            if (!AbstractCommand.class.isAssignableFrom(subcommandClass))
            {
                throw new IllegalArgumentException(
                String.format("Subcommand class %s is not an AbstractCommand and cannot be adapted. " +
                              "Only AbstractCommand subclasses are supported for leaf commands.",
                              subcommandClass.getName()));
            }

            adaptedCommand = new PicocliCommandAdapter((Class<? extends AbstractCommand>) subcommandClass);
        }

        subcommandMap.put(commandName, adaptedCommand);

        for (String alias : subcommandSpec.aliases())
            subcommandMap.putIfAbsent(alias, adaptedCommand);
    }

    @Override
    public CommandMetadata metadata()
    {
        return commandMetadata;
    }

    @Override
    public String execute(CommandExecutionArgs arguments, CommandExecutionContext context)
    {
        // CommandRegistry is not directly executable: routing happens at the invoker/CQL layer.
        throw new UnsupportedOperationException(
            String.format("CommandRegistry '%s' is not directly executable. " +
                         "Specify a subcommand. Available commands: %s",
                         name(),
                         String.join(", ", subcommandMap.keySet())));
    }

    @Override
    public Command<?> command(String name)
    {
        return subcommandMap.get(name);
    }

    @Override
    public Iterable<Map.Entry<String, Command<?>>> commands()
    {
        return subcommandMap.entrySet();
    }
}

