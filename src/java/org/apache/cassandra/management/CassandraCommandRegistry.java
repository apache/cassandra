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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionContext;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.CommandRegistry;
import org.apache.cassandra.management.api.CommandsProvider;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;

import static org.apache.cassandra.management.ManagementUtils.loadService;

public class CassandraCommandRegistry implements CommandRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraCommandRegistry.class);

    // TODO CASSANDRA-XXXXX These long-running commands block the calling thread for
    //  the entire command duration. They are excluded from the management API until a
    //  ProgressCommand (extends Command) interface is implemented to support long-running
    //  command execution with progress reporting.
    static final Set<String> UNSUPPORTED_COMMANDS = Set.of("repair", "consensus_admin");

    private final Map<String, Command<?>> commandMap = new ConcurrentHashMap<>();

    public CassandraCommandRegistry()
    {
        for (CommandsProvider provider : loadService(CommandsProvider.class))
            provider.commands().forEach(this::register);
    }

    public void register(Command<?> command)
    {
        if (UNSUPPORTED_COMMANDS.contains(command.name()))
        {
            logger.info("Skipping unsupported command '{}': long-running commands require " +
                        "ProgressCommand support (not yet implemented)", command.name());
            return;
        }

        Command<?> prev = commandMap.putIfAbsent(command.name(), command);

        if (prev != null)
            throw new IllegalStateException(String.format("Command name conflict: '%s' is already registered",
                                                          command.name()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Command<T> command(String name)
    {
        return (Command<T>) commandMap.get(name);
    }

    @Override
    public Iterable<Map.Entry<String, Command<?>>> commands()
    {
        return commandMap.entrySet();
    }

    @Override
    public CommandMetadata metadata()
    {
        // Return metadata that reflects this is a registry, not a leaf command
        return new RootCommandMetadata();
    }

    @Override
    public String execute(CommandExecutionArgs arguments, CommandExecutionContext context)
    {
        // CommandRegistry is not directly executable: routing happens at the invoker/CQL layer.
        throw new UnsupportedOperationException(
            String.format("CommandRegistry '%s' is not directly executable. " +
                         "Specify a subcommand. Available commands: %s",
                         name(),
                         String.join(", ", commandMap.keySet())));
    }

    private class RootCommandMetadata implements CommandMetadata
    {
        @Override
        public String name()
        {
            return "root";
        }

        @Override
        public String description()
        {
            return "Root command registry - use a specific subcommand";
        }

        @Override
        public List<OptionMetadata> options()
        {
            return Collections.emptyList();
        }

        @Override
        public List<ParameterMetadata> parameters()
        {
            return Collections.emptyList();
        }

        @Override
        public List<CommandMetadata> subcommands()
        {
            return commandMap.values().stream()
                    .map(Command::metadata)
                    .collect(Collectors.toList());
        }
    }
}
