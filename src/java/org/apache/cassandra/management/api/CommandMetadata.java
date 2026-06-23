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

package org.apache.cassandra.management.api;

import java.util.List;

/**
 * CommandMetadata is a protocol-agnostic descriptor of a management command's structure
 * and metadata information.
 * <p>
 * Each {@link Command} exposes a {@code CommandMetadata} that describes the command's name,
 * human-readable description, optional aliases, accepted options (named flags/values),
 * positional parameters, and subcommands.
 * <p>
 * This metadata is used by:
 * <ul>
 *   <li>{@link org.apache.cassandra.management.CommandExecutionArgsSerde} to serialize/deserialize
 *       command arguments to and from JSON or CQL row maps.</li>
 *   <li>{@link org.apache.cassandra.management.CommandInvokerService} to look up and invoke
 *       commands over existing MBean interfaces (only intefaces and corresponding implementaions
 *       are used, JMX might be disabled).;</li>
 *   <li>CQL syntax {@code COMMAND} to validate and convert user-supplied arguments.</li>
 * </ul>
 * <p>
 * The interface intentionally has no {@code parent()} accessor. Parent-child relationships are
 * owned by the {@link org.apache.cassandra.management.CassandraCommandRegistry} (and its
 * underlying {@link CommandRegistry} contract) rather than by individual commands, so that
 * a command can be registered in different registries.
 */
public interface CommandMetadata
{
    String name();
    String description();

    List<OptionMetadata> options();
    List<ParameterMetadata> parameters();
    List<CommandMetadata> subcommands();
}
