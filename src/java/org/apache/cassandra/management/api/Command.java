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

/**
 * A management command following the Command java-pattern. Each command is a self-describing,
 * executable unit that exposes its {@link CommandMetadata} and can be invoked uniformly by
 * the {@link org.apache.cassandra.management.CommandInvokerService} regardless of the
 * transport: JMX or CQL.
 * <p>
 * The type parameter {@code R} is the structured result type returned by
 * {@link #execute(CommandExecutionArgs, CommandExecutionContext)}. Existing picocli-based
 * commands return {@code Void} and write output via services obtained from the context;
 * new commands may return a typed result for protocol-level serialization.
 *
 * @param <R> the command result type, or {@link Void} for commands that only produce
 *            side-effect output through the execution context
 * @see CommandRegistry
 */
public interface Command<R>
{
    /** Get command metadata - replaces argClass() from the cep-38 doc with richer information. */
    CommandMetadata metadata();

    /**
     * Execute the command.
     *
     * @param arguments command arguments.
     * @param context execution context providing access to services via {@link CommandExecutionContext#service(Class)}.
     * @return structured data result, or {@code null} for commands that only produce
     *         side effect output (e.g. existing picocli-based commands).
     */
    R execute(CommandExecutionArgs arguments, CommandExecutionContext context);

    default String name() { return metadata().name(); }
    default String description() { return metadata().description(); }
}
