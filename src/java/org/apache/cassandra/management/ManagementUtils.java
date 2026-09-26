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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandRegistry;

import picocli.CommandLine;

public final class ManagementUtils
{
    public static final String COMMAND_NAME_DELIMITER = ".";

    public static <S> Iterable<S> loadService(Class<S> serviceClz)
    {
        return AccessController.doPrivileged((PrivilegedAction<Iterable<S>>) () -> ServiceLoader.load(serviceClz));
    }

    public static int countCommands(CommandRegistry registry)
    {
        int count = 0;
        for (Map.Entry<String, Command<?>> entry : registry.commands())
        {
            Command<?> cmd = entry.getValue();
            if (cmd instanceof CommandRegistry)
                count += countCommands((CommandRegistry) cmd);
            else
                count++;
        }
        return count;
    }

    public static String stripAngleBrackets(String name)
    {
        if (name == null || name.isEmpty())
            return name;

        String trimmed = name.trim();

        if (trimmed.length() >= 2 &&
            trimmed.charAt(0) == '<' &&
            trimmed.charAt(trimmed.length() - 1) == '>')
        {
            return trimmed.substring(1, trimmed.length() - 1).trim();
        }

        return name;
    }

    /** Normalize the option name by stripping leading dashes. */
    public static String normalizeOptionName(String name)
    {
        if (name.startsWith("--"))
            return name.substring(2);
        else if (name.startsWith("-"))
            return name.substring(1);
        return name;
    }

    /**
     * Builds a concise, client-safe error string from a throwable's cause chain: the distinct, non-empty
     * messages of the throwable and its causes, joined by {@code ": "}. Unlike a full stack trace, this
     * exposes only the human-readable messages (which command validation/usage errors rely on) without
     * leaking internal class names, file paths or line numbers. The full stack trace is logged separately
     * server-side.
     */
    public static String causeMessages(Throwable t)
    {
        Set<String> messages = new LinkedHashSet<>();
        for (Throwable cause : Throwables.getCausalChain(t))
        {
            String message = cause.getMessage();
            if (Strings.isNullOrEmpty(message))
                continue;
            // Skip a message already represented by an ancestor.
            if (messages.stream().anyMatch(existing -> existing.contains(message)))
                continue;
            messages.add(message);
        }
        return messages.isEmpty() ? t.getClass().getSimpleName() : String.join(": ", messages);
    }

    public static String fullCommandName(String parent, String name)
    {
        return Strings.isNullOrEmpty(parent) ? name : String.join(COMMAND_NAME_DELIMITER, parent, name);
    }

    public static <T> String fullCommandName(List<T> parentCommands, Function<T, String> nameExtractor)
    {
        StringBuilder sb = new StringBuilder();
        for (T part : parentCommands)
        {
            String extracted = nameExtractor.apply(part);
            if (Strings.isNullOrEmpty(extracted))
                continue;
            if (sb.length() > 0)
                sb.append(COMMAND_NAME_DELIMITER);
            sb.append(extracted);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T findRegistryCommand(String fullCommandName, CommandRegistry registry)
    {
        String[] parts = fullCommandName.split('\\' + COMMAND_NAME_DELIMITER, 2);
        Command<?> cmd = registry.command(parts[0]);
        if (cmd == null)
            return null;

        if (parts.length == 1)
            return (T) cmd;

        if (cmd instanceof CommandRegistry)
            return findRegistryCommand(parts[1], (CommandRegistry) cmd);

        return null;
    }

    public static Class<?> elementType(CommandLine.Model.ArgSpec spec)
    {
        Class<?> type = spec.type();
        if (!type.isArray() && !java.util.Collection.class.isAssignableFrom(type))
            return null;

        Class<?>[] auxiliaryTypes = spec.auxiliaryTypes();
        return auxiliaryTypes == null || auxiliaryTypes.length == 0 ? null : auxiliaryTypes[0];
    }
}
