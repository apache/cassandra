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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.inject.Inject;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;

import picocli.CommandLine;

import static org.junit.Assert.assertTrue;

public class NodetoolClassHierarchyTest extends CQLTester
{
    /**
     * Ensures no command in the nodetool hierarchy has duplicate {@code @Inject} fields
     * for the same type across its class inheritance chain.
     * <p>
     * {@code AbstractCommand} declares {@code @Inject} ann for {@code INodeProbeFactory}
     * and {@code Output}. If a subclass or any intermediate classes in the hierarchy
     * redeclares an {@code @Inject} field of the same type, the picocli factory would
     * inject both, leading to ambiguity and wasted resources.
     */
    @Test
    public void testNoDuplicatesForInjectableFields() throws Exception
    {
        checkInjectableDuplicates(NodeTool.createCommandLine(CommandLine.defaultFactory()));
    }

    private void checkInjectableDuplicates(CommandLine command)
    {
        for (CommandLine sub : command.getSubcommands().values())
            checkInjectableDuplicates(sub);

        if (command.getCommandSpec().userObject() instanceof AbstractCommand)
        {
            AbstractCommand userObject = (AbstractCommand) command.getCommandSpec().userObject();
            int nodeProbeFactoryCount = 0;
            int outputCount = 0;
            Class<?> beanClass = userObject.getClass();
            do
            {
                Field[] fields = beanClass.getDeclaredFields();
                for (Field field : fields)
                {
                    if (!field.isAnnotationPresent(Inject.class))
                        continue;
                    if (field.getType().equals(INodeProbeFactory.class))
                        nodeProbeFactoryCount++;
                    else if (field.getType().equals(Output.class))
                        outputCount++;
                    else
                        throw new AssertionError("Unexpected injectable field type: " + field.getType());
                }
            }
            while ((beanClass = beanClass.getSuperclass()) != null);

            if (nodeProbeFactoryCount > 1 || outputCount > 1)
                throw new AssertionError("Multiple injectable fields in the command class hierarchy (should be exactly 1 for each type): " +
                                         userObject.getClass().getCanonicalName());
        }
    }

    /**
     * Commands must be self-contained: every {@code @Option} and {@code @Parameters}
     * a command needs must be declared on the command itself or via {@code @Mixin},
     * not inherited through {@code @ParentCommand}. Self-contained commands can be
     * instantiated and executed independently without walking the parent hierarchy,
     * which is required for remote execution via CQL or MBean.
     */
    @Test
    public void testNodetoolCommandsMustBeSelfContained()
    {
        CommandLine root = new CommandLine(NodetoolCommand.class);
        Map<String, List<String>> affected = new TreeMap<>();

        commandTreeWalker(root, cmd -> {
            List<String> parentArgs = collectConsumedParentArgs(cmd);
            if (!parentArgs.isEmpty())
                affected.put(fullCommandName(cmd), parentArgs);
        });

        assertTrue("The following commands declare @ParentCommand and consume " +
                   "options or parameters from a parent command: " + buildAffectedCommandMessage(affected),
                   affected.isEmpty());
    }

    /**
     * Ensures that all commands implementing {@code LocalCommand} do not establish a JMX connection.
     */
    @Test
    public void testLocalCommandsShouldNotConnect() throws Exception
    {
        CommandLine root = NodeTool.createCommandLine(CommandLine.defaultFactory());
        List<String> failedCommands = new ArrayList<>();

        commandTreeWalker(root, cmd -> {
            Object userObject = cmd.getCommandSpec().userObject();
            if (userObject instanceof LocalCommand && userObject instanceof AbstractCommand)
            {
                AbstractCommand abstractCommand = (AbstractCommand) userObject;
                try
                {
                    if (abstractCommand.shouldConnect())
                        failedCommands.add(fullCommandName(cmd));
                }
                catch (Exception e)
                {
                    failedCommands.add(fullCommandName(cmd) + " (" + e.getMessage() + ")");
                }
            }
        });

        assertTrue("The following commands implement LocalCommand but shouldConnect() does not return false: " + failedCommands,
                   failedCommands.isEmpty());
    }

    /**
     * When command arguments are addressed by name rather than by CLI position, all arguments
     * of a command share a single flat, case-insensitive key namespace: options are keyed by
     * their normalized {@code paramLabel()} and names, positional parameters by their paramLabel.
     * No two arguments of the same command may claim the same normalized key, otherwise one value
     * is silently overwritten on write or misrouted on read (e.g. a command field and a
     * {@code @Mixin} field that produce the same paramLabel).
     */
    @Test
    public void testCommandArgumentKeysAreUnambiguous()
    {
        CommandLine root = new CommandLine(NodetoolCommand.class);
        Map<String, List<String>> affected = new TreeMap<>();

        commandTreeWalker(root, cmd -> {
            List<String> violations = collectAmbiguousArgumentKeys(cmd);
            if (!violations.isEmpty())
                affected.put(fullCommandName(cmd), violations);
        });

        assertTrue("The following commands have ambiguous argument keys (options are keyed by " +
                   "normalized paramLabel and names, positional parameters by paramLabel, all in " +
                   "one case-insensitive namespace). Rename the field or set an explicit, unique " +
                   "paramLabel:\n" +
                   buildAffectedCommandMessage(affected),
                   affected.isEmpty());
    }

    private static List<String> collectAmbiguousArgumentKeys(CommandLine cmd)
    {
        List<String> violations = new ArrayList<>();
        Map<String, String> keyOwners = new LinkedHashMap<>();

        for (CommandLine.Model.OptionSpec option : cmd.getCommandSpec().options())
        {
            if (option.usageHelp() || option.versionHelp())
                continue;

            String owner = "option " + String.join("/", option.names());
            // An option is addressable by its paramLabel and by every name/alias, so all of them
            // must stay unambiguous, a set tolerates a paramLabel equal to one of its own names.
            Set<String> keys = new LinkedHashSet<>();
            keys.add(normalizeOptionName(option.paramLabel()).toLowerCase());
            for (String name : option.names())
                keys.add(normalizeOptionName(name).toLowerCase());

            for (String key : keys)
                claimArgumentKey(key, owner, keyOwners, violations);
        }

        for (CommandLine.Model.PositionalParamSpec param : cmd.getCommandSpec().positionalParameters())
        {
            String owner = String.format("parameter index=%s (%s)", param.index(), param.paramLabel());
            claimArgumentKey(normalizeOptionName(param.paramLabel()).toLowerCase(), owner, keyOwners, violations);
        }

        return violations;
    }

    private static void claimArgumentKey(String key, String owner, Map<String, String> keyOwners, List<String> violations)
    {
        String previousOwner = keyOwners.putIfAbsent(key, owner);
        if (previousOwner != null)
            violations.add(String.format("key '%s' is claimed by both %s and %s", key, previousOwner, owner));
    }

    /** Strips the leading dashes from an option name. */
    private static String normalizeOptionName(String name)
    {
        if (name.startsWith("--"))
            return name.substring(2);
        else if (name.startsWith("-"))
            return name.substring(1);
        return name;
    }

    /**
     * For a given command, follows the {@code @ParentCommand} chain and collects all
     * options and parameters declared on each parent.
     */
    private static List<String> collectConsumedParentArgs(CommandLine cmd)
    {
        Object userObject = cmd.getCommandSpec().userObject();
        if (userObject == null)
            return List.of();

        List<String> parentArgs = new ArrayList<>();
        CommandLine currentCmd = cmd;
        Class<?> currentClass = userObject.getClass();

        while (true)
        {
            Field parentField = findAnnotatedField(currentClass);
            if (parentField == null)
                break;

            CommandLine parentCmd = currentCmd.getParent();
            if (parentCmd == null)
                break;

            addOptionsFromSpec(parentCmd, parentArgs);
            addParametersFromSpec(parentCmd, parentArgs);

            Object parentUserObject = parentCmd.getCommandSpec().userObject();
            if (parentUserObject == null)
                break;

            currentCmd = parentCmd;
            currentClass = parentUserObject.getClass();
        }

        return parentArgs;
    }

    private static void commandTreeWalker(CommandLine cmd, Consumer<CommandLine> consumer)
    {
        consumer.accept(cmd);
        for (CommandLine sub : cmd.getSubcommands().values())
            commandTreeWalker(sub, consumer);
    }

    private static Field findAnnotatedField(Class<?> clazz)
    {
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass())
        {
            for (Field field : c.getDeclaredFields())
            {
                if (field.isAnnotationPresent(CommandLine.ParentCommand.class))
                    return field;
            }
        }
        return null;
    }

    private static void addOptionsFromSpec(CommandLine cmd, List<String> out)
    {
        for (CommandLine.Model.OptionSpec option : cmd.getCommandSpec().options())
        {
            if (option.usageHelp() || option.versionHelp())
                continue;
            out.add(String.format("option '%s': %s (%s)",
                                  cmd.getCommandName(),
                                  String.join("/", option.names()),
                                  option.type().getSimpleName()));
        }
    }

    private static void addParametersFromSpec(CommandLine cmd, List<String> out)
    {
        for (CommandLine.Model.PositionalParamSpec param : cmd.getCommandSpec().positionalParameters())
        {
            out.add(String.format("parameter '%s': %s index=%s (%s)",
                                  cmd.getCommandName(),
                                  param.paramLabel(),
                                  param.index(),
                                  param.type().getSimpleName()));
        }
    }

    /** Build the full command path, e.g. {@code nodetool.removenode.status}. */
    private static String fullCommandName(CommandLine cmd)
    {
        List<String> parts = new ArrayList<>();
        CommandLine current = cmd;
        while (current != null)
        {
            parts.add(0, current.getCommandName());
            current = current.getParent();
        }
        return String.join(".", parts);
    }

    private static String buildAffectedCommandMessage(Map<String, List<String>> affected)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : affected.entrySet())
        {
            sb.append("  Command: ").append(entry.getKey()).append('\n');
            for (String detail : entry.getValue())
                sb.append("    - ").append(detail).append('\n');
        }
        return sb.toString();
    }
}
