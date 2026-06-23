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


import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionContext;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.CommandRegistry;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;
import static org.apache.cassandra.management.ManagementUtils.countCommands;
import static org.apache.cassandra.management.ManagementUtils.findRegistryCommand;
import static org.apache.cassandra.management.ManagementUtils.fullCommandName;

/**
 * Service that manages the command registry lifecycle and execution.
 * <p>
 * Similar to StorageService, SnapshotManager the service coordinates various aspects of command management:
 * - Manages lifecycle (initialization, shutdown)
 * - Coordinates MBean registration
 * - Provides command execution coordination
 * - Integrates with daemon startup
 * - Manages state (registry, MBean instances)
 */
public class CommandInvokerService implements CommandInvokerServiceMBean
{
    private static final String MBEAN_DOMAIN = "org.apache.cassandra.management";
    private static final String MBEAN_TYPE_COMMAND = "Command";
    private static final int MAX_EXECUTION_HISTORY = 100;
    private static final Logger logger = LoggerFactory.getLogger(CommandInvokerService.class);

    public static final CommandInvokerService instance = new CommandInvokerService();

    private final BoundedExecutionHistory executionHistory = new BoundedExecutionHistory(MAX_EXECUTION_HISTORY);
    private final Map<String, ObjectName> commandMBeanNames = new ConcurrentHashMap<>();
    private final MBeanAccessor accessor = new InternalNodeMBeanAccessor();
    private final CommandRegistry registry;

    private volatile boolean started = false;

    private CommandInvokerService()
    {
        this.registry = new CassandraCommandRegistry();
    }

    public synchronized void start()
    {
        if (started)
            return;

        logger.info("Starting command service");

        registerCommandMBeansRecursively(registry, "");
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        started = true;
        logger.info("Command service started with '{}' commands", countCommands(registry));
    }

    public static void shutdown()
    {
        instance.stop();
    }

    public synchronized void stop()
    {
        if (!started)
            return;

        logger.info("Stopping command service");

        unregisterCommandMBeans();

        try
        {
            MBeanWrapper.instance.unregisterMBean(MBEAN_NAME);
        }
        catch (Exception e)
        {
            logger.warn("Failed to unregister CommandInvokerService MBean", e);
        }

        started = false;
        logger.info("Command service stopped");
    }

    public CommandRegistry getRegistry()
    {
        return registry;
    }

    /**
     * @param fullCommandName Full command name, including command demimiter for subcommands.
     * @param argumentsSupplier Supplier of command execution arguments.
     *                          This is used to defer argument construction until after all validation checks are passed.
     * @return captured output of the command execution.
     */
    public CommandResult invokeCommand(String fullCommandName, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        if (!started)
            throw new IllegalStateException("CommandInvokerService is not started");

        Command<?> command = findRegistryCommand(fullCommandName, registry);
        if (command == null)
            throw new IllegalArgumentException("Command not found: " + fullCommandName);

        return  invokeCommand(fullCommandName, command, argumentsSupplier);
    }

    /**
     * Execute a command by name with the given arguments.
     *
     * @param command to execute.
     * @param argumentsSupplier supplier of command execution arguments.
     * @return captured output of the command execution.
     */
    private CommandResult invokeCommand(String fullCommandName, Command<?> command, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        if (!started)
            throw new IllegalStateException("CommandInvokerService is not started");

        UUID executionId = UUID.randomUUID();
        CapturingOutput captured = new CapturingOutput();
        ExecutionHistory record = new ExecutionHistory(executionId, fullCommandName, Clock.Global.currentTimeMillis());
        CommandExecutionContext executionContext = new ServerCommandExecutionContext(new NodeProbe(accessor, captured.createOutput()));
        executionHistory.add(record);

        try
        {
            // TODO: CASSANDRA-XXXXX. Restrict command execution to environments without authentication
            //  This is a temporary limitation until full authentication support is implemented.
            if (DatabaseDescriptor.getAuthenticator().requireAuthentication())
            {
                throw new CommandAuthorizationException(format("Command execution '%s' via management port is currently " +
                                                               "only supported when authentication is disabled " +
                                                               "(AllowAllAuthenticator). Full authentication and authorization " +
                                                               "support will be added in a future release.", fullCommandName));
            }

            logger.info("Executing command '{}' with execution ID: {}", fullCommandName, executionId);

            CommandExecutionArgs arguments = argumentsSupplier.get();
            validateArguments(arguments, command.metadata());

            // Currently, for picocli-based commands in C*, which have no structured result,
            // the output is written to the Output in the context.
            Object ignore = command.execute(arguments, executionContext);

            record.completed(Clock.Global.currentTimeMillis());
            logger.info("Command '{}' (execution ID: {}) completed successfully", fullCommandName, executionId);
            return new CommandResult(executionId,
                                     captured.getCapturedOutput(),
                                     record.startTime,
                                     record.endTime - record.startTime);
        }
        catch (CommandAuthorizationException e)
        {
            record.failed(Clock.Global.currentTimeMillis(), e);
            logger.error("Command '{}' (execution ID: {}) authorization failed", fullCommandName, executionId, e);
            throw e;
        }
        catch (IllegalStateException | IllegalArgumentException | MarshalException e)
        {
            record.failed(Clock.Global.currentTimeMillis(), e);
            String msg = format("Bad usage for command '%s' (execution ID: %s)", fullCommandName, executionId);
            logger.error(msg, e);
            throw new CommandValidationException(msg, e);
        }
        catch (Exception e)
        {
            record.failed(Clock.Global.currentTimeMillis(), e);
            String msg = format("Command '%s' (execution ID: %s) execution failed", fullCommandName, executionId);
            logger.error(msg, e);
            throw new CommandExecutionException(msg, e, executionId);
        }
        catch (Throwable e)
        {
            record.failed(Clock.Global.currentTimeMillis(), e);
            logger.error("Command '{}' (execution ID: {}) unexpected error", fullCommandName, executionId, e);
            throw new CommandExecutionException(format("Unexpected error while executing '%s': %s",
                                                       fullCommandName, e.getMessage()),
                                                e,
                                                executionId);
        }
    }

    @Override
    public String[] getCommandNames()
    {
        List<String> commandNames = new ArrayList<>();
        collectCommandNamesRecursively(registry, "", commandNames);
        return commandNames.toArray(new String[0]);
    }

    @VisibleForTesting
    List<ExecutionHistory> executionHistory()
    {
        return executionHistory.snapshot();
    }

    /** Validate command arguments against metadata. */
    protected void validateArguments(CommandExecutionArgs arguments, CommandMetadata metadata)
    {
        for (OptionMetadata option : metadata.options())
        {
            if (option.required() && !arguments.hasOption(option))
                throw new IllegalArgumentException(String.format("Required option '%s' is missing", option.paramLabel()));
        }

        for (ParameterMetadata param : metadata.parameters())
        {
            if (param.required() && !arguments.hasParameter(param))
                throw new IllegalArgumentException(String.format("Required parameter at index %d ('%s') is missing",
                                                                 param.index(), param.paramLabel()));
        }
    }

    private void collectCommandNamesRecursively(CommandRegistry registry,
                                                String parentCommandName,
                                                List<String> result)
    {
        for (Map.Entry<String, Command<?>> entry : registry.commands())
        {
            String commandName = entry.getKey();
            Command<?> command = entry.getValue();

            String fullCommandName = fullCommandName(parentCommandName, commandName);
            if (command instanceof CommandRegistry)
                collectCommandNamesRecursively((CommandRegistry) command, fullCommandName, result);
            else
                result.add(fullCommandName);
        }
    }

    @Override
    public int getCommandCount()
    {
        return countCommands(registry);
    }

    @Override
    public String getCommandMBeanName(String fullCommandName)
    {
        ObjectName objectName = commandMBeanNames.get(fullCommandName);
        if (objectName == null)
            throw new IllegalArgumentException("Command MBean name not found: " + fullCommandName);
        return objectName.toString();
    }

    public boolean isStarted()
    {
        return started;
    }

    private void registerCommandMBeansRecursively(CommandRegistry registry, String parentCommandName)
    {
        for (Map.Entry<String, Command<?>> e : registry.commands())
        {
            String commandName = e.getKey();
            Command<?> command = e.getValue();
            String fullCommandName = fullCommandName(parentCommandName, commandName);

            if (command instanceof CommandRegistry)
            {
                registerCommandMBeansRecursively((CommandRegistry) command, fullCommandName);
            }
            else
            {
                try
                {
                    String escapedName = ObjectName.quote(fullCommandName);
                    ObjectName objectName = new ObjectName(format("%s:type=%s,name=%s",
                                                                  MBEAN_DOMAIN, MBEAN_TYPE_COMMAND, escapedName));
                    CommandMBeanAdapter commandMBean = new CommandMBeanAdapter(fullCommandName, command, this::invokeCommand);
                    MBeanWrapper.instance.registerMBean(commandMBean, objectName, MBeanWrapper.OnException.LOG);

                    ObjectName prev = commandMBeanNames.putIfAbsent(fullCommandName, objectName);
                    if (prev != null)
                    {
                        throw new IllegalStateException("Command name conflict during MBean registration: '" +
                                                        fullCommandName + "' is already registered");
                    }
                    logger.debug("Registered command MBean: {} -> {}", fullCommandName, objectName);
                }
                catch (Exception ex)
                {
                    logger.warn("Failed to register MBean for command: {}", fullCommandName, ex);
                }
            }
        }
    }

    private void unregisterCommandMBeans()
    {
        for (ObjectName objectName : commandMBeanNames.values())
            MBeanWrapper.instance.unregisterMBean(objectName, MBeanWrapper.OnException.LOG);
        commandMBeanNames.clear();
    }

    @VisibleForTesting
    static class CapturingOutput
    {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final PrintStream output = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        private final PrintStream error = new PrintStream(buffer, true, StandardCharsets.UTF_8);

        public String getCapturedOutput()
        {
            output.flush();
            error.flush();
            return buffer.toString(StandardCharsets.UTF_8);
        }

        Output createOutput()
        {
            return new Output(output, error);
        }
    }

    private static class ServerCommandExecutionContext implements CommandExecutionContext
    {
        private final Map<Class<?>, Object> services = new HashMap<>();

        public ServerCommandExecutionContext(NodeProbe probe)
        {
            services.put(NodeProbe.class, probe);
            services.put(Output.class, probe.output());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T service(Class<T> serviceType)
        {
            T svc = (T) services.get(serviceType);
            if (svc == null)
                throw new IllegalArgumentException("Service not available in this context: " + serviceType.getName());
            return svc;
        }

        @Override
        public Set<Class<?>> services()
        {
            return services.keySet();
        }
    }

    public static class CommandResult
    {
        private final UUID executionId;
        private final String output;
        private final long startTime;
        private final long durationMillis;

        public CommandResult(UUID executionId, String output, long startTime, long durationMillis)
        {
            this.executionId = executionId;
            this.output = output;
            this.startTime = startTime;
            this.durationMillis = durationMillis;
        }

        public UUID getExecutionId()
        {
            return executionId;
        }

        public String getOutput()
        {
            return output;
        }

        public long getStartTime()
        {
            return startTime;
        }

        public long getDurationMillis()
        {
            return durationMillis;
        }
    }

    /**
     * Record of a single command execution. Retained in {@link BoundedExecutionHistory} and is
     * surfaced to operators as command execution history via virtual tables.
     */
    static class ExecutionHistory
    {
        final UUID executionId;
        final String commandName;
        final long startTime;
        volatile long endTime;
        volatile boolean success;
        volatile Throwable error;

        ExecutionHistory(UUID executionId, String commandName, long startTime)
        {
            this.executionId = executionId;
            this.commandName = commandName;
            this.startTime = startTime;
        }

        void completed(long endTime)
        {
            this.endTime = endTime;
            this.success = true;
        }

        void failed(long endTime, Throwable error)
        {
            this.endTime = endTime;
            this.success = false;
            this.error = error;
        }

        public UUID executionId() { return executionId; }
        public String commandName() { return commandName; }
        public boolean isSuccess() { return success; }
        public Throwable error() { return error; }
    }

    private static class BoundedExecutionHistory
    {
        private final Deque<ExecutionHistory> dq = new ConcurrentLinkedDeque<>();
        private final AtomicInteger size = new AtomicInteger(0);
        private final int maxSize;

        public BoundedExecutionHistory(int maxSize)
        {
            this.maxSize = maxSize;
        }

        public void add(ExecutionHistory info)
        {
            dq.offer(info);

            if (size.incrementAndGet() > maxSize)
            {
                ExecutionHistory removed = dq.pollFirst();
                if (removed != null)
                    size.decrementAndGet();
            }
        }

        /** Returns a point-in-time snapshot of the retained records, oldest first. */
        public List<ExecutionHistory> snapshot()
        {
            return new ArrayList<>(dq);
        }
    }

    @FunctionalInterface
    public interface Executor
    {
        CommandResult execute(String commandName, Supplier<CommandExecutionArgs> argsSupplier)
            throws CommandExecutionException, CommandValidationException, CommandAuthorizationException;
    }
}
