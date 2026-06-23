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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class CommandServiceTest extends CQLTester
{
    private static final String COMMAND_MBEAN_PATTERN = "org.apache.cassandra.management:type=Command,name=*";

    @Test
    public void testCommandServiceMBeanRegistered()
    {
        try
        {
            ObjectName serviceName = new ObjectName(CommandInvokerServiceMBean.MBEAN_NAME);
            assertThat(MBeanWrapper.instance.isRegistered(serviceName)).as("CommandInvokerService MBean should be registered after start()").isTrue();
        }
        catch (Exception e)
        {
            throw new AssertionError("Failed to check MBean registration", e);
        }
    }

    @Test
    public void testCommandMBeansRegistered()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        try
        {
            ObjectName pattern = new ObjectName(COMMAND_MBEAN_PATTERN);
            Set<ObjectName> commandMBeans = MBeanWrapper.instance.queryNames(pattern, null);

            assertThat(commandMBeans).as("At least one Command MBean should be registered").isNotEmpty();

            String[] commandNames = service.getCommandNames();
            assertThat(commandNames).as("Service should return command names").isNotEmpty();

            for (String commandName : commandNames)
            {
                String mbeanName = service.getCommandMBeanName(commandName);
                assertNotNull("MBean name should not be null for command: " + commandName, mbeanName);

                ObjectName objectName = new ObjectName(mbeanName);
                assertThat(MBeanWrapper.instance.isRegistered(objectName)).as("Command MBean should be registered for: " + commandName).isTrue();
            }
        }
        catch (Exception e)
        {
            throw new AssertionError("Failed to verify Command MBeans", e);
        }
    }

    @Test
    public void testUnsupportedCommandsNotRegistered()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        String[] commandNames = service.getCommandNames();

        for (String unsupported : CassandraCommandRegistry.UNSUPPORTED_COMMANDS)
        {
            Command<?> cmd = service.getRegistry().command(unsupported);
            assertThat(cmd).as("Unsupported command '%s' should not be in the registry", unsupported).isNull();

            assertThat(Arrays.asList(commandNames))
                .as("Unsupported command '%s' should not appear in getCommandNames()", unsupported)
                .noneMatch(name -> name.equals(unsupported) || name.startsWith(unsupported + "."));
        }
    }

    @Test
    public void testCommandMBeanInvoke()
    {
        CommandInvokerService service = CommandInvokerService.instance;

        try
        {
            for (String testCommand : service.getCommandNames())
            {
                String mbeanName = service.getCommandMBeanName(testCommand);
                ObjectName commandObjectName = new ObjectName(mbeanName);
                assertThat(MBeanWrapper.instance.isRegistered(commandObjectName)).as("Command MBean should be registered").isTrue();

                MBeanServer mbs = MBeanWrapper.instance.getMBeanServer();
                String schema = (String) mbs.invoke(commandObjectName, "getJsonSchema", null, null);
                assertThat(schema).as("getJsonSchema() should return non-null JSON string").isNotNull().isNotEmpty();

                assertThat(schema.trim()).as("Schema should start with '{'").startsWith("{");
            }
        }
        catch (Exception e)
        {
            throw new AssertionError("Failed to test CommandMBeanAdapter invoke", e);
        }
    }

    /**
     * A successful execution is recorded in the bounded execution history as a success.
     */
    @Test
    public void testSuccessfulExecutionRecordedInHistory() throws Exception
    {
        CommandInvokerService service = CommandInvokerService.instance;
        UUID executionId = service.invokeCommand("version", CommandServiceTest::emptyArgs).getExecutionId();

        CommandInvokerService.ExecutionHistory record = findInHistory(service, executionId);
        assertThat(record).as("Successful 'version' execution should be recorded in history").isNotNull();
        assertThat(record.isSuccess()).as("Record should be marked successful").isTrue();
        assertThat(record.commandName()).isEqualTo("version");
    }

    @Test
    public void testValidationFailureRecordedInHistory()
    {
        CommandInvokerService service = CommandInvokerService.instance;

        int before = service.executionHistory().size();
        assertThatThrownBy(() -> service.invokeCommand("getauthcacheconfig", CommandServiceTest::emptyArgs))
            .isInstanceOf(CommandValidationException.class);

        List<CommandInvokerService.ExecutionHistory> history = service.executionHistory();
        assertThat(history.size()).as("Failed execution should still be recorded").isGreaterThan(before);

        CommandInvokerService.ExecutionHistory last = history.get(history.size() - 1);
        assertThat(last.commandName()).isEqualTo("getauthcacheconfig");
        assertThat(last.isSuccess()).as("Validation failure should be recorded as not successful").isFalse();
        assertThat(last.error()).as("Failure record should retain the error").isNotNull();
    }

    @Test
    public void testMalformedJsonMappedToValidationException()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        CommandMetadata metadata = service.getRegistry().command("version").metadata();

        assertThatThrownBy(() -> service.invokeCommand("version", () -> CommandExecutionArgsSerde.fromJson("{not valid json", metadata)))
            .isInstanceOf(CommandValidationException.class)
            .hasMessageContaining("Bad usage");
    }

    @Test
    public void testMalformedJsonReportedAsValidationErrorViaMBean()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        Command<?> command = service.getRegistry().command("version");
        CommandMBeanAdapter adapter = new CommandMBeanAdapter("version", command, service::invokeCommand);

        assertThatThrownBy(() -> adapter.invoke(CommandMBeanAdapter.INVOKE_METHOD,
                                                new Object[]{ "{not valid json" },
                                                new String[]{ String.class.getName() }))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Error decoding JSON string");
    }

    private static CommandExecutionArgs emptyArgs()
    {
        return new SimpleCommandExecutionArgs(Collections.emptyMap(), Collections.emptyMap());
    }

    private static CommandInvokerService.ExecutionHistory findInHistory(CommandInvokerService service, UUID executionId)
    {
        return service.executionHistory().stream()
                      .filter(r -> executionId.equals(r.executionId()))
                      .reduce((first, second) -> second) // last match
                      .orElse(null);
    }
}
