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

package org.apache.cassandra.security;

import java.io.File;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.StandardSystemProperty;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.logging.VirtualTableAppender;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Runs {@link UDFSecurityMechanism#install()} in a separate JVM per configuration, since the mechanism is resolved
 * once per JVM and a Security Manager cannot be removed.
 */
public class UDFSecurityMechanismStartupTest
{
    private static final int FEATURE = Runtime.version().feature();

    @Test
    public void testStartup() throws Exception
    {
        startup("bytecode", false, null);
        startup("auto", FEATURE < 24, null);
        startup("securitymanager", true, FEATURE >= 24 ? "is not supported on Java " + FEATURE : null);
        if (FEATURE >= 12)
            startup("securitymanager", true, FEATURE >= 24 ? "is not supported on Java" : "Could not install the Security Manager",
                    "-Djava.security.manager=disallow");
    }

    @Test
    public void testLoggingStartupChecksRunWithoutSecurityManager() throws Exception
    {
        for (String mechanism : new String[]{ "bytecode", "auto" })
            startup(mechanism, LoggingStartup.class, Collections.emptyList());
    }

    private static void startup(String mechanism, boolean installed, String failure, String... options) throws Exception
    {
        startup(mechanism, Startup.class, Arrays.asList(Boolean.toString(installed), failure == null ? "" : failure), options);
    }

    private static void startup(String mechanism, Class<?> mainClass, List<String> args, String... options) throws Exception
    {
        List<String> command = new ArrayList<>();
        command.add(StandardSystemProperty.JAVA_HOME.value() + File.separator + "bin" + File.separator + "java");
        if (FEATURE >= 17 && FEATURE < 24)
            command.add("-Djava.security.manager=allow");
        command.addAll(Arrays.asList(options));
        command.add("-Dcassandra.udf.security_mechanism=" + mechanism);
        command.add("-cp");
        command.add(StandardSystemProperty.JAVA_CLASS_PATH.value());
        command.add(mainClass.getName());
        command.addAll(args);
        try (ToolRunner.ObservableTool tool = ToolRunner.invokeAsync(command.toArray(new String[0])))
        {
            await().atMost(60, TimeUnit.SECONDS).until(tool::isDone);
            ToolRunner.ToolResult result = tool.waitComplete();
            assertEquals(mechanism + ' ' + args + ' ' + Arrays.toString(options) + ":\n" + result.getStdout() + result.getStderr(),
                         0, result.getExitCode());
        }
    }

    /** args: whether a Security Manager must be installed, and the expected ConfigurationException message or "". */
    public static class Startup
    {
        @SuppressWarnings("removal")
        public static void main(String[] args)
        {
            DatabaseDescriptor.clientInitialization();
            boolean installed = Boolean.parseBoolean(args[0]);
            String failure = args.length > 1 ? args[1] : "";
            Policy policy = FEATURE < 24 ? Policy.getPolicy() : null;
            try
            {
                UDFSecurityMechanism.install();
                if (!failure.isEmpty())
                    throw new AssertionError("Expected a configuration failure: " + failure);
                if ((System.getSecurityManager() != null) != installed)
                    throw new AssertionError("Unexpected installed security manager: " + System.getSecurityManager());
                if (!installed && FEATURE < 24 && policy != Policy.getPolicy())
                    throw new AssertionError("The bytecode sandbox changed the security policy");
            }
            catch (ConfigurationException e)
            {
                if (failure.isEmpty() || !e.getMessage().contains(failure))
                    throw e;
            }
        }
    }

    /** Logging startup checks (duplicate appenders) must still run when no Security Manager is installed. */
    public static class LoggingStartup
    {
        @SuppressWarnings("unchecked")
        public static void main(String[] args)
        {
            DatabaseDescriptor.clientInitialization();
            Appender<?> first = new VirtualTableAppender();
            Appender<?> second = new VirtualTableAppender();
            first.setName("first");
            second.setName("second");
            Logger logger = (Logger) LoggerFactory.getLogger(LoggingStartup.class);
            // Logback supplies LoggingEvent instances to these appenders
            logger.addAppender((Appender<ILoggingEvent>) first);
            logger.addAppender((Appender<ILoggingEvent>) second);
            try
            {
                UDFSecurityMechanism.install();
                throw new AssertionError("Expected duplicate appender failure");
            }
            catch (IllegalStateException e)
            {
                if (!e.getMessage().contains("multiple appenders of class " + first.getClass().getName()))
                    throw e;
            }
        }
    }
}
