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
import java.nio.charset.StandardCharsets;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.StandardSystemProperty;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.logging.SlowQueriesAppender;
import org.apache.cassandra.utils.logging.VirtualTableAppender;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ThreadAwareSecurityManagerTest
{
    @Test
    public void selection()
    {
        for (int version : new int[]{ 11, 17, 21, 23, 24, 25 })
        {
            assertEquals(version < 24, ThreadAwareSecurityManager.useSecurityManager("auto", version));
            assertTrue(ThreadAwareSecurityManager.useSecurityManager("securitymanager", version));
            assertFalse(ThreadAwareSecurityManager.useSecurityManager("sandbox", version));
        }
        assertFalse(ThreadAwareSecurityManager.useSecurityManager(" SANDBOX ", 11));
        for (String invalid : new String[]{ "", "false", "disabled", "security-manager" })
        {
            try
            {
                ThreadAwareSecurityManager.useSecurityManager(invalid, 11);
                fail("Accepted " + invalid);
            }
            catch (ConfigurationException e)
            {
                assertTrue(e.getMessage().contains("cassandra.udf.security_mechanism"));
            }
        }
    }

    @Test
    public void startup() throws Exception
    {
        startup("sandbox", false, false);
        startup("auto", Runtime.version().feature() < 24, false);
        startup("securitymanager", true, Runtime.version().feature() >= 24);
        startup("invalid", false, true);
        if (Runtime.version().feature() >= 12)
            startup("securitymanager", true, true, "-Djava.security.manager=disallow");
    }

    private static void startup(String mechanism, boolean installed, boolean failure, String... options) throws Exception
    {
        startup(mechanism, Startup.class, Arrays.asList(Boolean.toString(installed), Boolean.toString(failure)), options);
    }

    @Test
    public void loggingStartup() throws Exception
    {
        for (String mechanism : new String[]{ "sandbox", "auto" })
        {
            startup(mechanism, LoggingStartup.class, Arrays.asList("virtual"));
            startup(mechanism, LoggingStartup.class, Arrays.asList("slow"));
        }
    }

    private static void startup(String mechanism, Class<?> mainClass, List<String> args, String... options) throws Exception
    {
        List<String> command = new ArrayList<>();
        command.add(StandardSystemProperty.JAVA_HOME.value() + File.separator + "bin" + File.separator + "java");
        if (Runtime.version().feature() >= 17 && Runtime.version().feature() < 24)
            command.add("-Djava.security.manager=allow");
        command.addAll(Arrays.asList(options));
        command.add("-Dcassandra.udf.security_mechanism=" + mechanism);
        command.add("-cp");
        command.add(StandardSystemProperty.JAVA_CLASS_PATH.value());
        command.add(mainClass.getName());
        command.addAll(args);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try
        {
            assertTrue("Startup did not finish", process.waitFor(30, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(output, 0, process.exitValue());
        }
        finally
        {
            process.destroyForcibly();
        }
    }

    public static class LoggingStartup
    {
        @SuppressWarnings("unchecked")
        public static void main(String[] args)
        {
            DatabaseDescriptor.clientInitialization();
            Supplier<Appender<?>> factory = args[0].equals("virtual") ? VirtualTableAppender::new : SlowQueriesAppender::new;
            Appender<?> first = factory.get();
            Appender<?> second = factory.get();
            first.setName("first");
            second.setName("second");
            Logger logger = (Logger) LoggerFactory.getLogger(LoggingStartup.class);
            // Logback supplies LoggingEvent instances to these appenders.
            logger.addAppender((Appender<ILoggingEvent>) first);
            logger.addAppender((Appender<ILoggingEvent>) second);
            try
            {
                ThreadAwareSecurityManager.install();
                throw new AssertionError("Expected duplicate appender failure");
            }
            catch (IllegalStateException e)
            {
                if (!e.getMessage().contains("multiple appenders of class " + first.getClass().getName()))
                    throw e;
            }
        }
    }

    public static class Startup
    {
        public static void main(String[] args)
        {
            DatabaseDescriptor.clientInitialization();
            Policy policy = Runtime.version().feature() < 24 ? Policy.getPolicy() : null;
            ThreadAwareSecurityManager.isSecuredThread();
            boolean failure = Boolean.parseBoolean(args[1]);
            try
            {
                ThreadAwareSecurityManager.install();
                if (failure)
                    throw new AssertionError("Expected configuration failure");
                if ((System.getSecurityManager() != null) != Boolean.parseBoolean(args[0]))
                    throw new AssertionError("Unexpected installed security manager");
                if (!Boolean.parseBoolean(args[0]) && Runtime.version().feature() < 24 && policy != Policy.getPolicy())
                    throw new AssertionError("Sandbox changed the security policy");
            }
            catch (ConfigurationException e)
            {
                if (!failure)
                    throw e;
            }
        }
    }
}
