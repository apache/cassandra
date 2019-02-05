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

package org.apache.cassandra.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Verifies that {@link DatabaseDescriptor#clientInitialization()} } and a couple of <i>apply</i> methods
 * do not somehow lazily initialize any unwanted part of Cassandra like schema, commit log or start
 * unexpected threads.
 *
 * {@link DatabaseDescriptor#toolInitialization()} is tested via unit tests extending
 * {@link org.apache.cassandra.tools.ToolsTester}.
 */
public class DatabaseDescriptorRefTest
{
    static final String[] validClasses = {
    "org.apache.cassandra.auth.IInternodeAuthenticator",
    "org.apache.cassandra.auth.IAuthenticator",
    "org.apache.cassandra.auth.IAuthorizer",
    "org.apache.cassandra.auth.IRoleManager",
    "org.apache.cassandra.config.DatabaseDescriptor",
    "org.apache.cassandra.config.ConfigurationLoader",
    "org.apache.cassandra.config.Config",
    "org.apache.cassandra.config.Config$1",
    "org.apache.cassandra.config.Config$RequestSchedulerId",
    "org.apache.cassandra.config.Config$CommitLogSync",
    "org.apache.cassandra.config.Config$DiskAccessMode",
    "org.apache.cassandra.config.Config$DiskFailurePolicy",
    "org.apache.cassandra.config.Config$CommitFailurePolicy",
    "org.apache.cassandra.config.Config$DiskOptimizationStrategy",
    "org.apache.cassandra.config.Config$InternodeCompression",
    "org.apache.cassandra.config.Config$MemtableAllocationType",
    "org.apache.cassandra.config.Config$UserFunctionTimeoutPolicy",
    "org.apache.cassandra.config.RequestSchedulerOptions",
    "org.apache.cassandra.config.ParameterizedClass",
    "org.apache.cassandra.config.EncryptionOptions",
    "org.apache.cassandra.config.EncryptionOptions$ClientEncryptionOptions",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptions",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptions$InternodeEncryption",
    "org.apache.cassandra.config.YamlConfigurationLoader",
    "org.apache.cassandra.config.YamlConfigurationLoader$PropertiesChecker",
    "org.apache.cassandra.config.YamlConfigurationLoader$PropertiesChecker$1",
    "org.apache.cassandra.config.YamlConfigurationLoader$CustomConstructor",
    "org.apache.cassandra.config.TransparentDataEncryptionOptions",
    "org.apache.cassandra.dht.IPartitioner",
    "org.apache.cassandra.distributed.impl.InstanceClassLoader",
    "org.apache.cassandra.distributed.impl.InstanceConfig",
    "org.apache.cassandra.distributed.impl.InvokableInstance",
    "org.apache.cassandra.distributed.impl.InvokableInstance$CallableNoExcept",
    "org.apache.cassandra.distributed.impl.InvokableInstance$InstanceFunction",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableBiConsumer",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableBiFunction",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableCallable",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableConsumer",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableFunction",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableRunnable",
    "org.apache.cassandra.distributed.impl.InvokableInstance$SerializableTriFunction",
    "org.apache.cassandra.distributed.impl.InvokableInstance$TriFunction",
    "org.apache.cassandra.distributed.impl.Message",
    "org.apache.cassandra.exceptions.ConfigurationException",
    "org.apache.cassandra.exceptions.RequestValidationException",
    "org.apache.cassandra.exceptions.CassandraException",
    "org.apache.cassandra.exceptions.TransportException",
    "org.apache.cassandra.locator.IEndpointSnitch",
    "org.apache.cassandra.io.FSWriteError",
    "org.apache.cassandra.io.FSError",
    "org.apache.cassandra.io.compress.ICompressor",
    "org.apache.cassandra.io.compress.LZ4Compressor",
    "org.apache.cassandra.io.sstable.metadata.MetadataType",
    "org.apache.cassandra.io.util.BufferedDataOutputStreamPlus",
    "org.apache.cassandra.io.util.DataOutputBuffer",
    "org.apache.cassandra.io.util.DataOutputBufferFixed",
    "org.apache.cassandra.io.util.DataOutputStreamPlus",
    "org.apache.cassandra.io.util.DataOutputPlus",
    "org.apache.cassandra.io.util.DiskOptimizationStrategy",
    "org.apache.cassandra.io.util.SpinningDiskOptimizationStrategy",
    "org.apache.cassandra.locator.SimpleSeedProvider",
    "org.apache.cassandra.locator.SeedProvider",
    "org.apache.cassandra.net.BackPressureStrategy",
    "org.apache.cassandra.scheduler.IRequestScheduler",
    "org.apache.cassandra.security.EncryptionContext",
    "org.apache.cassandra.service.CacheService$CacheType",
    "org.apache.cassandra.utils.FBUtilities",
    "org.apache.cassandra.utils.FBUtilities$1",
    "org.apache.cassandra.utils.CloseableIterator",
    "org.apache.cassandra.utils.Pair",
    "org.apache.cassandra.OffsetAwareConfigurationLoader",
    "org.apache.cassandra.ConsoleAppender",
    "org.apache.cassandra.ConsoleAppender$1",
    "org.apache.cassandra.LogbackStatusListener",
    "org.apache.cassandra.LogbackStatusListener$ToLoggerOutputStream",
    "org.apache.cassandra.LogbackStatusListener$WrappedPrintStream",
    "org.apache.cassandra.TeeingAppender",
    // generated classes
    "org.apache.cassandra.config.ConfigBeanInfo",
    "org.apache.cassandra.config.ConfigCustomizer",
    "org.apache.cassandra.config.EncryptionOptionsBeanInfo",
    "org.apache.cassandra.config.EncryptionOptionsCustomizer",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptionsBeanInfo",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptionsCustomizer",
    "org.apache.cassandra.ConsoleAppenderBeanInfo",
    "org.apache.cassandra.ConsoleAppenderCustomizer",
    };

    static final Set<String> checkedClasses = new HashSet<>(Arrays.asList(validClasses));

    @Test
    public void testDatabaseDescriptorRef() throws Throwable
    {
        PrintStream out = System.out;
        PrintStream err = System.err;

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        int threadCount = threads.getThreadCount();

        ClassLoader delegate = Thread.currentThread().getContextClassLoader();

        List<Pair<String, Exception>> violations = Collections.synchronizedList(new ArrayList<>());

        ClassLoader cl = new ClassLoader(null)
        {
            final Map<String, Class<?>> classMap = new HashMap<>();

            public URL getResource(String name)
            {
                return delegate.getResource(name);
            }

            public InputStream getResourceAsStream(String name)
            {
                return delegate.getResourceAsStream(name);
            }

            protected Class<?> findClass(String name) throws ClassNotFoundException
            {
                Class<?> cls = classMap.get(name);
                if (cls != null)
                    return cls;

                if (name.startsWith("org.apache.cassandra."))
                {
                    // out.println(name);

                    if (!checkedClasses.contains(name))
                        violations.add(Pair.create(name, new Exception()));
                }

                URL url = delegate.getResource(name.replace('.', '/') + ".class");
                if (url == null)
                    throw new ClassNotFoundException(name);
                try (InputStream in = url.openConnection().getInputStream())
                {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    int c;
                    while ((c = in.read()) != -1)
                        os.write(c);
                    byte[] data = os.toByteArray();
                    cls = defineClass(name, data, 0, data.length);
                    classMap.put(name, cls);
                    return cls;
                }
                catch (IOException e)
                {
                    throw new ClassNotFoundException(name, e);
                }
            }
        };

        Thread.currentThread().setContextClassLoader(cl);

        assertEquals("thread started", threadCount, threads.getThreadCount());

        Class cDatabaseDescriptor = Class.forName("org.apache.cassandra.config.DatabaseDescriptor", true, cl);

        for (String methodName : new String[]{
            "clientInitialization",
            "applyAddressConfig",
            "applyThriftHSHA",
            "applyInitialTokens",
            // no seed provider in default configuration for clients
            // "applySeedProvider",
            // definitely not safe for clients - implicitly instantiates schema
            // "applyPartitioner",
            // definitely not safe for clients - implicitly instantiates StorageService
            // "applySnitch",
            "applyEncryptionContext",
            // starts "REQUEST-SCHEDULER" thread via RoundRobinScheduler
            // "applyRequestScheduler",
        })
        {
            Method method = cDatabaseDescriptor.getDeclaredMethod(methodName);
            method.invoke(null);

            if ("clientInitialization".equals(methodName) &&
                threadCount + 1 == threads.getThreadCount())
            {
                // ignore the "AsyncAppender-Worker-ASYNC" thread
                threadCount++;
            }

            if (threadCount != threads.getThreadCount())
            {
                for (ThreadInfo threadInfo : threads.getThreadInfo(threads.getAllThreadIds()))
                    out.println("Thread #" + threadInfo.getThreadId() + ": " + threadInfo.getThreadName());
                assertEquals("thread started in " + methodName, threadCount, ManagementFactory.getThreadMXBean().getThreadCount());
            }

            checkViolations(err, violations);
        }
    }

    private void checkViolations(PrintStream err, List<Pair<String, Exception>> violations)
    {
        if (!violations.isEmpty())
        {
            for (Pair<String, Exception> violation : new ArrayList<>(violations))
            {
                err.println();
                err.println();
                err.println("VIOLATION: " + violation.left);
                violation.right.printStackTrace(err);
            }

            fail();
        }
    }
}
