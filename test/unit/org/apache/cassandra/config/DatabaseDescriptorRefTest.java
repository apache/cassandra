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
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Verifies that {@link DatabaseDescriptor#clientInitialization()} and a couple of <i>apply</i> methods
 * do not somehow lazily initialize any unwanted part of Cassandra like schema, commit log or start
 * unexpected threads.
 *
 * {@link DatabaseDescriptor#toolInitialization()} is tested via unit tests extending
 * {@link org.apache.cassandra.tools.OfflineToolUtils}.
 */
public class DatabaseDescriptorRefTest
{
    static final String[] validClasses = {
    "org.apache.cassandra.ConsoleAppender",
    "org.apache.cassandra.ConsoleAppender$1",
    "org.apache.cassandra.ConsoleAppenderBeanInfo",
    "org.apache.cassandra.ConsoleAppenderCustomizer",
    "org.apache.cassandra.LogbackStatusListener",
    "org.apache.cassandra.LogbackStatusListener$ToLoggerOutputStream",
    "org.apache.cassandra.LogbackStatusListener$WrappedPrintStream",
    "org.apache.cassandra.TeeingAppender",
    "org.apache.cassandra.audit.AuditLogOptions",
    "org.apache.cassandra.audit.BinAuditLogger",
    "org.apache.cassandra.audit.BinLogAuditLogger",
    "org.apache.cassandra.audit.IAuditLogger",
    "org.apache.cassandra.auth.AllowAllInternodeAuthenticator",
    "org.apache.cassandra.auth.AuthCache$BulkLoader",
    "org.apache.cassandra.auth.Cacheable",
    "org.apache.cassandra.auth.IAuthenticator",
    "org.apache.cassandra.auth.IAuthorizer",
    "org.apache.cassandra.auth.IInternodeAuthenticator",
    "org.apache.cassandra.auth.ICIDRAuthorizer",
    "org.apache.cassandra.auth.ICIDRAuthorizer$CIDRAuthorizerMode",
    "org.apache.cassandra.auth.INetworkAuthorizer",
    "org.apache.cassandra.auth.IRoleManager",
    "org.apache.cassandra.config.CassandraRelevantProperties",
    "org.apache.cassandra.config.CassandraRelevantProperties$PropertyConverter",
    "org.apache.cassandra.config.Config",
    "org.apache.cassandra.config.Config$1",
    "org.apache.cassandra.config.Config$CommitFailurePolicy",
    "org.apache.cassandra.config.Config$CommitLogSync",
    "org.apache.cassandra.config.Config$CorruptedTombstoneStrategy",
    "org.apache.cassandra.config.Config$DiskAccessMode",
    "org.apache.cassandra.config.Config$DiskFailurePolicy",
    "org.apache.cassandra.config.Config$DiskOptimizationStrategy",
    "org.apache.cassandra.config.Config$FlushCompression",
    "org.apache.cassandra.config.Config$InternodeCompression",
    "org.apache.cassandra.config.Config$MemtableAllocationType",
    "org.apache.cassandra.config.Config$PaxosOnLinearizabilityViolation",
    "org.apache.cassandra.config.Config$PaxosStatePurging",
    "org.apache.cassandra.config.Config$PaxosVariant",
    "org.apache.cassandra.config.Config$RepairCommandPoolFullStrategy",
    "org.apache.cassandra.config.Config$SSTableConfig",
    "org.apache.cassandra.config.Config$UserFunctionTimeoutPolicy",
    "org.apache.cassandra.config.ConfigBeanInfo",
    "org.apache.cassandra.config.ConfigCustomizer",
    "org.apache.cassandra.config.ConfigurationLoader",
    "org.apache.cassandra.config.DataRateSpec",
    "org.apache.cassandra.config.DataRateSpec$DataRateUnit",
    "org.apache.cassandra.config.DataRateSpec$DataRateUnit$1",
    "org.apache.cassandra.config.DataRateSpec$DataRateUnit$2",
    "org.apache.cassandra.config.DataRateSpec$DataRateUnit$3",
    "org.apache.cassandra.config.DataRateSpec$LongBytesPerSecondBound",
    "org.apache.cassandra.config.DataStorageSpec",
    "org.apache.cassandra.config.DataStorageSpec$DataStorageUnit",
    "org.apache.cassandra.config.DataStorageSpec$DataStorageUnit$1",
    "org.apache.cassandra.config.DataStorageSpec$DataStorageUnit$2",
    "org.apache.cassandra.config.DataStorageSpec$DataStorageUnit$3",
    "org.apache.cassandra.config.DataStorageSpec$DataStorageUnit$4",
    "org.apache.cassandra.config.DataStorageSpec$IntBytesBound",
    "org.apache.cassandra.config.DataStorageSpec$IntKibibytesBound",
    "org.apache.cassandra.config.DataStorageSpec$IntMebibytesBound",
    "org.apache.cassandra.config.DataStorageSpec$LongBytesBound",
    "org.apache.cassandra.config.DataStorageSpec$LongMebibytesBound",
    "org.apache.cassandra.config.DatabaseDescriptor",
    "org.apache.cassandra.config.DatabaseDescriptor$ByteUnit",
    "org.apache.cassandra.config.DurationSpec",
    "org.apache.cassandra.config.DurationSpec$IntMillisecondsBound",
    "org.apache.cassandra.config.DurationSpec$IntMinutesBound",
    "org.apache.cassandra.config.DurationSpec$IntSecondsBound",
    "org.apache.cassandra.config.DurationSpec$LongMillisecondsBound",
    "org.apache.cassandra.config.DurationSpec$LongMicrosecondsBound",
    "org.apache.cassandra.config.DurationSpec$LongNanosecondsBound",
    "org.apache.cassandra.config.DurationSpec$LongSecondsBound",
    "org.apache.cassandra.config.EncryptionOptions",
    "org.apache.cassandra.config.EncryptionOptions$ClientEncryptionOptions",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptions",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptions$InternodeEncryption",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptions$OutgoingEncryptedPortSource",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptionsBeanInfo",
    "org.apache.cassandra.config.EncryptionOptions$ServerEncryptionOptionsCustomizer",
    "org.apache.cassandra.config.EncryptionOptionsBeanInfo",
    "org.apache.cassandra.config.EncryptionOptionsCustomizer",
    "org.apache.cassandra.config.GuardrailsOptions",
    "org.apache.cassandra.config.GuardrailsOptions$Config",
    "org.apache.cassandra.config.GuardrailsOptions$ConsistencyLevels",
    "org.apache.cassandra.config.GuardrailsOptions$TableProperties",
    "org.apache.cassandra.config.ParameterizedClass",
    "org.apache.cassandra.config.RepairConfig",
    "org.apache.cassandra.config.RepairRetrySpec",
    "org.apache.cassandra.config.RetrySpec",
    "org.apache.cassandra.config.RetrySpec$MaxAttempt",
    "org.apache.cassandra.config.RetrySpec$Type",
    "org.apache.cassandra.config.ReplicaFilteringProtectionOptions",
    "org.apache.cassandra.config.StartupChecksOptions",
    "org.apache.cassandra.config.SubnetGroups",
    "org.apache.cassandra.config.TrackWarnings",
    "org.apache.cassandra.config.TransparentDataEncryptionOptions",
    "org.apache.cassandra.config.YamlConfigurationLoader",
    "org.apache.cassandra.config.YamlConfigurationLoader$CustomConstructor",
    "org.apache.cassandra.config.YamlConfigurationLoader$PropertiesChecker",
    "org.apache.cassandra.config.YamlConfigurationLoader$PropertiesChecker$1",
    "org.apache.cassandra.config.YamlConfigurationLoader$CustomConstructor",
    "org.apache.cassandra.config.TransparentDataEncryptionOptions",
    "org.apache.cassandra.config.StartupChecksOptions",
    "org.apache.cassandra.config.SubnetGroups",
    "org.apache.cassandra.config.TrackWarnings",
    "org.apache.cassandra.config.StorageAttachedIndexOptions",
    "org.apache.cassandra.db.ConsistencyLevel",
    "org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager",
    "org.apache.cassandra.db.commitlog.CommitLog",
    "org.apache.cassandra.db.commitlog.CommitLogMBean",
    "org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC",
    "org.apache.cassandra.db.commitlog.CommitLogSegmentManagerFactory",
    "org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard",
    "org.apache.cassandra.db.commitlog.DefaultCommitLogSegmentMgrFactory",
    "org.apache.cassandra.db.guardrails.GuardrailsConfig",
    "org.apache.cassandra.db.guardrails.GuardrailsConfig$ConsistencyLevels",
    "org.apache.cassandra.db.guardrails.GuardrailsConfig$TableProperties",
    "org.apache.cassandra.db.guardrails.GuardrailsConfigMBean",
    "org.apache.cassandra.db.guardrails.Values$Config",
    "org.apache.cassandra.db.rows.UnfilteredSource",
    "org.apache.cassandra.dht.IPartitioner",
    "org.apache.cassandra.distributed.api.IInstance",
    "org.apache.cassandra.distributed.api.IInvokableInstance",
    "org.apache.cassandra.distributed.api.IIsolatedExecutor",
    "org.apache.cassandra.distributed.impl.InstanceConfig",
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
    "org.apache.cassandra.distributed.impl.NetworkTopology",
    "org.apache.cassandra.distributed.shared.InstanceClassLoader",
    "org.apache.cassandra.exceptions.CassandraException",
    "org.apache.cassandra.exceptions.ConfigurationException",
    "org.apache.cassandra.exceptions.InvalidRequestException",
    "org.apache.cassandra.exceptions.RequestValidationException",
    "org.apache.cassandra.exceptions.TransportException",
    "org.apache.cassandra.fql.FullQueryLogger",
    "org.apache.cassandra.fql.FullQueryLoggerOptions",
    "org.apache.cassandra.gms.IFailureDetector",
    "org.apache.cassandra.io.FSError",
    "org.apache.cassandra.io.FSWriteError",
    "org.apache.cassandra.io.compress.ICompressor",
    "org.apache.cassandra.io.compress.ICompressor$Uses",
    "org.apache.cassandra.io.compress.LZ4Compressor",
    "org.apache.cassandra.io.sstable.Component",
    "org.apache.cassandra.io.sstable.Component$Type",
    "org.apache.cassandra.io.sstable.IScrubber",
    "org.apache.cassandra.io.sstable.MetricsProviders",
    "org.apache.cassandra.io.sstable.SSTable",
    "org.apache.cassandra.io.sstable.SSTable$Builder",
    "org.apache.cassandra.io.sstable.format.AbstractSSTableFormat",
    "org.apache.cassandra.io.sstable.format.SSTableFormat",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$Components",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$Components$Types",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$Factory",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$KeyCacheValueSerializer",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$SSTableReaderFactory",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$SSTableWriterFactory",
    "org.apache.cassandra.io.sstable.format.SSTableFormat$Type",
    "org.apache.cassandra.io.sstable.format.SSTableReader",
    "org.apache.cassandra.io.sstable.format.SSTableReader$Builder",
    "org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder",
    "org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter",
    "org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter$Builder",
    "org.apache.cassandra.io.sstable.format.SortedTableReaderLoadingBuilder",
    "org.apache.cassandra.io.sstable.format.SSTableWriter",
    "org.apache.cassandra.io.sstable.format.SSTableWriter$Builder",
    "org.apache.cassandra.io.sstable.format.SortedTableWriter",
    "org.apache.cassandra.io.sstable.format.SortedTableWriter$Builder",
    "org.apache.cassandra.io.sstable.format.Version",
    "org.apache.cassandra.io.sstable.format.big.BigFormat",
    "org.apache.cassandra.io.sstable.format.big.BigFormat$BigFormatFactory",
    "org.apache.cassandra.io.sstable.format.big.BigFormat$BigTableReaderFactory",
    "org.apache.cassandra.io.sstable.format.big.BigFormat$BigTableWriterFactory",
    "org.apache.cassandra.io.sstable.format.big.BigFormat$BigVersion",
    "org.apache.cassandra.io.sstable.format.big.BigFormat$Components",
    "org.apache.cassandra.io.sstable.format.big.BigFormat$Components$Types",
    "org.apache.cassandra.io.sstable.format.big.BigSSTableReaderLoadingBuilder",
    "org.apache.cassandra.io.sstable.format.big.BigTableReader",
    "org.apache.cassandra.io.sstable.format.big.BigTableReader$Builder",
    "org.apache.cassandra.io.sstable.format.big.BigTableWriter",
    "org.apache.cassandra.io.sstable.format.big.BigTableWriter$Builder",
    "org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport",
    "org.apache.cassandra.io.sstable.keycache.KeyCacheSupport",
    "org.apache.cassandra.io.sstable.metadata.MetadataType",
    "org.apache.cassandra.io.util.BufferedDataOutputStreamPlus",
    "org.apache.cassandra.io.util.DataInputPlus",
    "org.apache.cassandra.io.util.DataInputPlus$DataInputStreamPlus",
    "org.apache.cassandra.io.util.DataOutputBuffer",
    "org.apache.cassandra.io.util.DataOutputBufferFixed",
    "org.apache.cassandra.io.util.DataOutputPlus",
    "org.apache.cassandra.io.util.DataOutputStreamPlus",
    "org.apache.cassandra.io.util.DiskOptimizationStrategy",
    "org.apache.cassandra.io.util.File",
    "org.apache.cassandra.io.util.FileInputStreamPlus",
    "org.apache.cassandra.io.util.FileOutputStreamPlus",
    "org.apache.cassandra.io.util.PathUtils$IOToLongFunction",
    "org.apache.cassandra.io.util.RebufferingInputStream",
    "org.apache.cassandra.io.util.SpinningDiskOptimizationStrategy",
    "org.apache.cassandra.locator.IEndpointSnitch",
    "org.apache.cassandra.locator.InetAddressAndPort",
    "org.apache.cassandra.locator.Replica",
    "org.apache.cassandra.locator.ReplicaCollection",
    "org.apache.cassandra.locator.SeedProvider",
    "org.apache.cassandra.locator.SimpleSeedProvider",
    "org.apache.cassandra.security.EncryptionContext",
    "org.apache.cassandra.security.ISslContextFactory",
    "org.apache.cassandra.security.SSLFactory",
    "org.apache.cassandra.service.CacheService$CacheType",
    "org.apache.cassandra.security.AbstractCryptoProvider",
    "org.apache.cassandra.transport.ProtocolException",
    "org.apache.cassandra.utils.Closeable",
    "org.apache.cassandra.utils.CloseableIterator",
    "org.apache.cassandra.utils.FBUtilities",
    "org.apache.cassandra.utils.FBUtilities$1",
    "org.apache.cassandra.utils.Pair",
    "org.apache.cassandra.utils.binlog.BinLogOptions",
    "org.apache.cassandra.utils.concurrent.RefCounted",
    "org.apache.cassandra.utils.concurrent.SelfRefCounted",
    "org.apache.cassandra.utils.concurrent.Transactional",
    "org.apache.cassandra.utils.concurrent.UncheckedInterruptedException",
    "org.apache.cassandra.utils.StorageCompatibilityMode"
    };

    static final Set<String> checkedClasses = new HashSet<>(Arrays.asList(validClasses));

    @Test
    @SuppressWarnings({"DynamicRegexReplaceableByCompiledPattern", "UseOfSystemOutOrSystemErr"})
    public void testDatabaseDescriptorRef() throws Throwable
    {
        PrintStream out = System.out;
        PrintStream err = System.err;

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        int threadCount = threads.getThreadCount();
        List<Long> existingThreadIDs = Arrays.stream(threads.getAllThreadIds()).boxed().collect(Collectors.toList());

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
                if (name.startsWith("java."))
                    // Java 11 does not allow a "custom" class loader (i.e. user code)
                    // to define classes in protected packages (like java, java.sql, etc).
                    // Therefore we have to delegate the call to the delegate class loader
                    // itself.
                    return delegate.loadClass(name);

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
                {
                    // For Java 11: system class files are not readable via getResource(), so
                    // try it this way
                    cls = Class.forName(name, false, delegate);
                    classMap.put(name, cls);
                    return cls;
                }

                // Java8 way + all non-system class files
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

        Class<?> databaseDescriptorClass = Class.forName("org.apache.cassandra.config.DatabaseDescriptor", true, cl);

        // During DatabaseDescriptor instantiation some threads are spawned. We need to take them into account in
        // threadCount variable, otherwise they will be considered as new threads spawned by methods below. There is a
        // trick: in case of multiple runs of this test in the same JVM the number of such threads will be multiplied by
        // the number of runs. That's because DatabaseDescriptor is instantiated via a different class loader. So in
        // order to keep calculation logic correct, we ignore existing threads that were spawned during the previous
        // runs and change threadCount variable for the new threads only (if they have some specific names).
        for (ThreadInfo threadInfo : threads.getThreadInfo(threads.getAllThreadIds()))
        {
            // All existing threads have been already taken into account in threadCount variable, so we ignore them
            if (existingThreadIDs.contains(threadInfo.getThreadId()))
                continue;
            // Logback AsyncAppender thread needs to be taken into account
            if (threadInfo.getThreadName().equals("AsyncAppender-Worker-ASYNC"))
                threadCount++;
            // Logback basic threads need to be taken into account
            if (threadInfo.getThreadName().matches("logback-\\d+"))
                threadCount++;
            // Dynamic Attach thread needs to be taken into account, generally it is spawned by IDE
            if (threadInfo.getThreadName().equals("Attach Listener"))
                threadCount++;
        }

        for (String methodName : new String[]{
            "clientInitialization",
            "applyAddressConfig",
            "applyTokensConfig",
            // no seed provider in default configuration for clients
            // "applySeedProvider",
            // definitely not safe for clients - implicitly instantiates schema
            // "applyPartitioner",
            // definitely not safe for clients - implicitly instantiates StorageService
            // "applySnitch",
            "applyEncryptionContext",
            // starts "REQUEST-SCHEDULER" thread via RoundRobinScheduler
        })
        {
            Method method = databaseDescriptorClass.getDeclaredMethod(methodName);
            method.invoke(null);

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
            StringBuilder sb = new StringBuilder();
            for (Pair<String, Exception> violation : new ArrayList<>(violations))
                sb.append("\n\n")
                  .append("VIOLATION: ").append(violation.left); //.append('\n')
                  //.append(Throwables.getStackTraceAsString(violation.right));
            String msg = sb.toString();
            err.println(msg);

            fail(msg);
        }
    }
}
