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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.FBUtilities.preventIllegalAccessWarnings;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Helper class for running a tool and doing in-process checks
 */
public abstract class OfflineToolUtils
{
    static
    {
        preventIllegalAccessWarnings();
    }

    private static List<ThreadInfo> initialThreads;

    static final String[] OPTIONAL_THREADS_WITH_SCHEMA = {
    "ScheduledTasks:[1-9]",
    "ScheduledFastTasks:[1-9]",
    "OptionalTasks:[1-9]",
    "Reference-Reaper",
    "LocalPool-Cleaner(-networking|-chunk-cache)",
    "CacheCleanupExecutor:[1-9]",
    "CompactionExecutor:[1-9]",
    "ValidationExecutor:[1-9]",
    "NonPeriodicTasks:[1-9]",
    "Sampler:[1-9]",
    "SecondaryIndexManagement:[1-9]",
    "Strong-Reference-Leak-Detector:[1-9]",
    "Background_Reporter:[1-9]",
    "EXPIRING-MAP-REAPER:[1-9]",
    "ObjectCleanerThread",
    "process reaper",  // spawned by the jvm when executing external processes
                       // and may still be active when we check
    "Attach Listener", // spawned in intellij IDEA
    "JNA Cleaner",     // spawned by JNA
    };

    static final String[] NON_DEFAULT_MEMTABLE_THREADS =
    {
    "((Native|Slab|Heap)Pool|Logged)Cleaner"
    };

    public void assertNoUnexpectedThreadsStarted(String[] optionalThreadNames, boolean allowNonDefaultMemtableThreads)
    {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();

        Set<String> initial = initialThreads
                              .stream()
                              .map(ThreadInfo::getThreadName)
                              .collect(Collectors.toSet());

        Set<String> current = Arrays.stream(threads.getThreadInfo(threads.getAllThreadIds()))
                                    .filter(Objects::nonNull)
                                    .map(ThreadInfo::getThreadName)
                                    .collect(Collectors.toSet());
        Iterable<String> optionalNames = optionalThreadNames != null
                                         ? Arrays.asList(optionalThreadNames)
                                         : Collections.emptyList();
        if (allowNonDefaultMemtableThreads && DatabaseDescriptor.getMemtableConfigurations().containsKey("default"))
            optionalNames = Iterables.concat(optionalNames, Arrays.asList(NON_DEFAULT_MEMTABLE_THREADS));

        List<Pattern> optional = StreamSupport.stream(optionalNames.spliterator(), false)
                                              .map(Pattern::compile)
                                              .collect(Collectors.toList());

        current.removeAll(initial);

        Set<String> remain = current.stream()
                                    .filter(threadName -> optional.stream().noneMatch(pattern -> pattern.matcher(threadName).matches()))
                                    .collect(Collectors.toSet());

        if (!remain.isEmpty())
            System.err.println("Unexpected thread names: " + remain);

        assertTrue("Wrong thread status, active threads unaccounted for: " + remain, remain.isEmpty());
    }

    public void assertSchemaNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.schema.Schema");
    }

    public void assertSchemaLoaded()
    {
        assertClassLoaded("org.apache.cassandra.schema.Schema");
    }

    public void assertKeyspaceNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.db.Keyspace");
    }

    public void assertKeyspaceLoaded()
    {
        assertClassLoaded("org.apache.cassandra.db.Keyspace");
    }

    public void assertServerNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.transport.Server");
    }

    public void assertSystemKSNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.db.SystemKeyspace");
    }

    public void assertCLSMNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.db.commitlog.CommitLogSegmentManager");
    }

    public void assertClassLoaded(String clazz)
    {
        assertClassLoadedStatus(clazz, true);
    }

    public void assertClassNotLoaded(String clazz)
    {
        assertClassLoadedStatus(clazz, false);
    }

    private void assertClassLoadedStatus(String clazz, boolean expected)
    {
        for (ClassLoader cl = Thread.currentThread().getContextClassLoader(); cl != null; cl = cl.getParent())
        {
            try
            {
                Method mFindLoadedClass = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
                mFindLoadedClass.setAccessible(true);
                boolean loaded = mFindLoadedClass.invoke(cl, clazz) != null;

                if (expected)
                {
                    if (loaded)
                        return;
                }
                else
                    assertFalse(clazz + " has been loaded", loaded);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        if (expected)
            fail(clazz + " has not been loaded");
    }

    @BeforeClass
    public static void setupTester()
    {
        CassandraRelevantProperties.PARTITIONER.setString("org.apache.cassandra.dht.Murmur3Partitioner");

        // may start an async appender
        LoggerFactory.getLogger(OfflineToolUtils.class);

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        initialThreads = Arrays.asList(threads.getThreadInfo(threads.getAllThreadIds()));
    }

    public static String findOneSSTable(String ks, String cf) throws IOException
    {
        File cfDir = sstableDir(ks, cf);
        File[] sstableFiles = cfDir.tryList((file) -> file.isFile() && file.name().endsWith("-Data.db"));
        return sstableFiles[0].absolutePath();
    }

    public static String sstableDirName(String ks, String cf) throws IOException
    {
        return sstableDir(ks, cf).absolutePath();
    }

    public static File sstableDir(String ks, String cf) throws IOException
    {
        File dataDir = copySSTables();
        File ksDir = new File(dataDir, ks);
        File[] cfDirs = ksDir.tryList((dir, name) -> cf.equals(name) || name.startsWith(cf + '-'));
        return cfDirs[0];
    }

    public static File copySSTables() throws IOException
    {
        File dataDir = new File("build/test/cassandra/data");
        File srcDir = new File("test/data/legacy-sstables/ma");
        FileUtils.copyDirectory(new File(srcDir, "legacy_tables").toJavaIOFile(), new File(dataDir, "legacy_sstables").toJavaIOFile());
        return dataDir;
    }
    
    protected void assertCorrectEnvPostTest()
    {
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, true);
        assertSchemaLoaded();
        assertServerNotLoaded();
    }
}
