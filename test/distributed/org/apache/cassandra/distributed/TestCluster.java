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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * TestCluster creates, initializes and manages Cassandra instances ({@link Instance}.
 *
 * All instances created under the same cluster will have a shared ClassLoader that'll preload
 * common classes required for configuration and communication (byte buffers, primitives, config
 * objects etc). Shared classes are listed in {@link InstanceClassLoader#commonClasses}.
 *
 * Each instance has its own class loader that will load logging, yaml libraries and all non-shared
 * Cassandra package classes. The rule of thumb is that we'd like to have all Cassandra-specific things
 * (unless explitily shared through the common classloader) on a per-classloader basis in order to
 * allow creating more than one instance of DatabaseDescriptor and other Cassandra singletones.
 *
 * All actions (reading, writing, schema changes, etc) are executed by serializing lambda/runnables,
 * transferring them to instance-specific classloaders, deserializing and running them there. Most of
 * the things can be simply captured in closure or passed through `apply` method of the wrapped serializable
 * function/callable. You can use {@link InvokableInstance#{applies|runs|consumes}OnInstance} for executing
 * code on specific instance.
 *
 * Each instance has its own logger. Each instance log line will contain INSTANCE{instance_id}.
 *
 * As of today, messaging is faked by hooking into MessagingService, so we're not using usual Cassandra
 * handlers for internode to have more control over it. Messaging is wired by passing verbs manually.
 * coordinator-handling code and hooks to the callbacks can be found in {@link Coordinator}.
 */
public class TestCluster implements AutoCloseable
{
    // WARNING: we have this logger not (necessarily) for logging, but
    // to ensure we have instantiated the main classloader's LoggerFactory (and any LogbackStatusListener)
    // before we instantiate any for a new instance
    private static final Logger logger = LoggerFactory.getLogger(TestCluster.class);

    private final File root;
    private final List<Instance> instances;
    private final Coordinator coordinator;
    private final Map<InetAddressAndPort, Instance> instanceMap;
    private final MessageFilters filters;

    private TestCluster(File root, List<Instance> instances)
    {
        this.root = root;
        this.instances = instances;
        this.instanceMap = new HashMap<>();
        this.coordinator = new Coordinator(instances.get(0));
        this.filters = new MessageFilters(this);
    }

    void launch()
    {
        FBUtilities.waitOnFutures(instances.stream()
                .map(i -> i.isolatedExecutor.submit(() -> i.launch(this)))
                .collect(Collectors.toList())
        );
        for (Instance instance : instances)
            instanceMap.put(instance.getBroadcastAddress(), instance);
    }

    public int size()
    {
        return instances.size();
    }

    public Coordinator coordinator()
    {
        return coordinator;
    }

    /**
     * WARNING: we index from 1 here, for consistency with inet address!
     */
    public Instance get(int idx)
    {
        return instances.get(idx - 1);
    }

    public Stream<Instance> stream() { return instances.stream(); }

    public Instance get(InetAddressAndPort addr)
    {
        return instanceMap.get(addr);
    }

    MessageFilters filters()
    {
        return filters;
    }

    MessageFilters.Builder verbs(MessagingService.Verb ... verbs)
    {
        return filters.verbs(verbs);
    }

    public void disableAutoCompaction(String keyspace)
    {
        for (Instance instance : instances)
        {
            instance.runOnInstance(() -> {
                for (ColumnFamilyStore cs : Keyspace.open(keyspace).getColumnFamilyStores())
                    cs.disableAutoCompaction();
            });
        }
    }

    public void schemaChange(String query)
    {
        try (SchemaChangeMonitor monitor = new SchemaChangeMonitor())
        {
            // execute the schema change
            coordinator().execute(query, ConsistencyLevel.ALL);
            monitor.waitForAgreement();
        }
    }

    /**
     * Will wait for a schema change AND agreement that occurs after it is created
     * (and precedes the invocation to waitForAgreement)
     *
     * Works by simply checking if all UUIDs agree after any schema version change event,
     * so long as the waitForAgreement method has been entered (indicating the change has
     * taken place on the coordinator)
     *
     * This could perhaps be made a little more robust, but this should more than suffice.
     */
    public class SchemaChangeMonitor implements AutoCloseable
    {
        public SchemaChangeMonitor() {}

        @Override
        public void close() { }

        public void waitForAgreement()
        {
            long start = System.nanoTime();
            while (1 != instances.stream().map(Instance::getSchemaVersion).distinct().count())
            {
                if (System.nanoTime() - start > TimeUnit.MINUTES.toNanos(1L))
                    throw new IllegalStateException("Schema agreement not reached");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        }
    }

    public void schemaChange(String statement, int instance)
    {
        get(instance).schemaChange(statement);
    }

    public static TestCluster create(int nodeCount) throws Throwable
    {
        return create(nodeCount, Files.createTempDirectory("dtests").toFile());
    }

    public static TestCluster create(int nodeCount, File root)
    {
        root.mkdirs();
        setupLogging(root);

        IntFunction<ClassLoader> classLoaderFactory = InstanceClassLoader.createFactory(
                (URLClassLoader) Thread.currentThread().getContextClassLoader());
        List<Instance> instances = new ArrayList<>();
        long token = Long.MIN_VALUE + 1, increment = 2 * (Long.MAX_VALUE / nodeCount);
        for (int i = 0 ; i < nodeCount ; ++i)
        {
            InstanceConfig instanceConfig = InstanceConfig.generate(i + 1, root, String.valueOf(token));
            instances.add(new Instance(instanceConfig, classLoaderFactory.apply(i + 1)));
            token += increment;
        }

        TestCluster cluster = new TestCluster(root, instances);
        cluster.launch();
        return cluster;
    }

    private static void setupLogging(File root)
    {
        try
        {
            String testConfPath = "test/conf/logback-dtest.xml";
            Path logConfPath = Paths.get(root.getPath(), "/logback-dtest.xml");
            if (!logConfPath.toFile().exists())
            {
                Files.copy(new File(testConfPath).toPath(),
                           logConfPath);
            }
            System.setProperty("logback.configurationFile", "file://" + logConfPath);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        List<Future<?>> futures = instances.stream()
                .map(i -> i.isolatedExecutor.submit(i::shutdown))
                .collect(Collectors.toList());

        // Make sure to only delete directory when threads are stopped
        FBUtilities.waitOnFutures(futures, 60, TimeUnit.SECONDS);
        FileUtils.deleteRecursive(root);

        //withThreadLeakCheck(futures);
        System.gc();
    }

    // We do not want this check to run every time until we fix problems with tread stops
    private void withThreadLeakCheck(List<Future<?>> futures)
    {
        FBUtilities.waitOnFutures(futures);

        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        threadSet = Sets.difference(threadSet, Collections.singletonMap(Thread.currentThread(), null).keySet());
        if (!threadSet.isEmpty())
        {
            for (Thread thread : threadSet)
            {
                System.out.println(thread);
                System.out.println(Arrays.toString(thread.getStackTrace()));
            }
            throw new RuntimeException(String.format("Not all threads have shut down. %d threads are still running: %s", threadSet.size(), threadSet));
        }
    }

}

