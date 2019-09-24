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

package org.apache.cassandra.distributed.impl;

import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * AbstractCluster creates, initializes and manages Cassandra instances ({@link Instance}.
 *
 * All instances created under the same cluster will have a shared ClassLoader that'll preload
 * common classes required for configuration and communication (byte buffers, primitives, config
 * objects etc). Shared classes are listed in {@link InstanceClassLoader}.
 *
 * Each instance has its own class loader that will load logging, yaml libraries and all non-shared
 * Cassandra package classes. The rule of thumb is that we'd like to have all Cassandra-specific things
 * (unless explitily shared through the common classloader) on a per-classloader basis in order to
 * allow creating more than one instance of DatabaseDescriptor and other Cassandra singletones.
 *
 * All actions (reading, writing, schema changes, etc) are executed by serializing lambda/runnables,
 * transferring them to instance-specific classloaders, deserializing and running them there. Most of
 * the things can be simply captured in closure or passed through `apply` method of the wrapped serializable
 * function/callable. You can use {@link Instance#{applies|runs|consumes}OnInstance} for executing
 * code on specific instance.
 *
 * Each instance has its own logger. Each instance log line will contain INSTANCE{instance_id}.
 *
 * As of today, messaging is faked by hooking into MessagingService, so we're not using usual Cassandra
 * handlers for internode to have more control over it. Messaging is wired by passing verbs manually.
 * coordinator-handling code and hooks to the callbacks can be found in {@link Coordinator}.
 */
public abstract class AbstractCluster<I extends IInstance> implements ICluster, AutoCloseable
{
    // WARNING: we have this logger not (necessarily) for logging, but
    // to ensure we have instantiated the main classloader's LoggerFactory (and any LogbackStatusListener)
    // before we instantiate any for a new instance
    private static final Logger logger = LoggerFactory.getLogger(AbstractCluster.class);
    private static final AtomicInteger generation = new AtomicInteger();

    private final File root;
    private final ClassLoader sharedClassLoader;

    // mutated by starting/stopping a node
    private final List<I> instances;
    private final Map<InetAddressAndPort, I> instanceMap;

    // mutated by user-facing API
    private final MessageFilters filters;

    protected class Wrapper extends DelegatingInvokableInstance implements IUpgradeableInstance
    {
        private final int generation;
        private final InstanceConfig config;
        private volatile IInvokableInstance delegate;
        private volatile Versions.Version version;
        private volatile boolean isShutdown = true;

        protected IInvokableInstance delegate()
        {
            if (delegate == null)
                delegate = newInstance(generation);
            return delegate;
        }

        public Wrapper(int generation, Versions.Version version, InstanceConfig config)
        {
            this.generation = generation;
            this.config = config;
            this.version = version;
            // we ensure there is always a non-null delegate, so that the executor may be used while the node is offline
            this.delegate = newInstance(generation);
        }

        private IInvokableInstance newInstance(int generation)
        {
            ClassLoader classLoader = new InstanceClassLoader(generation, version.classpath, sharedClassLoader);
            return Instance.transferAdhoc((SerializableBiFunction<IInstanceConfig, ClassLoader, Instance>)Instance::new, classLoader)
                           .apply(config, classLoader);
        }

        public IInstanceConfig config()
        {
            return config;
        }

        public boolean isShutdown()
        {
            return isShutdown;
        }

        @Override
        public synchronized void startup()
        {
            if (!isShutdown)
                throw new IllegalStateException();
            delegate().startup(AbstractCluster.this);
            isShutdown = false;
            updateMessagingVersions();
        }

        @Override
        public synchronized Future<Void> shutdown()
        {
            return shutdown(true);
        }

        @Override
        public synchronized Future<Void> shutdown(boolean graceful)
        {
            if (isShutdown)
                throw new IllegalStateException();
            isShutdown = true;
            Future<Void> future = delegate.shutdown(graceful);
            delegate = null;
            return future;
        }

        @Override
        public void receiveMessage(IMessage message)
        {
            IInvokableInstance delegate = this.delegate;
            if (!isShutdown && delegate != null) // since we sync directly on the other node, we drop messages immediately if we are shutdown
                delegate.receiveMessage(message);
        }

        @Override
        public synchronized void setVersion(Versions.Version version)
        {
            if (!isShutdown)
                throw new IllegalStateException("Must be shutdown before version can be modified");
            // re-initialise
            this.version = version;
            if (delegate != null)
            {
                // we can have a non-null delegate even thought we are shutdown, if delegate() has been invoked since shutdown.
                delegate.shutdown();
                delegate = null;
            }
        }
    }

    protected AbstractCluster(File root, Versions.Version version, List<InstanceConfig> configs,
                              ClassLoader sharedClassLoader)
    {
        this.root = root;
        this.sharedClassLoader = sharedClassLoader;
        this.instances = new ArrayList<>();
        this.instanceMap = new HashMap<>();
        int generation = AbstractCluster.generation.incrementAndGet();

        for (InstanceConfig config : configs)
        {
            I instance = newInstanceWrapper(generation, version, config);
            instances.add(instance);
            // we use the config().broadcastAddressAndPort() here because we have not initialised the Instance
            I prev = instanceMap.put(instance.broadcastAddressAndPort(), instance);
            if (null != prev)
                throw new IllegalStateException("Cluster cannot have multiple nodes with same InetAddressAndPort: " + instance.broadcastAddressAndPort() + " vs " + prev.broadcastAddressAndPort());
        }
        this.filters = new MessageFilters();
    }

    protected abstract I newInstanceWrapper(int generation, Versions.Version version, InstanceConfig config);

    /**
     * WARNING: we index from 1 here, for consistency with inet address!
     */
    public ICoordinator coordinator(int node)
    {
        return instances.get(node - 1).coordinator();
    }
    /**
     * WARNING: we index from 1 here, for consistency with inet address!
     */
    public I get(int node) { return instances.get(node - 1); }
    public I get(InetAddressAndPort addr) { return instanceMap.get(addr); }

    public int size()
    {
        return instances.size();
    }

    public Stream<I> stream() { return instances.stream(); }

    public Stream<I> stream(String dcName)
    {
        return instances.stream().filter(i -> i.config().localDatacenter().equals(dcName));
    }

    public Stream<I> stream(String dcName, String rackName)
    {
        return instances.stream().filter(i -> i.config().localDatacenter().equals(dcName) &&
                                              i.config().localRack().equals(rackName));
    }

    public void forEach(IIsolatedExecutor.SerializableRunnable runnable) { forEach(i -> i.sync(runnable)); }
    public void forEach(Consumer<? super I> consumer) { instances.forEach(consumer); }
    public void parallelForEach(IIsolatedExecutor.SerializableConsumer<? super I> consumer, long timeout, TimeUnit units)
    {
        FBUtilities.waitOnFutures(instances.stream()
                                           .map(i -> i.async(consumer).apply(i))
                                           .collect(Collectors.toList()),
                                  timeout, units);
    }


    public IMessageFilters filters() { return filters; }
    public MessageFilters.Builder verbs(MessagingService.Verb ... verbs) { return filters.verbs(verbs); }

    public void disableAutoCompaction(String keyspace)
    {
        forEach(() -> {
            for (ColumnFamilyStore cs : Keyspace.open(keyspace).getColumnFamilyStores())
                cs.disableAutoCompaction();
        });
    }

    public void schemaChange(String query)
    {
        get(1).sync(() -> {
            try (SchemaChangeMonitor monitor = new SchemaChangeMonitor())
            {
                // execute the schema change
                coordinator(1).execute(query, ConsistencyLevel.ALL);
                monitor.waitForAgreement();
            }
        }).run();
    }

    private void updateMessagingVersions()
    {
        for (IInstance reportTo: instances)
        {
            if (reportTo.isShutdown())
                continue;

            for (IInstance reportFrom: instances)
            {
                if (reportFrom == reportTo || reportFrom.isShutdown())
                    continue;

                int minVersion = Math.min(reportFrom.getMessagingVersion(), reportTo.getMessagingVersion());
                reportTo.setMessagingVersion(reportFrom.broadcastAddressAndPort(), minVersion);
            }
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
        final List<IListen.Cancel> cleanup;
        volatile boolean schemaHasChanged;
        final SimpleCondition agreement = new SimpleCondition();

        public SchemaChangeMonitor()
        {
            this.cleanup = new ArrayList<>(instances.size());
            for (IInstance instance : instances)
                cleanup.add(instance.listen().schema(this::signal));
        }

        private void signal()
        {
            if (schemaHasChanged && 1 == instances.stream().filter(i -> !i.isShutdown()).map(IInstance::schemaVersion).distinct().count())
                agreement.signalAll();
        }

        @Override
        public void close()
        {
            for (IListen.Cancel cancel : cleanup)
                cancel.cancel();
        }

        public void waitForAgreement()
        {
            schemaHasChanged = true;
            signal();
            try
            {
                if (!agreement.await(1L, TimeUnit.MINUTES))
                    throw new InterruptedException();
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException("Schema agreement not reached");
            }
        }
    }

    public void schemaChange(String statement, int instance)
    {
        get(instance).schemaChangeInternal(statement);
    }

    public void startup()
    {
        forEach(I::startup);
    }

    protected interface Factory<I extends IInstance, C extends AbstractCluster<I>>
    {
        C newCluster(File root, Versions.Version version, List<InstanceConfig> configs, ClassLoader sharedClassLoader);
    }

    public static class Builder<I extends IInstance, C extends AbstractCluster<I>>
    {
        private final Factory<I, C> factory;
        private int nodeCount;
        private int subnet;
        private Map<Integer, Pair<String,String>> nodeIdTopology;
        private File root;
        private Versions.Version version;
        private Consumer<InstanceConfig> configUpdater;

        public Builder(Factory<I, C> factory)
        {
            this.factory = factory;
        }

        public Builder<I, C> withSubnet(int subnet)
        {
            this.subnet = subnet;
            return this;
        }

        public Builder<I, C> withNodes(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder<I, C> withDCs(int dcCount)
        {
            return withRacks(dcCount, 1);
        }

        public Builder<I, C> withRacks(int dcCount, int racksPerDC)
        {
            if (nodeCount == 0)
                throw new IllegalStateException("Node count will be calculated. Do not supply total node count in the builder");

            int totalRacks = dcCount * racksPerDC;
            int nodesPerRack = (nodeCount + totalRacks - 1) / totalRacks; // round up to next integer
            return withRacks(dcCount, racksPerDC, nodesPerRack);
        }

        public Builder<I, C> withRacks(int dcCount, int racksPerDC, int nodesPerRack)
        {
            if (nodeIdTopology != null)
                throw new IllegalStateException("Network topology already created. Call withDCs/withRacks once or before withDC/withRack calls");

            nodeIdTopology = new HashMap<>();
            int nodeId = 1;
            for (int dc = 1; dc <= dcCount; dc++)
            {
                for (int rack = 1; rack <= racksPerDC; rack++)
                {
                    for (int rackNodeIdx = 0; rackNodeIdx < nodesPerRack; rackNodeIdx++)
                        nodeIdTopology.put(nodeId++, Pair.create(dcName(dc), rackName(rack)));
                }
            }
            // adjust the node count to match the allocatation
            final int adjustedNodeCount = dcCount * racksPerDC * nodesPerRack;
            if (adjustedNodeCount != nodeCount)
            {
                assert adjustedNodeCount > nodeCount : "withRacks should only ever increase the node count";
                logger.info("Network topology of {} DCs with {} racks per DC and {} nodes per rack required increasing total nodes to {}",
                            dcCount, racksPerDC, nodesPerRack, adjustedNodeCount);
                nodeCount = adjustedNodeCount;
            }
            return this;
        }

        public Builder<I, C> withDC(String dcName, int nodeCount)
        {
            return withRack(dcName, rackName(1), nodeCount);
        }

        public Builder<I, C> withRack(String dcName, String rackName, int nodesInRack)
        {
            if (nodeIdTopology == null)
            {
                if (nodeCount > 0)
                    throw new IllegalStateException("Node count must not be explicitly set, or allocated using withDCs/withRacks");

                nodeIdTopology = new HashMap<>();
            }
            for (int nodeId = nodeCount + 1; nodeId <= nodeCount + nodesInRack; nodeId++)
                nodeIdTopology.put(nodeId, Pair.create(dcName, rackName));

            nodeCount += nodesInRack;
            return this;
        }

        // Map of node ids to dc and rack - must be contiguous with an entry nodeId 1 to nodeCount
        public Builder<I, C> withNodeIdTopology(Map<Integer,Pair<String,String>> nodeIdTopology)
        {
            if (nodeIdTopology.isEmpty())
                throw new IllegalStateException("Topology is empty. It must have an entry for every nodeId.");

            IntStream.rangeClosed(1, nodeIdTopology.size()).forEach(nodeId -> {
                if (nodeIdTopology.get(nodeId) == null)
                    throw new IllegalStateException("Topology is missing entry for nodeId " + nodeId);
            });

            if (nodeCount != nodeIdTopology.size())
            {
                nodeCount = nodeIdTopology.size();
                logger.info("Adjusting node count to {} for supplied network topology", nodeCount);

            }

            this.nodeIdTopology = new HashMap<>(nodeIdTopology);

            return this;
        }

        public Builder<I, C> withRoot(File root)
        {
            this.root = root;
            return this;
        }

        public Builder<I, C> withVersion(Versions.Version version)
        {
            this.version = version;
            return this;
        }

        public Builder<I, C> withConfig(Consumer<InstanceConfig> updater)
        {
            this.configUpdater = updater;
            return this;
        }

        public C createWithoutStarting() throws IOException
        {
            File root = this.root;
            Versions.Version version = this.version;

            if (root == null)
                root = Files.createTempDirectory("dtests").toFile();

            if (version == null)
                version = Versions.CURRENT;

            if (nodeCount <= 0)
                throw new IllegalStateException("Cluster must have at least one node");

            if (nodeIdTopology == null)
                nodeIdTopology = IntStream.rangeClosed(1, nodeCount).boxed()
                                          .collect(Collectors.toMap(nodeId -> nodeId,
                                                                    nodeId -> Pair.create(dcName(0), rackName(0))));

            root.mkdirs();
            setupLogging(root);

            ClassLoader sharedClassLoader = Thread.currentThread().getContextClassLoader();

            List<InstanceConfig> configs = new ArrayList<>();
            long token = Long.MIN_VALUE + 1, increment = 2 * (Long.MAX_VALUE / nodeCount);

            String ipPrefix = "127.0." + subnet + ".";

            NetworkTopology networkTopology = NetworkTopology.build(ipPrefix, 7012, nodeIdTopology);

            for (int i = 0 ; i < nodeCount ; ++i)
            {
                int nodeNum = i + 1;
                String ipAddress = ipPrefix + nodeNum;
                InstanceConfig config = InstanceConfig.generate(i + 1, ipAddress, networkTopology, root, String.valueOf(token));
                if (configUpdater != null)
                    configUpdater.accept(config);
                configs.add(config);
                token += increment;
            }

            return factory.newCluster(root, version, configs, sharedClassLoader);
        }

        public C start() throws IOException
        {
            C cluster = createWithoutStarting();
            cluster.startup();
            return cluster;
        }
    }

    static String dcName(int index)
    {
        return "datacenter" + index;
    }

    static String rackName(int index)
    {
        return "rack" + index;
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
        FBUtilities.waitOnFutures(instances.stream()
                                           .filter(i -> !i.isShutdown())
                                           .map(IInstance::shutdown)
                                           .collect(Collectors.toList()),
                                  1L, TimeUnit.MINUTES);

        instances.clear();
        instanceMap.clear();
        // Make sure to only delete directory when threads are stopped
        FileUtils.deleteRecursive(root);

        //withThreadLeakCheck(futures);
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

