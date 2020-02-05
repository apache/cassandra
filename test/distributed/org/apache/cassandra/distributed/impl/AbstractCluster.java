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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
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

    private final Versions.Version initialVersion;

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
            ClassLoader classLoader = new InstanceClassLoader(generation, config.num, version.classpath, sharedClassLoader);
            return Instance.transferAdhoc((SerializableBiFunction<IInstanceConfig, ClassLoader, Instance>) Instance::new, classLoader)
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

        public int liveMemberCount()
        {
            if (!isShutdown && delegate != null)
                return delegate().liveMemberCount();

            throw new IllegalStateException("Cannot get live member count on shutdown instance");
        }

        public int nodetool(String... commandAndArgs)
        {
            return delegate().nodetool(commandAndArgs);
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

    protected AbstractCluster(File root, Versions.Version initialVersion, List<InstanceConfig> configs,
                              ClassLoader sharedClassLoader)
    {
        this.root = root;
        this.sharedClassLoader = sharedClassLoader;
        this.instances = new ArrayList<>();
        this.instanceMap = new HashMap<>();
        this.initialVersion = initialVersion;
        int generation = AbstractCluster.generation.incrementAndGet();

        for (InstanceConfig config : configs)
        {
            I instance = newInstanceWrapperInternal(generation, initialVersion, config);
            instances.add(instance);
            // we use the config().broadcastAddressAndPort() here because we have not initialised the Instance
            I prev = instanceMap.put(instance.broadcastAddressAndPort(), instance);
            if (null != prev)
                throw new IllegalStateException("Cluster cannot have multiple nodes with same InetAddressAndPort: " + instance.broadcastAddressAndPort() + " vs " + prev.broadcastAddressAndPort());
        }
        this.filters = new MessageFilters();
    }

    protected abstract I newInstanceWrapper(int generation, Versions.Version version, InstanceConfig config);

    protected I newInstanceWrapperInternal(int generation, Versions.Version version, InstanceConfig config)
    {
        config.validate();
        return newInstanceWrapper(generation, version, config);
    }

    public I bootstrap(InstanceConfig config)
    {
        if (!config.has(Feature.GOSSIP) || !config.has(Feature.NETWORK))
            throw new IllegalStateException("New nodes can only be bootstrapped when gossip and networking is enabled.");

        I instance = newInstanceWrapperInternal(0, initialVersion, config);

        instances.add(instance);
        I prev = instanceMap.put(config.broadcastAddressAndPort(), instance);

        if (null != prev)
        {
            throw new IllegalStateException(String.format("This cluster already contains a node (%d) with with same address and port: %s",
                                                          config.num,
                                                          instance));
        }

        return instance;
    }

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
    public I get(int node)
    {
        return instances.get(node - 1);
    }

    public I get(InetAddressAndPort addr)
    {
        return instanceMap.get(addr);
    }

    public int size()
    {
        return instances.size();
    }

    public Stream<I> stream()
    {
        return instances.stream();
    }

    public Stream<I> stream(String dcName)
    {
        return instances.stream().filter(i -> i.config().localDatacenter().equals(dcName));
    }

    public Stream<I> stream(String dcName, String rackName)
    {
        return instances.stream().filter(i -> i.config().localDatacenter().equals(dcName) &&
                                              i.config().localRack().equals(rackName));
    }

    public void forEach(IIsolatedExecutor.SerializableRunnable runnable)
    {
        forEach(i -> i.sync(runnable));
    }

    public void forEach(Consumer<? super I> consumer)
    {
        forEach(instances, consumer);
    }

    public void forEach(List<I> instancesForOp, Consumer<? super I> consumer)
    {
        instancesForOp.forEach(consumer);
    }

    public void parallelForEach(IIsolatedExecutor.SerializableConsumer<? super I> consumer, long timeout, TimeUnit unit)
    {
        parallelForEach(instances, consumer, timeout, unit);
    }

    public void parallelForEach(List<I> instances, IIsolatedExecutor.SerializableConsumer<? super I> consumer, long timeout, TimeUnit unit)
    {
        FBUtilities.waitOnFutures(instances.stream()
                                           .map(i -> i.async(consumer).apply(i))
                                           .collect(Collectors.toList()),
                                  timeout, unit);
    }

    public IMessageFilters filters()
    {
        return filters;
    }

    public MessageFilters.Builder verbs(MessagingService.Verb... verbs)
    {
        int[] ids = new int[verbs.length];
        for (int i = 0; i < verbs.length; ++i)
            ids[i] = verbs[i].ordinal();
        return filters.verbs(ids);
    }

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
                monitor.waitForCompletion();
            }
        }).run();
    }

    private void updateMessagingVersions()
    {
        for (IInstance reportTo : instances)
        {
            if (reportTo.isShutdown())
                continue;

            for (IInstance reportFrom : instances)
            {
                if (reportFrom == reportTo || reportFrom.isShutdown())
                    continue;

                int minVersion = Math.min(reportFrom.getMessagingVersion(), reportTo.getMessagingVersion());
                reportTo.setMessagingVersion(reportFrom.broadcastAddressAndPort(), minVersion);
            }
        }
    }

    public abstract class ChangeMonitor implements AutoCloseable
    {
        final List<IListen.Cancel> cleanup;
        final SimpleCondition completed;
        private final long timeOut;
        private final TimeUnit timeoutUnit;
        volatile boolean changed;

        public ChangeMonitor(long timeOut, TimeUnit timeoutUnit)
        {
            this.timeOut = timeOut;
            this.timeoutUnit = timeoutUnit;
            this.cleanup = new ArrayList<>(instances.size());
            this.completed = new SimpleCondition();
        }

        protected void signal()
        {
            if (changed && isCompleted())
                completed.signalAll();
        }

        @Override
        public void close()
        {
            for (IListen.Cancel cancel : cleanup)
                cancel.cancel();
        }

        public void waitForCompletion()
        {
            startPolling();
            changed = true;
            signal();
            try
            {
                if (!completed.await(timeOut, timeoutUnit))
                    throw new InterruptedException();
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException(getMonitorTimeoutMessage());
            }
        }

        private void startPolling()
        {
            for (IInstance instance : instances)
                cleanup.add(startPolling(instance));
        }

        protected abstract IListen.Cancel startPolling(IInstance instance);

        protected abstract boolean isCompleted();

        protected abstract String getMonitorTimeoutMessage();
    }


    /**
     * Will wait for a schema change AND agreement that occurs after it is created
     * (and precedes the invocation to waitForAgreement)
     * <p>
     * Works by simply checking if all UUIDs agree after any schema version change event,
     * so long as the waitForAgreement method has been entered (indicating the change has
     * taken place on the coordinator)
     * <p>
     * This could perhaps be made a little more robust, but this should more than suffice.
     */
    public class SchemaChangeMonitor extends ChangeMonitor
    {
        public SchemaChangeMonitor()
        {
            super(70, TimeUnit.SECONDS);
        }

        protected IListen.Cancel startPolling(IInstance instance)
        {
            return instance.listen().schema(this::signal);
        }

        protected boolean isCompleted()
        {
            return 1 == instances.stream().map(IInstance::schemaVersion).distinct().count();
        }

        protected String getMonitorTimeoutMessage()
        {
            return "Schema agreement not reached";
        }
    }

    public class AllMembersAliveMonitor extends ChangeMonitor
    {
        public AllMembersAliveMonitor()
        {
            super(60, TimeUnit.SECONDS);
        }

        protected IListen.Cancel startPolling(IInstance instance)
        {
            return instance.listen().liveMembers(this::signal);
        }

        protected boolean isCompleted()
        {
            return instances.stream().allMatch(i -> !i.config().has(Feature.GOSSIP) || i.liveMemberCount() == instances.size());
        }

        protected String getMonitorTimeoutMessage()
        {
            return "Live member count did not converge across all instances";
        }
    }

    public void schemaChange(String statement, int instance)
    {
        get(instance).schemaChangeInternal(statement);
    }

    public void startup()
    {
        try (AllMembersAliveMonitor monitor = new AllMembersAliveMonitor())
        {
            // Start any instances with auto_bootstrap enabled first, and in series to avoid issues
            // with multiple nodes bootstrapping with consistent range movement enabled,
            // and then start any instances with it disabled in parallel.
            List<I> startSequentially = new ArrayList<>();
            List<I> startParallel = new ArrayList<>();
            for (int i = 0; i < instances.size(); i++)
            {
                I instance = instances.get(i);

                if (i == 0 || (boolean) instance.config().get("auto_bootstrap"))
                    startSequentially.add(instance);
                else
                    startParallel.add(instance);
            }

            forEach(startSequentially, I::startup);
            parallelForEach(startParallel, I::startup, 0, null);
            monitor.waitForCompletion();
        }
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
        private Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology;
        private TokenSupplier tokenSupplier;
        private File root;
        private Versions.Version version = Versions.CURRENT;
        private Consumer<InstanceConfig> configUpdater;

        public Builder(Factory<I, C> factory)
        {
            this.factory = factory;
        }

        public Builder<I, C> withTokenSupplier(TokenSupplier tokenSupplier)
        {
            this.tokenSupplier = tokenSupplier;
            return this;
        }

        public Builder<I, C> withSubnet(int subnet)
        {
            this.subnet = subnet;
            return this;
        }

        public Builder<I, C> withNodes(int nodeCount)
        {
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
                        nodeIdTopology.put(nodeId++, NetworkTopology.dcAndRack(dcName(dc), rackName(rack)));
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
                nodeIdTopology.put(nodeId, NetworkTopology.dcAndRack(dcName, rackName));

            nodeCount += nodesInRack;
            return this;
        }

        // Map of node ids to dc and rack - must be contiguous with an entry nodeId 1 to nodeCount
        public Builder<I, C> withNodeIdTopology(Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology)
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
            if (root == null)
                root = Files.createTempDirectory("dtests").toFile();

            if (nodeCount <= 0)
                throw new IllegalStateException("Cluster must have at least one node");

            if (nodeIdTopology == null)
            {
                nodeIdTopology = IntStream.rangeClosed(1, nodeCount).boxed()
                                          .collect(Collectors.toMap(nodeId -> nodeId,
                                                                    nodeId -> NetworkTopology.dcAndRack(dcName(0), rackName(0))));
            }

            root.mkdirs();
            setupLogging(root);

            ClassLoader sharedClassLoader = Thread.currentThread().getContextClassLoader();

            List<InstanceConfig> configs = new ArrayList<>();

            if (tokenSupplier == null)
                tokenSupplier = evenlyDistributedTokens(nodeCount);

            for (int i = 0; i < nodeCount; ++i)
            {
                int nodeNum = i + 1;
                configs.add(createInstanceConfig(nodeNum));
            }

            return factory.newCluster(root, version, configs, sharedClassLoader);
        }

        public InstanceConfig newInstanceConfig(C cluster)
        {
            return createInstanceConfig(cluster.size() + 1);
        }

        private InstanceConfig createInstanceConfig(int nodeNum)
        {
            String ipPrefix = "127.0." + subnet + ".";
            String seedIp = ipPrefix + "1";
            String ipAddress = ipPrefix + nodeNum;
            long token = tokenSupplier.token(nodeNum);

            NetworkTopology topology = NetworkTopology.build(ipPrefix, 7012, nodeIdTopology);

            InstanceConfig config = InstanceConfig.generate(nodeNum, ipAddress, topology, root, String.valueOf(token), seedIp);
            if (configUpdater != null)
                configUpdater.accept(config);

            return config;
        }

        public C start() throws IOException
        {
            C cluster = createWithoutStarting();
            cluster.startup();
            return cluster;
        }
    }

    public static TokenSupplier evenlyDistributedTokens(int numNodes)
    {
        long increment = (Long.MAX_VALUE / numNodes) * 2;
        return (int nodeId) -> {
            assert nodeId <= numNodes : String.format("Can not allocate a token for a node %s, since only %s nodes are allowed by the token allocation strategy",
                                                      nodeId, numNodes);
            return Long.MIN_VALUE + 1 + nodeId * increment;
        };
    }

    public static interface TokenSupplier
    {
        public long token(int nodeId);
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

