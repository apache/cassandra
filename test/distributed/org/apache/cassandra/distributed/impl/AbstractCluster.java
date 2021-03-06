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
import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.distributed.shared.Isolated;
import org.apache.cassandra.distributed.shared.MessageFilters;
import org.apache.cassandra.distributed.shared.Metrics;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Shared;
import org.apache.cassandra.distributed.shared.ShutdownException;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import static org.apache.cassandra.distributed.shared.NetworkTopology.addressAndPort;

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
public abstract class AbstractCluster<I extends IInstance> implements ICluster<I>, AutoCloseable
{
    public static Versions.Version CURRENT_VERSION = new Versions.Version(FBUtilities.getReleaseVersionString(), Versions.getClassPath());;

    // WARNING: we have this logger not (necessarily) for logging, but
    // to ensure we have instantiated the main classloader's LoggerFactory (and any LogbackStatusListener)
    // before we instantiate any for a new instance
    private static final Logger logger = LoggerFactory.getLogger(AbstractCluster.class);
    private static final AtomicInteger GENERATION = new AtomicInteger();

    // include byteman so tests can use
    private static final Set<String> SHARED_CLASSES = findClassesMarkedForSharedClassLoader();
    private static final Set<String> ISOLATED_CLASSES = findClassesMarkedForInstanceClassLoader();
    private static final Predicate<String> SHARED_PREDICATE = s -> {
        if (ISOLATED_CLASSES.contains(s))
            return false;

        return SHARED_CLASSES.contains(s) ||
               InstanceClassLoader.getDefaultLoadSharedFilter().test(s) ||
               s.startsWith("org.jboss.byteman");
    };

    private final UUID clusterId = UUID.randomUUID();
    private final File root;
    private final ClassLoader sharedClassLoader;
    private final int subnet;
    private final TokenSupplier tokenSupplier;
    private final Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology;
    private final Consumer<IInstanceConfig> configUpdater;
    private final int broadcastPort;

    // mutated by starting/stopping a node
    private final List<I> instances;
    private final Map<InetSocketAddress, I> instanceMap;

    private final Versions.Version initialVersion;

    // mutated by user-facing API
    private final MessageFilters filters;
    private final INodeProvisionStrategy.Strategy nodeProvisionStrategy;
    private final BiConsumer<ClassLoader, Integer> instanceInitializer;
    private final int datadirCount;
    private volatile Thread.UncaughtExceptionHandler previousHandler = null;
    private volatile BiPredicate<Integer, Throwable> ignoreUncaughtThrowable = null;
    private final List<Throwable> uncaughtExceptions = new CopyOnWriteArrayList<>();

    /**
     * Common builder, add methods that are applicable to both Cluster and Upgradable cluster here.
     */
    public static abstract class AbstractBuilder<I extends IInstance, C extends ICluster, B extends AbstractBuilder<I, C, B>>
        extends org.apache.cassandra.distributed.shared.AbstractBuilder<I, C, B>
    {
        private INodeProvisionStrategy.Strategy nodeProvisionStrategy = INodeProvisionStrategy.Strategy.MultipleNetworkInterfaces;

        public AbstractBuilder(Factory<I, C, B> factory)
        {
            super(factory);
        }

        public B withNodeProvisionStrategy(INodeProvisionStrategy.Strategy nodeProvisionStrategy)
        {
            this.nodeProvisionStrategy = nodeProvisionStrategy;
            return (B) this;
        }
    }

    protected class Wrapper extends DelegatingInvokableInstance implements IUpgradeableInstance
    {
        private final int generation;
        private final IInstanceConfig config;
        private volatile IInvokableInstance delegate;
        private volatile Versions.Version version;
        @GuardedBy("this")
        private volatile boolean isShutdown = true;
        @GuardedBy("this")
        private InetSocketAddress broadcastAddress;

        protected IInvokableInstance delegate()
        {
            if (delegate == null)
                throw new IllegalStateException("Can't use shut down instances, delegate is null");
            return delegate;
        }

        protected IInvokableInstance delegateForStartup()
        {
            if (delegate == null)
                delegate = newInstance(generation);
            return delegate;
        }

        public Wrapper(int generation, Versions.Version version, IInstanceConfig config)
        {
            this.generation = generation;
            this.config = config;
            this.version = version;
            // we ensure there is always a non-null delegate, so that the executor may be used while the node is offline
            this.delegate = newInstance(generation);
            this.broadcastAddress = config.broadcastAddress();
        }

        private IInvokableInstance newInstance(int generation)
        {
            ClassLoader classLoader = new InstanceClassLoader(generation, config.num(), version.classpath, sharedClassLoader, SHARED_PREDICATE);
            if (instanceInitializer != null)
                instanceInitializer.accept(classLoader, config.num());
            return Instance.transferAdhoc((SerializableBiFunction<IInstanceConfig, ClassLoader, Instance>)Instance::new, classLoader)
                                        .apply(config.forVersion(version.major), classLoader);
        }

        public IInstanceConfig config()
        {
            return config;
        }

        public boolean isShutdown()
        {
            return isShutdown;
        }

        private boolean isRunning()
        {
            return !isShutdown;
        }

        @Override
        public synchronized void startup()
        {
            startup(AbstractCluster.this);
        }
        public synchronized void startup(ICluster cluster)
        {
            if (cluster != AbstractCluster.this)
                throw new IllegalArgumentException("Only the owning cluster can be used for startup");
            if (isRunning())
                throw new IllegalStateException("Can not start a instance that is already running");
            isShutdown = false;
            if (!broadcastAddress.equals(config.broadcastAddress()))
            {
                // previous address != desired address, so cleanup
                InetSocketAddress previous = broadcastAddress;
                InetSocketAddress newAddress = config.broadcastAddress();
                instanceMap.put(newAddress, (I) this); // if the broadcast address changes, update
                instanceMap.remove(previous);
                broadcastAddress = newAddress;
            }
            try
            {
                delegateForStartup().startup(cluster);
            }
            catch (Throwable t)
            {
                if (config.get(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN) == null)
                {
                    // its possible that the failure happens after listening and threads are started up
                    // but without knowing the start up phase it isn't safe to call shutdown, so assume
                    // that a failed to start instance was shutdown (which would be true if each instance
                    // was its own JVM).
                    isShutdown = true;
                }
                else
                {
                    // user was explict about the desired behavior, respect it
                    // the most common reason to set this is to set 'false', this will leave the
                    // instance marked as running, which will have .close shut it down.
                    isShutdown = (boolean) config.get(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN);
                }
                throw t;
            }
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
            if (isShutdown())
                throw new IllegalStateException("Instance is not running, so can not be shutdown");
            isShutdown = true;
            Future<Void> future = delegate.shutdown(graceful);
            delegate = null;
            return future;
        }

        public int liveMemberCount()
        {
            if (isRunning() && delegate != null)
                return delegate().liveMemberCount();

            throw new IllegalStateException("Cannot get live member count on shutdown instance: " + config.num());
        }

        public Metrics metrics()
        {
            if (isShutdown)
                throw new IllegalStateException();

            return delegate.metrics();
        }

        public NodeToolResult nodetoolResult(boolean withNotifications, String... commandAndArgs)
        {
            return delegate().nodetoolResult(withNotifications, commandAndArgs);
        }

        public long killAttempts()
        {
            IInvokableInstance local = delegate;
            // if shutdown cleared the delegate, then no longer know how many kill attempts happened, so return -1
            if (local == null)
                return -1;
            return local.killAttempts();
        }

        @Override
        public void receiveMessage(IMessage message)
        {
            IInvokableInstance delegate = this.delegate;
            if (isRunning() && delegate != null) // since we sync directly on the other node, we drop messages immediately if we are shutdown
                delegate.receiveMessage(message);
        }

        @Override
        public boolean getLogsEnabled()
        {
            return delegate().getLogsEnabled();
        }

        @Override
        public LogAction logs()
        {
            return delegate().logs();
        }

        @Override
        public synchronized void setVersion(Versions.Version version)
        {
            if (isRunning())
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

        @Override
        public void uncaughtException(Thread thread, Throwable throwable)
        {
            IInvokableInstance delegate = this.delegate;
            if (delegate != null)
                delegate.uncaughtException(thread, throwable);
            else
                logger.error("uncaught exception in thread {}", thread, throwable);
        }

        @Override
        public String toString()
        {
            IInvokableInstance delegate = this.delegate;
            return delegate == null ? "node" + config.num() : delegate.toString();
        }
    }

    protected AbstractCluster(AbstractBuilder<I, ? extends ICluster<I>, ?> builder)
    {
        this.root = builder.getRoot();
        this.sharedClassLoader = builder.getSharedClassLoader();
        this.subnet = builder.getSubnet();
        this.tokenSupplier = builder.getTokenSupplier();
        this.nodeIdTopology = builder.getNodeIdTopology();
        this.configUpdater = builder.getConfigUpdater();
        this.broadcastPort = builder.getBroadcastPort();
        this.nodeProvisionStrategy = builder.nodeProvisionStrategy;
        this.instances = new ArrayList<>();
        this.instanceMap = new ConcurrentHashMap<>();
        this.initialVersion = builder.getVersion();
        this.filters = new MessageFilters();
        this.instanceInitializer = builder.getInstanceInitializer();
        this.datadirCount = builder.getDatadirCount();

        int generation = GENERATION.incrementAndGet();
        for (int i = 0; i < builder.getNodeCount(); ++i)
        {
            int nodeNum = i + 1;
            InstanceConfig config = createInstanceConfig(nodeNum);

            I instance = newInstanceWrapperInternal(generation, initialVersion, config);
            instances.add(instance);
            // we use the config().broadcastAddressAndPort() here because we have not initialised the Instance
            I prev = instanceMap.put(instance.config().broadcastAddress(), instance);
            if (null != prev)
                throw new IllegalStateException("Cluster cannot have multiple nodes with same InetAddressAndPort: " + instance.broadcastAddress() + " vs " + prev.broadcastAddress());
        }
    }

    public InstanceConfig newInstanceConfig()
    {
        return createInstanceConfig(size() + 1);
    }

    private InstanceConfig createInstanceConfig(int nodeNum)
    {
        INodeProvisionStrategy provisionStrategy = nodeProvisionStrategy.create(subnet);
        long token = tokenSupplier.token(nodeNum);
        NetworkTopology topology = buildNetworkTopology(provisionStrategy, nodeIdTopology);
        InstanceConfig config = InstanceConfig.generate(nodeNum, provisionStrategy, topology, root, Long.toString(token), datadirCount);
        config.set(Constants.KEY_DTEST_API_CLUSTER_ID, clusterId.toString());
        if (configUpdater != null)
            configUpdater.accept(config);
        return config;
    }

    public static NetworkTopology buildNetworkTopology(INodeProvisionStrategy provisionStrategy,
                                                       Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology)
    {
        NetworkTopology topology = NetworkTopology.build("", 0, Collections.emptyMap());

        IntStream.rangeClosed(1, nodeIdTopology.size()).forEach(nodeId -> {
            InetSocketAddress addressAndPort = addressAndPort(provisionStrategy.ipAddress(nodeId), provisionStrategy.storagePort(nodeId));
            NetworkTopology.DcAndRack dcAndRack = nodeIdTopology.get(nodeId);
            topology.put(addressAndPort, dcAndRack);
        });
        return topology;
    }


    protected abstract I newInstanceWrapper(int generation, Versions.Version version, IInstanceConfig config);

    protected I newInstanceWrapperInternal(int generation, Versions.Version version, IInstanceConfig config)
    {
        config.validate();
        return newInstanceWrapper(generation, version, config);
    }

    public I bootstrap(IInstanceConfig config)
    {
        I instance = newInstanceWrapperInternal(0, initialVersion, config);
        instances.add(instance);
        I prev = instanceMap.put(config.broadcastAddress(), instance);

        if (null != prev)
        {
            throw new IllegalStateException(String.format("This cluster already contains a node (%d) with with same address and port: %s",
                                                          config.num(),
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

    public Stream<ICoordinator> coordinators()
    {
        return stream().map(IInstance::coordinator);
    }

    /**
     * WARNING: we index from 1 here, for consistency with inet address!
     */
    public I get(int node)
    {
        return instances.get(node - 1);
    }

    public I get(InetSocketAddress addr)
    {
        return instanceMap.get(addr);
    }

    public I getFirstRunningInstance()
    {
        return stream().filter(i -> !i.isShutdown()).findFirst().orElseThrow(
            () -> new IllegalStateException("All instances are shutdown"));
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

    public void run(Consumer<? super I> action,  Predicate<I> filter)
    {
        run(Collections.singletonList(action), filter);
    }

    public void run(Collection<Consumer<? super I>> actions, Predicate<I> filter)
    {
        stream().forEach(instance -> {
            for (Consumer<? super I> action : actions)
            {
                if (filter.test(instance))
                    action.accept(instance);
            }

        });
    }

    public void run(Consumer<? super I> action, int instanceId, int... moreInstanceIds)
    {
        run(Collections.singletonList(action), instanceId, moreInstanceIds);
    }

    public void run(List<Consumer<? super I>> actions, int instanceId, int... moreInstanceIds)
    {
        int[] instanceIds = new int[moreInstanceIds.length + 1];
        instanceIds[0] = instanceId;
        System.arraycopy(moreInstanceIds, 0, instanceIds, 1, moreInstanceIds.length);

        for (int idx : instanceIds)
        {
            for (Consumer<? super I> action : actions)
                action.accept(this.get(idx));
        }
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

    public IMessageFilters.Builder verbs(Verb... verbs)
    {
        int[] ids = new int[verbs.length];
        for (int i = 0; i < verbs.length; ++i)
            ids[i] = verbs[i].id;
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
        schemaChange(query, false);
    }

    /**
     * Change the schema of the cluster, tolerating stopped nodes.  N.B. the schema
     * will not automatically be updated when stopped nodes are restarted, individual tests need to
     * re-synchronize somehow (by gossip or some other mechanism).
     * @param query Schema altering statement
     */
    public void schemaChangeIgnoringStoppedInstances(String query)
    {
        schemaChange(query, true);
    }

    private void schemaChange(String query, boolean ignoreStoppedInstances)
    {
        I instance = ignoreStoppedInstances ? getFirstRunningInstance() : get(1);
        schemaChange(query, ignoreStoppedInstances, instance);
    }

    public void schemaChange(String query, boolean ignoreStoppedInstances, I instance)
    {
        instance.sync(() -> {
            try (SchemaChangeMonitor monitor = new SchemaChangeMonitor())
            {
                if (ignoreStoppedInstances)
                    monitor.ignoreStoppedInstances();
                monitor.startPolling();

                // execute the schema change
                instance.coordinator().execute(query, ConsistencyLevel.ALL);
                monitor.waitForCompletion();
            }
        }).run();
    }

    public void schemaChange(String statement, int instance)
    {
        get(instance).schemaChangeInternal(statement);
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
                reportTo.setMessagingVersion(reportFrom.broadcastAddress(), minVersion);
            }
        }
    }

    public abstract class ChangeMonitor implements AutoCloseable
    {
        final List<IListen.Cancel> cleanup;
        final SimpleCondition completed;
        private final long timeOut;
        private final TimeUnit timeoutUnit;
        protected Predicate<IInstance> instanceFilter;
        volatile boolean initialized;

        public ChangeMonitor(long timeOut, TimeUnit timeoutUnit)
        {
            this.timeOut = timeOut;
            this.timeoutUnit = timeoutUnit;
            this.instanceFilter = i -> true;
            this.cleanup = new ArrayList<>(instances.size());
            this.completed = new SimpleCondition();
        }

        public void ignoreStoppedInstances()
        {
            instanceFilter = instanceFilter.and(i -> !i.isShutdown());
        }

        protected void signal()
        {
            if (initialized && !completed.isSignaled() && isCompleted())
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
            initialized = true;
            signal();
            try
            {
                if (!completed.await(timeOut, timeoutUnit))
                    throw new IllegalStateException(getMonitorTimeoutMessage());
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException("Caught exception while waiting for completion", e);
            }
        }

        protected void startPolling()
        {
            instances.stream().filter(instanceFilter).forEach(instance -> cleanup.add(startPolling(instance)));
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
            return 1 == instances.stream().filter(instanceFilter).map(IInstance::schemaVersion).distinct().count();
        }

        protected String getMonitorTimeoutMessage()
        {
            return String.format("Schema agreement not reached. Schema versions of the instances: %s",
                                 instances.stream().map(IInstance::schemaVersion).collect(Collectors.toList()));
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

    public void startup()
    {
        previousHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(this::uncaughtExceptions);
        try (AllMembersAliveMonitor monitor = new AllMembersAliveMonitor())
        {
            monitor.startPolling();

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

    private void uncaughtExceptions(Thread thread, Throwable error)
    {
        if (!(thread.getContextClassLoader() instanceof InstanceClassLoader))
        {
            Thread.UncaughtExceptionHandler handler = previousHandler;
            if (null != handler)
                handler.uncaughtException(thread, error);
            return;
        }

        InstanceClassLoader cl = (InstanceClassLoader) thread.getContextClassLoader();
        get(cl.getInstanceId()).uncaughtException(thread, error);

        BiPredicate<Integer, Throwable> ignore = ignoreUncaughtThrowable;
        I instance = get(cl.getInstanceId());
        if ((ignore == null || !ignore.test(cl.getInstanceId(), error)) && instance != null && !instance.isShutdown())
            uncaughtExceptions.add(error);
    }

    @Override
    public void setUncaughtExceptionsFilter(BiPredicate<Integer, Throwable> ignoreUncaughtThrowable)
    {
        this.ignoreUncaughtThrowable = ignoreUncaughtThrowable;
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
        if (root.exists())
            FileUtils.deleteRecursive(root);
        Thread.setDefaultUncaughtExceptionHandler(previousHandler);
        previousHandler = null;
        checkAndResetUncaughtExceptions();
        //checkForThreadLeaks();
        //withThreadLeakCheck(futures);
    }

    @Override
    public void checkAndResetUncaughtExceptions()
    {
        List<Throwable> drain = new ArrayList<>(uncaughtExceptions.size());
        uncaughtExceptions.removeIf(e -> {
            drain.add(e);
            return true;
        });
        if (!drain.isEmpty())
            throw new ShutdownException(drain);
    }

    private void checkForThreadLeaks()
    {
        //This is an alternate version of the thread leak check that just checks to see if any threads are still alive
        // with the context classloader.
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        threadSet.stream().filter(t->t.getContextClassLoader() instanceof InstanceClassLoader).forEach(t->{
            t.setContextClassLoader(null);
            throw new RuntimeException("Unterminated thread detected " + t.getName() + " in group " + t.getThreadGroup().getName());
        });
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

    public List<Token> tokens()
    {
        return stream()
               .map(i ->
                    {
                        try
                        {
                            IPartitioner partitioner = ((IPartitioner)Class.forName(i.config().getString("partitioner")).newInstance());
                            return partitioner.getTokenFactory().fromString(i.config().getString("initial_token"));
                        }
                        catch (Throwable t)
                        {
                            throw new RuntimeException(t);
                        }
                    })
               .collect(Collectors.toList());
    }

    private static Set<String> findClassesMarkedForSharedClassLoader()
    {
        return findClassesMarkedWith(Shared.class);
    }

    private static Set<String> findClassesMarkedForInstanceClassLoader()
    {
        return findClassesMarkedWith(Isolated.class);
    }

    private static Set<String> findClassesMarkedWith(Class<? extends Annotation> annotation)
    {
        return new Reflections(ConfigurationBuilder.build("org.apache.cassandra").setExpandSuperTypes(false))
               .getTypesAnnotatedWith(annotation).stream()
               .map(Class::getName)
               .collect(Collectors.toSet());
    }
}

