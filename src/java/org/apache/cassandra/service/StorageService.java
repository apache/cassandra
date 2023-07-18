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
package org.apache.cassandra.service;

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.management.ListenerNotFoundException;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthSchemaChangeListener;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.concurrent.FutureTaskWithResources;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.PaxosStatePurging;
import org.apache.cassandra.config.Converters;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.SnapshotDetailsTabularData;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OwnedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.dht.StreamStateStore;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.fql.FullQueryLogger;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.fql.FullQueryLoggerOptionsCompositeData;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.locator.SystemReplicas;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.metrics.SamplingManager;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupLocalCoordinator;
import org.apache.cassandra.service.paxos.cleanup.PaxosTableRepairs;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.compatibility.GossipHelper;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.migration.GossipCMSListener;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.transformations.Assassinate;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.tcm.transformations.cms.EntireRange;
import org.apache.cassandra.tcm.transformations.cms.RemoveFromCMS;
import org.apache.cassandra.transport.ClientResourceLimits;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.logging.LoggingSupportFactory;
import org.apache.cassandra.utils.progress.ProgressListener;
import org.apache.cassandra.utils.progress.jmx.JMXBroadcastExecutor;
import org.apache.cassandra.utils.progress.jmx.JMXProgressSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONSISTENT_RANGE_MOVEMENT;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONSISTENT_SIMULTANEOUS_MOVES_ALLOW;
import static org.apache.cassandra.config.CassandraRelevantProperties.DRAIN_EXECUTOR_TIMEOUT_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_RETRIES;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_RETRY_DELAY_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_WRITE_SURVEY;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.index.SecondaryIndexManager.getIndexName;
import static org.apache.cassandra.index.SecondaryIndexManager.isIndexColumnFamily;
import static org.apache.cassandra.schema.SchemaConstants.isLocalSystemKeyspace;
import static org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import static org.apache.cassandra.service.ActiveRepairService.repairCommandExecutor;
import static org.apache.cassandra.service.StorageService.Mode.DECOMMISSIONED;
import static org.apache.cassandra.service.StorageService.Mode.DECOMMISSION_FAILED;
import static org.apache.cassandra.service.StorageService.Mode.JOINING_FAILED;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.tcm.compatibility.TokenRingUtils.getAllRanges;
import static org.apache.cassandra.tcm.compatibility.TokenRingUtils.getPrimaryRangeForEndpointWithinDC;
import static org.apache.cassandra.tcm.compatibility.TokenRingUtils.getPrimaryRangesForEndpoint;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_JOIN;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_REPLACE;
import static org.apache.cassandra.tcm.membership.NodeState.BOOTSTRAPPING;
import static org.apache.cassandra.tcm.membership.NodeState.BOOT_REPLACING;
import static org.apache.cassandra.tcm.membership.NodeState.JOINED;
import static org.apache.cassandra.tcm.membership.NodeState.LEAVING;
import static org.apache.cassandra.tcm.membership.NodeState.MOVING;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.FBUtilities.now;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService extends NotificationBroadcasterSupport implements IEndpointStateChangeSubscriber, StorageServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    public static final int INDEFINITE = -1;
    public static final int RING_DELAY_MILLIS = getRingDelay(); // delay after which we assume ring has stablized

    private final JMXProgressSupport progressSupport = new JMXProgressSupport(this);
    private final AtomicReference<BootStrapper> ongoingBootstrap = new AtomicReference<>();

    private static int getRingDelay()
    {
        String newdelay = CassandraRelevantProperties.RING_DELAY.getString();
        if (newdelay != null)
        {
            logger.info("Overriding RING_DELAY to {}ms", newdelay);
            return Integer.parseInt(newdelay);
        }
        else
        {
            return 30 * 1000;
        }
    }

    public volatile VersionedValue.VersionedValueFactory valueFactory =
    new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

    private Thread drainOnShutdown = null;
    private volatile boolean isShutdown = false;
    private final List<Runnable> preShutdownHooks = new ArrayList<>();
    private final List<Runnable> postShutdownHooks = new ArrayList<>();

    public final SnapshotManager snapshotManager = new SnapshotManager();

    public static final StorageService instance = new StorageService();

    private final SamplingManager samplingManager = new SamplingManager();

    @VisibleForTesting // this is used for dtests only, see CASSANDRA-18152
    public volatile boolean skipNotificationListeners = false;

    private final java.util.function.Predicate<Keyspace> anyOutOfRangeOpsRecorded
    = keyspace -> keyspace.metric.outOfRangeTokenReads.getCount() > 0
                  || keyspace.metric.outOfRangeTokenWrites.getCount() > 0
                  || keyspace.metric.outOfRangeTokenPaxosRequests.getCount() > 0;

    private long[] getOutOfRangeOperationCounts(Keyspace keyspace)
    {
        return new long[]
               {
               keyspace.metric.outOfRangeTokenReads.getCount(),
               keyspace.metric.outOfRangeTokenWrites.getCount(),
               keyspace.metric.outOfRangeTokenPaxosRequests.getCount()
               };
    }

    public Map<String, long[]> getOutOfRangeOperationCounts()
    {
        return Schema.instance.getKeyspaces()
                              .stream()
                              .map(Keyspace::open)
                              .filter(anyOutOfRangeOpsRecorded)
                              .collect(Collectors.toMap(Keyspace::getName, this::getOutOfRangeOperationCounts));
    }

    public void incOutOfRangeOperationCount()
    {
        (isStarting() ? StorageMetrics.startupOpsForInvalidToken : StorageMetrics.totalOpsForInvalidToken).inc();
    }

    @Deprecated
    public boolean isInShutdownHook()
    {
        return isShutdown();
    }

    public boolean isShutdown()
    {
        return isShutdown;
    }

    /**
     * for in-jvm dtest use - forces isShutdown to be set to whatever passed in.
     */
    @VisibleForTesting
    public void setIsShutdownUnsafeForTests(boolean isShutdown)
    {
        this.isShutdown = isShutdown;
    }

    public RangesAtEndpoint getLocalReplicas(String keyspaceName)
    {
        return getReplicas(keyspaceName, FBUtilities.getBroadcastAddressAndPort());
    }

    public RangesAtEndpoint getReplicas(String keyspaceName, InetAddressAndPort endpoint)
    {
        return Keyspace.open(keyspaceName).getReplicationStrategy()
                       .getAddressReplicas(ClusterMetadata.current(), getBroadcastAddressAndPort());
    }

    public List<Range<Token>> getLocalRanges(String ks)
    {
        InetAddressAndPort broadcastAddress = getBroadcastAddressAndPort();
        Keyspace keyspace = Keyspace.open(ks);
        List<Range<Token>> ranges = new ArrayList<>();
        for (Replica r : keyspace.getReplicationStrategy().getAddressReplicas(ClusterMetadata.current(),
                                                                              broadcastAddress))
            ranges.add(r.range());
        return ranges;
    }

    public Collection<Range<Token>> getLocalAndPendingRanges(String ks)
    {
        return ClusterMetadata.current().localWriteRanges(Keyspace.open(ks).getMetadata());
    }

    public OwnedRanges getNormalizedLocalRanges(String keyspaceName)
    {
        return new OwnedRanges(getLocalReplicas(keyspaceName).ranges());
    }

    public Collection<Range<Token>> getPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, getBroadcastAddressAndPort());
    }

    public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace)
    {
        return getPrimaryRangeForEndpointWithinDC(keyspace, getBroadcastAddressAndPort());
    }

    private final Set<InetAddressAndPort> replicatingNodes = Sets.newConcurrentHashSet();
    private CassandraDaemon daemon;

    private InetAddressAndPort removingNode;

    /* we bootstrap but do NOT join the ring unless told to do so */
    private boolean isSurveyMode = TEST_WRITE_SURVEY.getBoolean(false);
    /* true if node is rebuilding and receiving data */
    private volatile boolean initialized = false;
    private final AtomicBoolean authSetupCalled = new AtomicBoolean(false);
    private volatile boolean authSetupComplete = false;

    /* the probability for tracing any particular request, 0 disables tracing and 1 enables for all */
    private double traceProbability = 0.0;

    public enum Mode { STARTING, NORMAL, JOINING, JOINING_FAILED, LEAVING, DECOMMISSIONED, DECOMMISSION_FAILED, MOVING, DRAINING, DRAINED }
    private volatile Mode operationMode = Mode.STARTING;

    /* Can currently hold DECOMMISSIONED, DECOMMISSION_FAILED, DRAINING, DRAINED for legacy compatibility. */
    private volatile Optional<Mode> transientMode = Optional.empty();

    private volatile int totalCFs, remainingCFs;

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();

    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    private final String jmxObjectName;

    // true when keeping strict consistency while bootstrapping
    public static final boolean useStrictConsistency = CONSISTENT_RANGE_MOVEMENT.getBoolean();
    private static final boolean allowSimultaneousMoves = CONSISTENT_SIMULTANEOUS_MOVES_ALLOW.getBoolean();
    private static final boolean joinRing = JOIN_RING.getBoolean();

    final StreamStateStore streamStateStore = new StreamStateStore();

    public final SSTablesGlobalTracker sstablesTracker;

    public boolean isSurveyMode()
    {
        return isSurveyMode;
    }

    public StorageService()
    {
        // use dedicated executor for handling JMX notifications
        super(JMXBroadcastExecutor.executor);

        jmxObjectName = "org.apache.cassandra.db:type=StorageService";

        sstablesTracker = new SSTablesGlobalTracker(DatabaseDescriptor.getSelectedSSTableFormat());
    }

    private void registerMBeans()
    {
        MBeanWrapper.instance.registerMBean(this, jmxObjectName);
        MBeanWrapper.instance.registerMBean(StreamManager.instance, StreamManager.OBJECT_NAME);
    }

    public void registerDaemon(CassandraDaemon daemon)
    {
        this.daemon = daemon;
    }

    public void register(IEndpointLifecycleSubscriber subscriber)
    {
        lifecycleSubscribers.add(subscriber);
    }

    public void unregister(IEndpointLifecycleSubscriber subscriber)
    {
        lifecycleSubscribers.remove(subscriber);
    }

    // should only be called via JMX
    public void stopGossiping()
    {
        if (isGossipRunning())
        {
            if (!isNormal() && joinRing)
                throw new IllegalStateException("Unable to stop gossip because the node is not in the normal state. Try to stop the node instead.");

            logger.warn("Stopping gossip by operator request");

            if (isNativeTransportRunning())
            {
                logger.warn("Disabling gossip while native transport is still active is unsafe");
            }

            Gossiper.instance.stop();
        }
    }

    // should only be called via JMX
    public synchronized void startGossiping()
    {
        if (!isGossipRunning())
        {
            checkServiceAllowedToStart("gossip");

            logger.warn("Starting gossip by operator request");
            Collection<Token> tokens = SystemKeyspace.getSavedTokens();

            boolean validTokens = tokens != null && !tokens.isEmpty();

            // shouldn't be called before these are set if we intend to join the ring/are in the process of doing so
            if (!isStarting() || joinRing)
                assert validTokens : "Cannot start gossiping for a node intended to join without valid tokens";

            List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>();
            states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
            states.add(Pair.create(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(tokens)));
            states.add(Pair.create(ApplicationState.STATUS, valueFactory.normal(tokens)));
            logger.info("Node {} jump to NORMAL", getBroadcastAddressAndPort());
            Gossiper.instance.addLocalApplicationStates(states);
            Gossiper.instance.forceNewerGeneration();
            Gossiper.instance.start((int) (currentTimeMillis() / 1000), true);
        }
    }

    // should only be called via JMX
    public boolean isGossipRunning()
    {
        return Gossiper.instance.isEnabled();
    }

    public synchronized void startNativeTransport()
    {
        checkServiceAllowedToStart("native transport");

        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }

        try
        {
            daemon.startNativeTransport();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error starting native transport: " + e.getMessage());
        }
    }

    public void stopNativeTransport()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        daemon.stopNativeTransport();
    }

    public boolean isNativeTransportRunning()
    {
        if (daemon == null)
        {
            return false;
        }
        return daemon.isNativeTransportRunning();
    }

    @Override
    public void enableNativeTransportOldProtocolVersions()
    {
        DatabaseDescriptor.setNativeTransportAllowOlderProtocols(true);
    }

    @Override
    public void disableNativeTransportOldProtocolVersions()
    {
        DatabaseDescriptor.setNativeTransportAllowOlderProtocols(false);
    }

    public void stopTransports()
    {
        if (isNativeTransportRunning())
        {
            logger.error("Stopping native transport");
            stopNativeTransport();
        }
        if (isGossipActive())
        {
            logger.error("Stopping gossiper");
            stopGossiping();
        }
    }

    /**
     * Set the Gossip flag RPC_READY to false and then
     * shutdown the client services (thrift and CQL).
     * <p>
     * Note that other nodes will do this for us when
     * they get the Gossip shutdown message, so even if
     * we don't get time to broadcast this, it is not a problem.
     *
     * See {@code Gossiper#markAsShutdown(InetAddressAndPort)}
     */
    private void shutdownClientServers()
    {
        setRpcReady(false);
        stopNativeTransport();
    }

    public void stopClient()
    {
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
        // give it a second so that task accepted before the MessagingService shutdown gets submitted to the stage (to avoid RejectedExecutionException)
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        Stage.shutdownNow();
    }

    /**
     * Only used in jvm dtest when not using GOSSIP.
     * See org.apache.cassandra.distributed.impl.Instance#startup(org.apache.cassandra.distributed.api.ICluster)
     */
    public void unsafeSetInitialized()
    {
        initialized = true;
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    @Deprecated
    public boolean isGossipActive()
    {
        return isGossipRunning();
    }

    public boolean isDaemonSetupCompleted()
    {
        return daemon != null && daemon.setupCompleted();
    }

    public void stopDaemon()
    {
        if (daemon == null)
            throw new IllegalStateException("No configured daemon");
        daemon.deactivate();
    }

    // for testing only
    public void unsafeInitialize() throws ConfigurationException
    {
        initialized = true;
        Gossiper.instance.register(this);
        Gossiper.instance.start((int) (currentTimeMillis() / 1000)); // needed for node-ring gathering.
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        MessagingService.instance().listen();
    }

    public synchronized void initServer() throws ConfigurationException
    {
        logger.info("Cassandra version: {}", FBUtilities.getReleaseVersionString());
        logger.info("Git SHA: {}", FBUtilities.getGitSHA());
        logger.info("CQL version: {}", QueryProcessor.CQL_VERSION);
        logger.info("Native protocol supported versions: {} (default: {})",
                    StringUtils.join(ProtocolVersion.supportedVersions(), ", "), ProtocolVersion.CURRENT);

        try
        {
            // Ensure StorageProxy is initialized on start-up; see CASSANDRA-3797.
            Class.forName("org.apache.cassandra.service.StorageProxy");
            // also IndexSummaryManager, which is otherwise unreferenced
            Class.forName("org.apache.cassandra.io.sstable.indexsummary.IndexSummaryManager");
        }
        catch (ClassNotFoundException e)
        {
            throw new AssertionError(e);
        }

        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        drainOnShutdown = NamedThreadFactory.createThread(new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException, ExecutionException, IOException
            {
                drain(true);
                try
                {
                    ExecutorUtils.shutdownNowAndWait(1, MINUTES, ScheduledExecutors.scheduledFastTasks);
                }
                catch (Throwable t)
                {
                    logger.warn("Unable to terminate fast tasks within 1 minute.", t);
                }
                finally
                {
                    LoggingSupportFactory.getLoggingSupport().onShutdown();
                }
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);

        Schema.instance.saveSystemKeyspace();
        DatabaseDescriptor.getInternodeAuthenticator().setupInternode();
        try
        {
            MessagingService.instance().waitUntilListening();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Could not finish waiting until listening", e);
        }

        if (ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
        {
            // register listener before starting gossiper to avoid missing messages
            Gossiper.instance.register(new GossipCMSListener());
        }
        sstablesTracker.register((notification, o) -> {
            if (!(notification instanceof SSTablesVersionsInUseChangeNotification))
                return;

            Set<Version> versions = ((SSTablesVersionsInUseChangeNotification) notification).versionsInUse;
            logger.debug("Updating local sstables version in Gossip to {}", versions);

            Gossiper.instance.addLocalApplicationState(ApplicationState.SSTABLE_VERSIONS,
                                                       valueFactory.sstableVersions(versions));
        });

        Gossiper.instance.start(SystemKeyspace.incrementAndGetGeneration(),
                                ClusterMetadataService.state() != ClusterMetadataService.State.GOSSIP); // only populate local state if not running in gossip mode
        Gossiper.instance.register(this);
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        Gossiper.instance.addLocalApplicationState(ApplicationState.SSTABLE_VERSIONS,
                                                   valueFactory.sstableVersions(sstablesTracker.versionsInUse()));

        if (SystemKeyspace.wasDecommissioned())
            throw new ConfigurationException("This node was decommissioned and will not rejoin the ring unless cassandra.override_decommission=true has been set, or all existing data is removed and the node is bootstrapped again");

        if (DatabaseDescriptor.getReplaceTokens().size() > 0 || DatabaseDescriptor.getReplaceNode() != null)
            throw new RuntimeException("Replace method removed; use cassandra.replace_address instead");

        if (ClusterMetadataService.state() == ClusterMetadataService.State.REMOTE)
            Gossiper.instance.triggerRoundWithCMS();

        Gossiper.waitToSettle();

        NodeId self = Register.maybeRegister();

        try
        {
            joinRing();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not perform startup sequence and join cluster", e);
        }
    }

    // todo: move somewhere to sequences?
    @VisibleForTesting
    public void startup(Supplier<Transformation> startupSequence, boolean finishJoiningRing)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();

        // finish in-progress sequences first
        finishInProgressSequences(self);
        metadata = ClusterMetadata.current();

        switch (metadata.directory.peerState(self))
        {
            case REGISTERED:
            case LEFT:
                if (isReplacing())
                    maybeHandoverCMS(metadata, DatabaseDescriptor.getReplaceAddress());

                ClusterMetadataService.instance().commit(startupSequence.get(),
                                                         (metadata_) -> null,
                                                         (metadata_, code, reason) -> {
                                                             InProgressSequence<?> sequence = metadata_.inProgressSequences.get(self);
                                                             // We might have discovered a startup sequence we ourselves committed but got no response for
                                                             if (sequence == null || !InProgressSequences.STARTUP_SEQUENCE_KINDS.contains(sequence.kind()))
                                                             {
                                                                 throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting startup sequence.",
                                                                                                               reason));
                                                             }
                                                             return null;
                                                         });


                finishInProgressSequences(self);

                if (finishJoiningRing && (ClusterMetadata.current().directory.peerState(self) != JOINED))
                    throw new IllegalStateException("Did not finish joining the ring. Please check logs for details.");
                else
                {
                    logger.info("Did not finish joining the ring; node state is {}, bootstrap state is {}",
                                ClusterMetadata.current().directory.peerState(self),
                                SystemKeyspace.getBootstrapState());
                    break;
                }
            case JOINED:
                // JOINED appears before BOOTSTRAPPING & BOOT_REPLACE so we can fall
                // through when we start as REGISTERED/LEFT and complete a full startup
                logger.info("{}", Mode.NORMAL);
                break;
            case BOOTSTRAPPING:
            case BOOT_REPLACING:
                if (finishJoiningRing)
                {
                    throw new IllegalStateException("Expected to complete startup sequence, but did not. " +
                                                    "Can't proceed from the state " + metadata.directory.peerState(self));
                }
                break;
            default:
                throw new IllegalStateException("Can't proceed from the state " + metadata.directory.peerState(self));
        }
    }

    private void completeInitialization()
    {
        if (!initialized)
            registerMBeans();
        initialized = true;
    }

    private void finishInProgressSequences(NodeId self)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        while (true)
        {
            InProgressSequence<?> sequence = metadata.inProgressSequences.get(self);
            if (sequence == null)
                break;
            if (InProgressSequences.isLeave(sequence))
                maybeInitializeServices();
            if (InProgressSequences.resume(sequence))
                metadata = ClusterMetadata.current();
            else
                return;
        }
    }

    public boolean cancelInProgressSequences(String sequenceOwner, String expectedSequenceKind)
    {
        NodeId owner = NodeId.fromString(sequenceOwner);
        InProgressSequence<?> seq = ClusterMetadata.current().inProgressSequences.get(owner);
        if (seq == null)
            throw new IllegalArgumentException("No in progress sequence for "+sequenceOwner);
        InProgressSequences.Kind expectedKind = InProgressSequences.Kind.valueOf(expectedSequenceKind);
        if (seq.kind() != expectedKind)
            throw new IllegalArgumentException("No in progress sequence of kind " + expectedKind + " for " + owner + " (only " + seq.kind() +" in progress)");

        return cancelInProgressSequences(owner);
    }

    public boolean cancelInProgressSequences(NodeId sequenceOwner)
    {
        return ClusterMetadataService.instance().commit(new CancelInProgressSequence(sequenceOwner),
                                                        metadata -> true,
                                                        (metadata, code, message) -> {
                                                            // Succeeded following rejection; possibly we raced with another cancellation request
                                                            // or our initial attempt succeeded but the response was lost
                                                            if (!metadata.inProgressSequences.contains(sequenceOwner))
                                                                return true;

                                                            logger.warn(String.format("Could not cancel in-progress sequence: %s", message));
                                                            return false;
                                                        });
    }

    private boolean servicesInitialized = false;
    private void maybeInitializeServices()
    {
        if (servicesInitialized)
            return;

        StorageProxy.instance.initialLoadPartitionDenylist();
        LoadBroadcaster.instance.startBroadcasting();
        HintsService.instance.startDispatch();
        BatchlogManager.instance.start();
        startSnapshotManager();
        servicesInitialized = true;
    }

    private Transformation getStartupSequence(boolean finishJoiningRing, boolean shouldBootstrap)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (isReplacing())
        {
            if (SystemKeyspace.bootstrapComplete())
                throw new RuntimeException("Cannot replace with a node that is already bootstrapped");

            if (!shouldBootstrap() && !CassandraRelevantProperties.ALLOW_UNSAFE_REPLACE.getBoolean())
            {
                throw new RuntimeException("Replacing a node without bootstrapping risks invalidating consistency " +
                                           "guarantees as the expected data may not be present until repair is run. " +
                                           "To perform this operation, please restart with " +
                                           "-Dcassandra.allow_unsafe_replace=true");
            }

            InetAddressAndPort replacingEndpoint = DatabaseDescriptor.getReplaceAddress();

            NodeId replaced = ClusterMetadata.current().directory.peerId(replacingEndpoint);

            return new PrepareReplace(replaced,
                                      metadata.myNodeId(),
                                      ClusterMetadataService.instance().placementProvider(),
                                      finishJoiningRing,
                                      shouldBootstrap);
        }
        else if (finishJoiningRing && !shouldBootstrap)
        {
            return new UnsafeJoin(metadata.myNodeId(),
                                  new HashSet<>(BootStrapper.getBootstrapTokens(ClusterMetadata.current(), getBroadcastAddressAndPort())),
                                  ClusterMetadataService.instance().placementProvider());
        }
        else
        {
            return new PrepareJoin(metadata.myNodeId(),
                                   // TODO: use these tokens when setting up progess sequence
                                   new HashSet<>(BootStrapper.getBootstrapTokens(ClusterMetadata.current(), getBroadcastAddressAndPort())),
                                   ClusterMetadataService.instance().placementProvider(),
                                   finishJoiningRing,
                                   shouldBootstrap);
        }
    }

    public boolean isReplacing()
    {

        if (REPLACE_ADDRESS_FIRST_BOOT.getString() != null && SystemKeyspace.bootstrapComplete())
        {
            logger.info("Replace address on the first boot requested; this node is already bootstrapped");
            return false;
        }

        return DatabaseDescriptor.getReplaceAddress() != null;
    }

    /**
     * In the event of forceful termination we need to remove the shutdown hook to prevent hanging (OOM for instance)
     */
    public void removeShutdownHook()
    {
        PathUtils.clearOnExitThreads();

        if (drainOnShutdown != null)
            Runtime.getRuntime().removeShutdownHook(drainOnShutdown);
    }

    private boolean shouldBootstrap()
    {
        return DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && !isSeed();
    }

    public static boolean isSeed()
    {
        return DatabaseDescriptor.getSeeds().contains(getBroadcastAddressAndPort());
    }

    @VisibleForTesting
    public void startSnapshotManager()
    {
        snapshotManager.start();
    }

     public static boolean isReplacingSameAddress()
    {
        InetAddressAndPort replaceAddress = DatabaseDescriptor.getReplaceAddress();
        return replaceAddress != null && replaceAddress.equals(getBroadcastAddressAndPort());
    }

    public synchronized void joinRing() throws IOException
    {
        if (isStarting())
        {
            if (!joinRing)
                logger.info("Joining ring by operator request");
            try
            {
                boolean finishJoiningRing = !isSurveyMode;
                startup(() -> getStartupSequence(finishJoiningRing, shouldBootstrap()), finishJoiningRing);
            }
            catch (ConfigurationException e)
            {
                throw new IOException(e.getMessage());
            }
        }
        else if (isSurveyMode)
        {
            // if isSurveyMode is on then verify the node is in the right state to join the ring
            if (readyToFinishJoiningRing())
            {
                logger.info("Leaving write survey mode and joining ring at operator request");
                finishJoiningRing();
                isSurveyMode = false;
                daemon.start();
            }
            else
            {
                logger.warn("Can't join the ring because in write_survey mode and bootstrap hasn't completed");
            }
        }
        else if (isBootstrapMode())
        {
            // bootstrap is not complete hence node cannot join the ring
            logger.warn("Can't join the ring because bootstrap hasn't completed.");
            return;
        }

        doAuthSetup();
        maybeInitializeServices();
        completeInitialization();
    }

    public boolean readyToFinishJoiningRing()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId id = metadata.myNodeId();
        InProgressSequence<?> sequence = metadata.inProgressSequences.get(id);

        if (sequence == null && metadata.directory.peerState(id) == JOINED)
            return true;
        if (sequence instanceof BootstrapAndJoin && sequence.nextStep() == FINISH_JOIN)
            return true;
        if (sequence instanceof BootstrapAndReplace && sequence.nextStep() == FINISH_REPLACE)
            return true;

        return false;
    }

    public void finishJoiningRing()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId id = metadata.myNodeId();
        InProgressSequence<?> sequence = metadata.inProgressSequences.get(id);

        // Double check the conditions we verified in readyToFinishJoiningRing
        if (!(sequence instanceof BootstrapAndJoin) && !(sequence instanceof BootstrapAndReplace))
            throw new IllegalStateException("Can not finish joining ring as join sequence has not been started");

        if (sequence.nextStep() != FINISH_JOIN && sequence.nextStep() != FINISH_REPLACE)
            throw new IllegalStateException("Can not finish joining ring, sequence is in an incorrect state. " +
                                            "If no progress is made, cancel the join process for this node and retry");

        // create a new copy of the sequence with the finishJoining flag set to true, then complete it.
        // Note, this does not replace the existing sequence in ClusterMetadata, but it will remove it
        // as usual if execution is successful.
        boolean success = (sequence instanceof BootstrapAndJoin)
                          ? ((BootstrapAndJoin)sequence).finishJoiningRing().executeNext()
                          : ((BootstrapAndReplace)sequence).finishJoiningRing().executeNext();
        if (!success)
            throw new RuntimeException("Could not finish joining ring, restart this node and inflight operations will " +
                                       "attempt to complete. If no progress is made, cancel the join process for this node and retry");

    }

    @VisibleForTesting
    public void doAuthSetup()
    {
        if (!authSetupCalled.getAndSet(true))
        {
            DatabaseDescriptor.getRoleManager().setup();
            DatabaseDescriptor.getAuthenticator().setup();
            DatabaseDescriptor.getAuthorizer().setup();
            DatabaseDescriptor.getNetworkAuthorizer().setup();
            DatabaseDescriptor.getCIDRAuthorizer().setup();
            AuthCacheService.initializeAndRegisterCaches();
            Schema.instance.registerListener(new AuthSchemaChangeListener());
            authSetupComplete = true;
        }
    }

    public boolean isAuthSetupComplete()
    {
        return authSetupComplete;
    }

    @VisibleForTesting
    public boolean authSetupCalled()
    {
        return authSetupCalled.get();
    }

    public boolean isJoined()
    {
        return ClusterMetadata.current().directory.allAddresses().contains(getBroadcastAddressAndPort()) && !isSurveyMode;
    }

    public void rebuild(String sourceDc)
    {
        rebuild(sourceDc, null, null, null, false);
    }

    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources)
    {
        rebuild(sourceDc, keyspace, tokens, specificSources, false);
    }

    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources, boolean excludeLocalDatacenterNodes)
    {
        Rebuild.rebuild(sourceDc, keyspace, tokens, specificSources, excludeLocalDatacenterNodes);
    }

    public void setRpcTimeout(long value)
    {
        DatabaseDescriptor.setRpcTimeout(value);
        logger.info("set rpc timeout to {} ms", value);
    }

    public long getRpcTimeout()
    {
        return DatabaseDescriptor.getRpcTimeout(MILLISECONDS);
    }

    public void setReadRpcTimeout(long value)
    {
        DatabaseDescriptor.setReadRpcTimeout(value);
        logger.info("set read rpc timeout to {} ms", value);
    }

    public long getReadRpcTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS);
    }

    public void setRangeRpcTimeout(long value)
    {
        DatabaseDescriptor.setRangeRpcTimeout(value);
        logger.info("set range rpc timeout to {} ms", value);
    }

    public long getRangeRpcTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout(MILLISECONDS);
    }

    public void setWriteRpcTimeout(long value)
    {
        DatabaseDescriptor.setWriteRpcTimeout(value);
        logger.info("set write rpc timeout to {} ms", value);
    }

    public long getWriteRpcTimeout()
    {
        return DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
    }

    public void setInternodeTcpConnectTimeoutInMS(int value)
    {
        DatabaseDescriptor.setInternodeTcpConnectTimeoutInMS(value);
        logger.info("set internode tcp connect timeout to {} ms", value);
    }

    public int getInternodeTcpConnectTimeoutInMS()
    {
        return DatabaseDescriptor.getInternodeTcpConnectTimeoutInMS();
    }

    public void setInternodeTcpUserTimeoutInMS(int value)
    {
        DatabaseDescriptor.setInternodeTcpUserTimeoutInMS(value);
        logger.info("set internode tcp user timeout to {} ms", value);
    }

    public int getInternodeTcpUserTimeoutInMS()
    {
        return DatabaseDescriptor.getInternodeTcpUserTimeoutInMS();
    }

    public void setInternodeStreamingTcpUserTimeoutInMS(int value)
    {
        checkArgument(value >= 0, "TCP user timeout cannot be negative for internode streaming connection. Got %s", value);
        DatabaseDescriptor.setInternodeStreamingTcpUserTimeoutInMS(value);
        logger.info("set internode streaming tcp user timeout to {} ms", value);
    }

    public int getInternodeStreamingTcpUserTimeoutInMS()
    {
        return DatabaseDescriptor.getInternodeStreamingTcpUserTimeoutInMS();
    }

    public void setCounterWriteRpcTimeout(long value)
    {
        DatabaseDescriptor.setCounterWriteRpcTimeout(value);
        logger.info("set counter write rpc timeout to {} ms", value);
    }

    public long getCounterWriteRpcTimeout()
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS);
    }

    public void setCasContentionTimeout(long value)
    {
        DatabaseDescriptor.setCasContentionTimeout(value);
        logger.info("set cas contention rpc timeout to {} ms", value);
    }

    public long getCasContentionTimeout()
    {
        return DatabaseDescriptor.getCasContentionTimeout(MILLISECONDS);
    }

    public void setTruncateRpcTimeout(long value)
    {
        DatabaseDescriptor.setTruncateRpcTimeout(value);
        logger.info("set truncate rpc timeout to {} ms", value);
    }

    public long getTruncateRpcTimeout()
    {
        return DatabaseDescriptor.getTruncateRpcTimeout(MILLISECONDS);
    }

    @Deprecated
    public void setStreamThroughputMbPerSec(int value)
    {
        setStreamThroughputMbitPerSec(value);
    }

    public void setStreamThroughputMbitPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSecAsDouble();
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(value);
        StreamManager.StreamRateLimiter.updateThroughput();
        logger.info("setstreamthroughput: throttle set to {}{} megabits per second (was approximately {} megabits per second)",
                    value, value <= 0 ? " (unlimited)" : "", oldValue);
    }

    public void setStreamThroughputMebibytesPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getStreamThroughputOutboundMebibytesPerSec();
        DatabaseDescriptor.setStreamThroughputOutboundMebibytesPerSecAsInt(value);
        StreamManager.StreamRateLimiter.updateThroughput();
        logger.info("setstreamthroughput: throttle set to {}{} MiB/s (was {} MiB/s)", value, value <= 0 ? " (unlimited)" : "", oldValue);
    }

    public double getStreamThroughputMebibytesPerSecAsDouble()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMebibytesPerSec();
    }

    public int getStreamThroughputMebibytesPerSec()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMebibytesPerSecAsInt();
    }

    @Deprecated
    public int getStreamThroughputMbPerSec()
    {
        return getStreamThroughputMbitPerSec();
    }

    @Deprecated
    public int getStreamThroughputMbitPerSec()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
    }

    public double getStreamThroughputMbitPerSecAsDouble()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSecAsDouble();
    }

    public void setEntireSSTableStreamThroughputMebibytesPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundMebibytesPerSec();
        DatabaseDescriptor.setEntireSSTableStreamThroughputOutboundMebibytesPerSec(value);
        StreamManager.StreamRateLimiter.updateEntireSSTableThroughput();
        logger.info("setstreamthroughput (entire SSTable): throttle set to {}{} MiB/s (was {} MiB/s)",
                    value, value <= 0 ? " (unlimited)" : "", oldValue);
    }

    public double getEntireSSTableStreamThroughputMebibytesPerSecAsDouble()
    {
        return DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundMebibytesPerSec();
    }

    @Deprecated
    public void setInterDCStreamThroughputMbPerSec(int value)
    {
        setInterDCStreamThroughputMbitPerSec(value);
    }

    public void setInterDCStreamThroughputMbitPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSecAsDouble();
        DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(value);
        StreamManager.StreamRateLimiter.updateInterDCThroughput();
        logger.info("setinterdcstreamthroughput: throttle set to {}{} megabits per second (was {} megabits per second)", value, value <= 0 ? " (unlimited)" : "", oldValue);
    }

    @Deprecated
    public int getInterDCStreamThroughputMbPerSec()
    {
        return getInterDCStreamThroughputMbitPerSec();
    }

    @Deprecated
    public int getInterDCStreamThroughputMbitPerSec()
    {
        return DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec();
    }

    public double getInterDCStreamThroughputMbitPerSecAsDouble()
    {
        return DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSecAsDouble();
    }

    public void setInterDCStreamThroughputMebibytesPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getInterDCStreamThroughputOutboundMebibytesPerSec();
        DatabaseDescriptor.setInterDCStreamThroughputOutboundMebibytesPerSecAsInt(value);
        StreamManager.StreamRateLimiter.updateInterDCThroughput();
        logger.info("setinterdcstreamthroughput: throttle set to {}{} MiB/s (was {} MiB/s)", value, value <= 0 ? " (unlimited)" : "", oldValue);
    }

    public int getInterDCStreamThroughputMebibytesPerSec()
    {
        return DatabaseDescriptor.getInterDCStreamThroughputOutboundMebibytesPerSecAsInt();
    }

    public double getInterDCStreamThroughputMebibytesPerSecAsDouble()
    {
        return DatabaseDescriptor.getInterDCStreamThroughputOutboundMebibytesPerSec();
    }

    public void setEntireSSTableInterDCStreamThroughputMebibytesPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec();
        DatabaseDescriptor.setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(value);
        StreamManager.StreamRateLimiter.updateEntireSSTableInterDCThroughput();
        logger.info("setinterdcstreamthroughput (entire SSTable): throttle set to {}{} MiB/s (was {} MiB/s)", value, value <= 0 ? " (unlimited)" : "", oldValue);
    }

    public double getEntireSSTableInterDCStreamThroughputMebibytesPerSecAsDouble()
    {
        return DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec();
    }

    public double getCompactionThroughtputMibPerSecAsDouble()
    {
        return DatabaseDescriptor.getCompactionThroughputMebibytesPerSec();
    }

    public long getCompactionThroughtputBytesPerSec()
    {
        return (long)DatabaseDescriptor.getCompactionThroughputBytesPerSec();
    }

    @Deprecated
    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.getCompactionThroughputMebibytesPerSecAsInt();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        double oldValue = DatabaseDescriptor.getCompactionThroughputMebibytesPerSec();
        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(value);
        double valueInBytes = value * 1024.0 * 1024.0;
        CompactionManager.instance.setRateInBytes(valueInBytes);
        logger.info("compactionthroughput: throttle set to {} mebibytes per second (was {} mebibytes per second)",
                    value, oldValue);
    }

    public int getBatchlogReplayThrottleInKB()
    {
        return DatabaseDescriptor.getBatchlogReplayThrottleInKiB();
    }

    public void setBatchlogReplayThrottleInKB(int throttleInKB)
    {
        DatabaseDescriptor.setBatchlogReplayThrottleInKiB(throttleInKB);
        BatchlogManager.instance.setRate(throttleInKB);
    }

    public int getConcurrentCompactors()
    {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    public void setConcurrentCompactors(int value)
    {
        if (value <= 0)
            throw new IllegalArgumentException("Number of concurrent compactors should be greater than 0.");
        DatabaseDescriptor.setConcurrentCompactors(value);
        CompactionManager.instance.setConcurrentCompactors(value);
    }

    public void bypassConcurrentValidatorsLimit()
    {
        logger.info("Enabling the ability to set concurrent validations to an unlimited value");
        DatabaseDescriptor.allowUnlimitedConcurrentValidations = true ;
    }

    public void enforceConcurrentValidatorsLimit()
    {
        logger.info("Disabling the ability to set concurrent validations to an unlimited value");
        DatabaseDescriptor.allowUnlimitedConcurrentValidations = false ;
    }

    public boolean isConcurrentValidatorsLimitEnforced()
    {
        return DatabaseDescriptor.allowUnlimitedConcurrentValidations;
    }

    public int getConcurrentValidators()
    {
        return DatabaseDescriptor.getConcurrentValidations();
    }

    public void setConcurrentValidators(int value)
    {
        int concurrentCompactors = DatabaseDescriptor.getConcurrentCompactors();
        if (value > concurrentCompactors && !DatabaseDescriptor.allowUnlimitedConcurrentValidations)
            throw new IllegalArgumentException(
            String.format("Cannot set concurrent_validations greater than concurrent_compactors (%d)",
                          concurrentCompactors));

        if (value <= 0)
        {
            logger.info("Using default value of concurrent_compactors ({}) for concurrent_validations", concurrentCompactors);
            value = concurrentCompactors;
        }
        else
        {
            logger.info("Setting concurrent_validations to {}", value);
        }

        DatabaseDescriptor.setConcurrentValidations(value);
        CompactionManager.instance.setConcurrentValidations();
    }

    public int getConcurrentViewBuilders()
    {
        return DatabaseDescriptor.getConcurrentViewBuilders();
    }

    public void setConcurrentViewBuilders(int value)
    {
        if (value <= 0)
            throw new IllegalArgumentException("Number of concurrent view builders should be greater than 0.");
        DatabaseDescriptor.setConcurrentViewBuilders(value);
        CompactionManager.instance.setConcurrentViewBuilders(DatabaseDescriptor.getConcurrentViewBuilders());
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return DatabaseDescriptor.isIncrementalBackupsEnabled();
    }

    public void setIncrementalBackupsEnabled(boolean value)
    {
        DatabaseDescriptor.setIncrementalBackupsEnabled(value);
    }

    public Future<StreamState> startBootstrap(ClusterMetadata metadata,
                                              InetAddressAndPort beingReplaced,
                                              MovementMap movements,
                                              MovementMap strictMovements)
    {
        logger.info("Starting to bootstrap...");
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.IN_PROGRESS);
        BootStrapper bootstrapper = new BootStrapper(getBroadcastAddressAndPort(), metadata, movements, strictMovements);
        boolean res = ongoingBootstrap.compareAndSet(null, bootstrapper);
        if (!res)
            throw new IllegalStateException("Bootstrap can be started exactly once, but seems to have already started: " + bootstrapper);
        bootstrapper.addProgressListener(progressSupport);
        return bootstrapper.bootstrap(streamStateStore,
                                      useStrictConsistency && beingReplaced == null,
                                      beingReplaced); // handles token update
    }

    public void invalidateLocalRanges()
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.invalidateLocalRanges();
                }
            }
        }
    }

    /**
     * All MVs have been created during bootstrap, so mark them as built
     */
    public void markViewsAsBuilt()
    {
        for (String keyspace : Schema.instance.getUserKeyspaces().names())
        {
            for (ViewMetadata view: Schema.instance.getKeyspaceMetadata(keyspace).views)
                SystemKeyspace.finishViewBuildStatus(view.keyspace(), view.name());
        }
    }

    @Override
    public String getBootstrapState()
    {
        return SystemKeyspace.getBootstrapState().name();
    }

    // TODO: (TM/alexp): we can not use this method directly, since we need to be able to commit the "join" event.
    public boolean resumeBootstrap()
    {
        // todo:
//        if (isBootstrapMode && SystemKeyspace.bootstrapInProgress())
//        {
//            logger.info("Resuming bootstrap...");
//
//            // already bootstrapped ranges are filtered during bootstrap
//            BootStrapper bootstrapper = ongoingBootstrap.get();
//            if (bootstrapper == null)
//                throw new IllegalStateException("Can't continue bootstrap since it hasn't been started");
//
//            InetAddressAndPort replacingEndpoint = replacing
//                                                   ? DatabaseDescriptor.getReplaceAddress()
//                                                   : null;
//
//            Future<StreamState> bootstrapStream = bootstrapper.bootstrap(streamStateStore,
//                                                                         useStrictConsistency && !replacing,
//                                                                         replacingEndpoint); // handles token update
//            bootstrapStream.addCallback(new FutureCallback<StreamState>()
//            {
//                @Override
//                public void onSuccess(StreamState streamState)
//                {
//                    try
//                    {
//                        bootstrapFinished();
//                        if (isSurveyMode)
//                        {
//                            logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
//                        }
//                        else
//                        {
//                            isSurveyMode = false;
//                            progressSupport.progress("bootstrap", ProgressEvent.createNotification("Joining ring..."));
//                            finishJoiningRing();
//                            doAuthSetup(false);
//                        }
//                        progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
//                        if (!isNativeTransportRunning())
//                            daemon.initializeClientTransports();
//                        daemon.start();
//                        logger.info("Resume complete");
//                    }
//                    catch(Exception e)
//                    {
//                        onFailure(e);
//                        throw e;
//                    }
//                }
//
//                @Override
//                public void onFailure(Throwable e)
//                {
//                    String message = "Error during bootstrap: ";
//                    if (e instanceof ExecutionException && e.getCause() != null)
//                    {
//                        message += e.getCause().getMessage();
//                    }
//                    else
//                    {
//                        message += e.getMessage();
//                    }
//                    logger.error(message, e);
//                    progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.ERROR, 1, 1, message));
//                    progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
//                }
//            });
//            return true;
//        }
//        else
//        {
//            logger.info("Resuming bootstrap is requested, but the node is already bootstrapped.");
//            return false;
//        }
        return false;
    }

    public void abortBootstrap(String nodeStr, String endpoint)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId;
        if (!StringUtils.isEmpty(nodeStr))
            nodeId = NodeId.fromString(nodeStr);
        else
            nodeId = metadata.directory.peerId(InetAddressAndPort.getByNameUnchecked(endpoint));
        if (FailureDetector.instance.isAlive(metadata.directory.endpoint(nodeId)))
            throw new RuntimeException("Can't abort bootstrap for " + nodeId + " - it is alive");
        NodeState nodeState = metadata.directory.peerState(nodeId);
        switch (nodeState)
        {
            case REGISTERED:
            case BOOTSTRAPPING:
                if (metadata.inProgressSequences.contains(nodeId))
                {
                    InProgressSequence<?> seq = metadata.inProgressSequences.get(nodeId);
                    if (seq.kind() != InProgressSequences.Kind.JOIN)
                        throw new RuntimeException("Can't abort bootstrap for " + nodeId + " since it is not bootstrapping");
                    ClusterMetadataService.instance().commit(new CancelInProgressSequence(nodeId));
                }
                ClusterMetadataService.instance().commit(new Unregister(nodeId));
                break;
            default:
                throw new RuntimeException("Can't abort bootstrap for node " + nodeId + " since the state is " + nodeState);
        }
    }

    public Map<String,List<Integer>> getConcurrency(List<String> stageNames)
    {
        Stream<Stage> stageStream = stageNames.isEmpty() ? stream(Stage.values()) : stageNames.stream().map(Stage::fromPoolName);
        return stageStream.collect(toMap(s -> s.jmxName,
                                         s -> Arrays.asList(s.getCorePoolSize(), s.getMaximumPoolSize())));
    }

    public void setConcurrency(String threadPoolName, int newCorePoolSize, int newMaximumPoolSize)
    {
        Stage stage = Stage.fromPoolName(threadPoolName);
        if (newCorePoolSize >= 0)
            stage.setCorePoolSize(newCorePoolSize);
        stage.setMaximumPoolSize(newMaximumPoolSize);
    }

    public boolean isBootstrapMode()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        return metadata != null && (metadata.myNodeState() == BOOTSTRAPPING || metadata.myNodeState() == BOOT_REPLACING);
    }

    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace)
    {
        return getRangeToEndpointMap(keyspace, false);
    }

    public Map<List<String>, List<String>> getRangeToEndpointWithPortMap(String keyspace)
    {
         return getRangeToEndpointMap(keyspace, true);
    }

    /**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return the endpoint map
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace, boolean withPort)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey().asList(), Replicas.stringify(entry.getValue(), withPort));
        }
        return map;
    }

    /**
     * Return the native address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the native address
     */
    public String getNativeaddress(InetAddressAndPort endpoint, boolean withPort)
    {
        if (endpoint.equals(getBroadcastAddressAndPort()))
            return FBUtilities.getBroadcastNativeAddressAndPort().getHostAddress(withPort);
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT) != null)
        {
            try
            {
                InetAddressAndPort address = InetAddressAndPort.getByName(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value);
                return address.getHostAddress(withPort);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
             final String ipAddress;
             // If RPC_ADDRESS present in gossip for this endpoint use it.  This is expected for 3.x nodes.
             if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS) != null)
             {
                 ipAddress = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value;
             }
             else
             {
                 // otherwise just use the IP of the endpoint itself.
                 ipAddress = endpoint.getHostAddress(false);
             }

             // include the configured native_transport_port.
             try
             {
                 InetAddressAndPort address = InetAddressAndPort.getByNameOverrideDefaults(ipAddress, DatabaseDescriptor.getNativeTransportPort());
                 return address.getHostAddress(withPort);
             }
             catch (UnknownHostException e)
             {
                 throw new RuntimeException(e);
             }
         }
    }

    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace)
    {
        return getRangeToNativeaddressMap(keyspace, false);
    }

    public Map<List<String>, List<String>> getRangeToNativeaddressWithPortMap(String keyspace)
    {
        return getRangeToNativeaddressMap(keyspace, true);
    }

    /**
     * for a keyspace, return the ranges and corresponding RPC addresses for a given keyspace.
     * @param keyspace
     * @return the endpoint map
     */
    private Map<List<String>, List<String>> getRangeToNativeaddressMap(String keyspace, boolean withPort)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<>(entry.getValue().size());
            for (Replica replicas: entry.getValue())
            {
                rpcaddrs.add(getNativeaddress(replicas.endpoint(), withPort));
            }
            map.put(entry.getKey().asList(), rpcaddrs);
        }
        return map;
    }

    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace)
    {
        return getPendingRangeToEndpointMap(keyspace, false);
    }

    public Map<List<String>, List<String>> getPendingRangeToEndpointWithPortMap(String keyspace)
    {
        return getPendingRangeToEndpointMap(keyspace, true);
    }

    private Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace, boolean withPort)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonLocalStrategyKeyspaces().iterator().next().name;

        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : ClusterMetadata.current().pendingRanges(Keyspace.open(keyspace).getMetadata()).entrySet())
            map.put(entry.getKey().asList(), Replicas.stringify(entry.getValue(), withPort));

        return map;
    }

    public EndpointsByRange getRangeToAddressMap(String keyspace)
    {
        return getRangeToAddressMap(keyspace, ClusterMetadata.current().tokenMap.tokens());
    }

    public EndpointsByRange getRangeToAddressMapInLocalDC(String keyspace)
    {
        Predicate<Replica> isLocalDC = replica -> isLocalDC(replica.endpoint());

        EndpointsByRange origMap = getRangeToAddressMap(keyspace, getTokensInLocalDC());
        Map<Range<Token>, EndpointsForRange> filteredMap = Maps.newHashMap();
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : origMap.entrySet())
        {
            EndpointsForRange endpointsInLocalDC = entry.getValue().filter(isLocalDC);
            filteredMap.put(entry.getKey(), endpointsInLocalDC);
        }

        return new EndpointsByRange(filteredMap);
    }

    private List<Token> getTokensInLocalDC()
    {
        List<Token> filteredTokens = Lists.newArrayList();
        ClusterMetadata metadata = ClusterMetadata.current();
        for (Token token : metadata.tokenMap.tokens())
        {
            if (isLocalDC(metadata.tokenMap.owner(token), metadata))
                filteredTokens.add(token);
        }
        return filteredTokens;
    }

    private boolean isLocalDC(NodeId nodeId, ClusterMetadata metadata)
    {
        return metadata.directory.location(metadata.myNodeId()).datacenter.equals(metadata.directory.location(nodeId).datacenter);
    }

    private boolean isLocalDC(InetAddressAndPort targetHost)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        return isLocalDC(metadata.directory.peerId(targetHost), metadata);
    }

    private EndpointsByRange getRangeToAddressMap(String keyspace, List<Token> sortedTokens)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonLocalStrategyKeyspaces().iterator().next().name;

        List<Range<Token>> ranges = getAllRanges(sortedTokens);
        return constructRangeToEndpointMap(keyspace, ranges);
    }


    public List<String> describeRingJMX(String keyspace) throws IOException
    {
        return describeRingJMX(keyspace, false);
    }

    public List<String> describeRingWithPortJMX(String keyspace) throws IOException
    {
        return describeRingJMX(keyspace,true);
    }

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     */
    private List<String> describeRingJMX(String keyspace, boolean withPort) throws IOException
    {
        List<TokenRange> tokenRanges;
        try
        {
            tokenRanges = describeRing(keyspace, false, withPort);
        }
        catch (InvalidRequestException e)
        {
            throw new IOException(e.getMessage());
        }
        List<String> result = new ArrayList<>(tokenRanges.size());

        for (TokenRange tokenRange : tokenRanges)
            result.add(tokenRange.toString(withPort));

        return result;
    }

    /**
     * The TokenRange for a given keyspace.
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List<TokenRange> describeRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, false, false);
    }

    private List<TokenRange> describeRing(String keyspace, boolean includeOnlyLocalDC, boolean withPort) throws InvalidRequestException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspace))
            throw new InvalidRequestException("No such keyspace: " + keyspace);

        if (keyspace == null || Keyspace.open(keyspace).getReplicationStrategy() instanceof LocalStrategy)
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);

        List<TokenRange> ranges = new ArrayList<>();
        Token.TokenFactory tf = getTokenFactory();

        EndpointsByRange rangeToAddressMap =
                includeOnlyLocalDC
                        ? getRangeToAddressMapInLocalDC(keyspace)
                        : getRangeToAddressMap(keyspace);

        for (Map.Entry<Range<Token>, EndpointsForRange> entry : rangeToAddressMap.entrySet())
            ranges.add(TokenRange.create(tf, entry.getKey(), ImmutableList.copyOf(entry.getValue().endpoints()), withPort));

        return ranges;
    }

    public Map<String, String> getTokenToEndpointMap()
    {
        return getTokenToEndpointMap(false);
    }

    public Map<String, String> getTokenToEndpointWithPortMap()
    {
        return getTokenToEndpointMap(true);
    }

    private Map<String, String> getTokenToEndpointMap(boolean withPort)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Map<Token, NodeId> mapNodeId = new HashMap<>(metadata.tokenMap.asMap());
        // we don't yet have the joining nodes in tokenmap - but they are required
        // for nodetool status - grab them from the in progress sequences for the joining nodes;
        for (NodeId nodeId : metadata.directory.peerIds())
        {
            if (NodeState.isPreJoin(metadata.directory.peerState(nodeId)))
            {
                InProgressSequence<?> seq = metadata.inProgressSequences.get(nodeId);
                GossipHelper.getTokensFromSequence(seq).forEach(t -> mapNodeId.put(t, nodeId));
            }
        }
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<>(mapNodeId.size());
        List<Token> tokens = new ArrayList<>(mapNodeId.keySet());
        Collections.sort(tokens);
        for (Token token : tokens)
        {
            mapString.put(token.toString(), metadata.directory.endpoint(mapNodeId.get(token)).getHostAddress(withPort));
        }
        return mapString;
    }

    public String getLocalHostId()
    {
        return getLocalHostUUID().toString();
    }

    public UUID getLocalHostUUID()
    {
        // Metadata collector requires using local host id, and flush of IndexInfo may race with
        // creation and initialization of cluster metadata service. Metadata collector does accept
        // null localhost ID values, it's just that TokenMetadata was created earlier.
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null || metadata.directory.peerId(getBroadcastAddressAndPort()) == null)
            return null;
        return metadata.directory.peerId(getBroadcastAddressAndPort()).toUUID();
    }

    public Map<String, String> getHostIdMap()
    {
        return getEndpointToHostId();
    }


    public Map<String, String> getEndpointToHostId()
    {
        return getEndpointToHostId(false);
    }

    public Map<String, String> getEndpointWithPortToHostId()
    {
        return getEndpointToHostId(true);
    }

    private  Map<String, String> getEndpointToHostId(boolean withPort)
    {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<NodeId, NodeAddresses> entry : ClusterMetadata.current().directory.addresses.entrySet())
            mapOut.put(entry.getValue().broadcastAddress.getHostAddress(withPort), entry.getKey().toUUID().toString());
        return mapOut;
    }

    public Map<String, String> getHostIdToEndpoint()
    {
        return getHostIdToEndpoint(false);
    }

    public Map<String, String> getHostIdToEndpointWithPort()
    {
        return getHostIdToEndpoint(true);
    }

    private Map<String, String> getHostIdToEndpoint(boolean withPort)
    {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<NodeId, NodeAddresses> entry : ClusterMetadata.current().directory.addresses.entrySet())
            mapOut.put(entry.getKey().toUUID().toString(), entry.getValue().broadcastAddress.getHostAddress(withPort));
        return mapOut;
    }

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private EndpointsByRange constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspace);
        TokenMap tokenMap = metadata.tokenMap;

        Map<Range<Token>, EndpointsForRange> rangeToEndpointMap = new HashMap<>(ranges.size());
        for (Range<Token> range : ranges)
        {
            Token token = tokenMap.nextToken(tokenMap.tokens(), range.right.getToken());
            rangeToEndpointMap.put(range, metadata.placements.get(keyspaceMetadata.params.replication).reads.forRange(token));
        }

        return new EndpointsByRange(rangeToEndpointMap);

    }

    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // no-op
    }

    /*
     * Handle the reception of a new particular ApplicationState for a particular endpoint. Note that the value of the
     * ApplicationState has not necessarily "changed" since the last known value, if we already received the same update
     * from somewhere else.
     *
     * onChange only ever sees one ApplicationState piece change at a time (even if many ApplicationState updates were
     * received at the same time), so we perform a kind of state machine here. We are concerned with two events: knowing
     * the token associated with an endpoint, and knowing its operation mode. Nodes can start in either bootstrap or
     * normal mode, and from bootstrap mode can change mode to normal. A node in bootstrap mode needs to have
     * pendingranges set in TokenMetadata; a node in normal mode should instead be part of the token ring.
     *
     * Normal progression of ApplicationState.STATUS values for a node should be like this:
     * STATUS_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATUS_NORMAL,token
     *   ready to serve reads and writes.
     * STATUS_LEAVING,token
     *   get ready to leave the cluster as part of a decommission
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * STATUS_MOVING,newtoken
     *   set if node is currently moving to a new token in the ring
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removenode, decommission or move.
     */
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || Gossiper.instance.isDeadState(epState))
        {
            logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            return;
        }

        if (state == ApplicationState.INDEX_STATUS)
        {
            updateIndexStatus(endpoint, value);
            return;
        }

        if (ClusterMetadata.current().directory.allJoinedEndpoints().contains(endpoint))
        {
            switch (state)
            {
                case RELEASE_VERSION:
                    SystemKeyspace.updatePeerInfo(endpoint, "release_version", value.value);
                    break;
                case RPC_ADDRESS:
                    try
                    {
                        SystemKeyspace.updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(value.value));
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case NATIVE_ADDRESS_AND_PORT:
                    try
                    {
                        InetAddressAndPort address = InetAddressAndPort.getByName(value.value);
                        SystemKeyspace.updatePeerNativeAddress(endpoint, address);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case RPC_READY:
                    notifyRpcChange(endpoint, epState.isRpcReady());
                    break;
                case NET_VERSION:
                    updateNetVersion(endpoint, value);
                    break;
                case STATUS_WITH_PORT:
                    String[] pieces = splitValue(value);
                    String moveName = pieces[0];
                    if (moveName.equals(VersionedValue.SHUTDOWN))
                    {
                        logger.info("Node {} state jump to shutdown", endpoint);
                        Gossiper.runInGossipStageBlocking(() -> {
                            Gossiper.instance.markDead(endpoint, epState);
                        });
                    }
                    else if (moveName.equals(VersionedValue.STATUS_NORMAL))
                    {
                        logger.info("Node {} state jump to NORMAL", endpoint);
                    }
                    break;
                case SCHEMA:
                    SystemKeyspace.updatePeerInfo(endpoint, "schema_version", UUID.fromString(value.value));
                    break;
            }
        }
        else
        {
            logger.debug("Ignoring application state {} from {} because it is not a member in token metadata",
                         state, endpoint);
        }
    }

    private static String[] splitValue(VersionedValue value)
    {
        return value.value.split(VersionedValue.DELIMITER_STR, -1);
    }

    public static void updateIndexStatus(InetAddressAndPort endpoint, VersionedValue versionedValue)
    {
        IndexStatusManager.instance.receivePeerIndexStatus(endpoint, versionedValue);
    }

    private void updateNetVersion(InetAddressAndPort endpoint, VersionedValue value)
    {
        try
        {
            MessagingService.instance().versions.set(endpoint, Integer.parseInt(value.value));
        }
        catch (NumberFormatException e)
        {
            throw new AssertionError("Got invalid value for NET_VERSION application state: " + value.value);
        }
    }

    public void updateTopology()
    {
        logger.debug("Caller should be updated, updateTopology is no longer supported", new RuntimeException());
    }

    private void notifyRpcChange(InetAddressAndPort endpoint, boolean ready)
    {
        if (ready)
            notifyUp(endpoint);
        else
            notifyDown(endpoint);
    }

    private void notifyUp(InetAddressAndPort endpoint)
    {
        if (!isRpcReady(endpoint) || !Gossiper.instance.isAlive(endpoint))
            return;

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onUp(endpoint);
    }

    private void notifyDown(InetAddressAndPort endpoint)
    {
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onDown(endpoint);
    }

    public void notifyJoined(InetAddressAndPort endpoint)
    {
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onJoinCluster(endpoint);
    }

    public void notifyMoved(InetAddressAndPort endpoint)
    {
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onMove(endpoint);
    }

    public void notifyLeft(InetAddressAndPort endpoint)
    {
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(endpoint);
    }

    public boolean isRpcReady(InetAddressAndPort endpoint)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        return state != null && state.isRpcReady();
    }

    /**
     * Set the RPC status. Because when draining a node we need to set the RPC
     * status to not ready, and drain is called by the shutdown hook, it may be that value is false
     * and there is no local endpoint state. In this case it's OK to just do nothing. Therefore,
     * we assert that the local endpoint state is not null only when value is true.
     *
     * @param isRpcReady - true indicates that RPC is ready, false indicates the opposite.
     */
    public void setRpcReady(boolean isRpcReady)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(getBroadcastAddressAndPort());
        // if value is false we're OK with a null state, if it is true we are not.
        assert !isRpcReady || state != null;

        if (state != null)
            Gossiper.instance.addLocalApplicationState(ApplicationState.RPC_READY, valueFactory.rpcReady(isRpcReady));
    }

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        // Explicitly process STATUS or STATUS_WITH_PORT before the other
        // application states to maintain pre-4.0 semantics with the order
        // they are processed.  Otherwise the endpoint will not be added
        // to TokenMetadata so non-STATUS* appstates will be ignored.
        ApplicationState statusState = ApplicationState.STATUS_WITH_PORT;
        VersionedValue statusValue;
        statusValue = epState.getApplicationState(statusState);
        if (statusValue == null)
        {
            statusState = ApplicationState.STATUS;
            statusValue = epState.getApplicationState(statusState);
        }
        if (statusValue != null)
            Gossiper.instance.doOnChangeNotifications(endpoint, statusState, statusValue);

        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.states())
        {
            if (entry.getKey() == ApplicationState.STATUS_WITH_PORT || entry.getKey() == ApplicationState.STATUS)
                continue;
            Gossiper.instance.doOnChangeNotifications(endpoint, entry.getKey(), entry.getValue());
        }
    }

    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        if (ClusterMetadata.current().directory.allAddresses().contains(endpoint))
            notifyUp(endpoint);
    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        // interrupt any outbound connection; if the node is failing and we cannot reconnect,
        // this will rapidly lower the number of bytes we are willing to queue to the node
        MessagingService.instance().interruptOutbound(endpoint);
        notifyDown(endpoint);
    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(endpoint, state);

        // Then, the node may have been upgraded and changed its messaging protocol version. If so, we
        // want to update that before we mark the node live again to avoid problems like CASSANDRA-11128.
        VersionedValue netVersion = state.getApplicationState(ApplicationState.NET_VERSION);
        if (netVersion != null)
            updateNetVersion(endpoint, netVersion);
    }

    @Override
    public String getLoadString()
    {
        return FileUtils.stringifyFileSize(StorageMetrics.load.getCount());
    }

    @Override
    public String getUncompressedLoadString()
    {
        return FileUtils.stringifyFileSize(StorageMetrics.uncompressedLoad.getCount());
    }

    public Map<String, String> getLoadMapWithPort()
    {
        return getLoadMap(true);
    }

    public Map<String, String> getLoadMap()
    {
        return getLoadMap(false);
    }

    private Map<String, String> getLoadMap(boolean withPort)
    {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<InetAddressAndPort,Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet())
        {
            map.put(entry.getKey().getHostAddress(withPort), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(getBroadcastAddressAndPort().getHostAddress(withPort), getLoadString());
        return map;
    }

    // TODO
    public final void deliverHints(String host)
    {
        throw new UnsupportedOperationException();
    }

    public Collection<Token> getLocalTokens()
    {
        Collection<Token> tokens = SystemKeyspace.getSavedTokens();
        assert tokens != null && !tokens.isEmpty(); // should not be called before initServer sets this
        return tokens;
    }

    @Nullable
    public InetAddressAndPort getEndpointForHostId(UUID hostId)
    {
        Directory directory = ClusterMetadata.current().directory;
        NodeId nodeId = NodeId.isValidNodeId(hostId)
                        ? NodeId.fromUUID(hostId)
                        : directory.nodeIdFromHostId(hostId);

        return nodeId != null ? ClusterMetadata.current().directory.endpoint(nodeId) : null;
    }

    @Nullable
    public UUID getHostIdForEndpoint(InetAddressAndPort address)
    {
        NodeId nodeId = ClusterMetadata.current().directory.peerId(address);
        if (nodeId != null)
            return nodeId.toUUID();
        return null;
    }

    /* These methods belong to the MBean interface */

    public List<String> getTokens()
    {
        return getTokens(getBroadcastAddressAndPort());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException
    {
        return getTokens(InetAddressAndPort.getByName(endpoint));
    }

    private List<String> getTokens(InetAddressAndPort endpoint)
    {
        List<String> strTokens = new ArrayList<>();
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId = metadata.directory.peerId(endpoint);
        for (Token tok : metadata.tokenMap.tokens(nodeId))
            strTokens.add(tok.toString());
        return strTokens;
    }

    public String getReleaseVersion()
    {
        return FBUtilities.getReleaseVersionString();
    }

    @Override
    public String getGitSHA()
    {
        return FBUtilities.getGitSHA();
    }

    public String getSchemaVersion()
    {
        return Schema.instance.getVersion().toString();
    }

    public String getKeyspaceReplicationInfo(String keyspaceName)
    {
        Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspaceName);
        if (keyspaceInstance == null)
            throw new IllegalArgumentException(); // ideally should never happen
        ReplicationParams replicationParams = keyspaceInstance.getMetadata().params.replication;
        String replicationInfo = replicationParams.klass.getSimpleName() + " " + replicationParams.options.toString();
        return replicationInfo;
    }

    public Set<InetAddressAndPort> endpointsWithState(NodeState ... state)
    {
        Set<NodeState> states = Sets.newHashSet(state);
        ClusterMetadata metadata = ClusterMetadata.current();
        return metadata.directory.states.entrySet().stream()
                                               .filter(e -> states.contains(e.getValue()))
                                               .map(e -> metadata.directory.endpoint(e.getKey()))
                                               .collect(toSet());
    }

    @Deprecated
    public List<String> getLeavingNodes()
    {
        return stringify(endpointsWithState(LEAVING), false);
    }

    public List<String> getLeavingNodesWithPort()
    {
        return stringify(endpointsWithState(LEAVING), true);
    }

    @Deprecated
    public List<String> getMovingNodes()
    {
        List<String> endpoints = new ArrayList<>();

        for (InetAddressAndPort endpoint : endpointsWithState(MOVING))
        {
            endpoints.add(endpoint.getAddress().getHostAddress());
        }

        return endpoints;
    }

    public List<String> getMovingNodesWithPort()
    {
        List<String> endpoints = new ArrayList<>();

        for (InetAddressAndPort endpoint : endpointsWithState(MOVING))
        {
            endpoints.add(endpoint.getHostAddressAndPort());
        }

        return endpoints;
    }

    @Deprecated
    public List<String> getJoiningNodes()
    {
        return stringify(Iterables.concat(endpointsWithState(BOOTSTRAPPING),
                                          endpointsWithState(BOOT_REPLACING)),
                         false);
    }

    public List<String> getJoiningNodesWithPort()
    {
        return stringify(Iterables.concat(endpointsWithState(BOOTSTRAPPING),
                                          endpointsWithState(BOOT_REPLACING)),
                         true);
    }

    @Deprecated
    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers(), false);
    }

    public List<String> getLiveNodesWithPort()
    {
        return stringify(Gossiper.instance.getLiveMembers(), true);
    }

    public Set<InetAddressAndPort> getLiveRingMembers()
    {
        return getLiveRingMembers(false);
    }

    public Set<InetAddressAndPort> getLiveRingMembers(boolean excludeDeadStates)
    {
        Set<InetAddressAndPort> ret = new HashSet<>();
        for (InetAddressAndPort ep : Gossiper.instance.getLiveMembers())
        {
            if (excludeDeadStates)
            {
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
                if (epState == null || Gossiper.instance.isDeadState(epState))
                    continue;
            }

            if (ClusterMetadata.current().directory.allAddresses().contains(ep))
                ret.add(ep);
        }
        return ret;
    }


    @Deprecated
    public List<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers(), false);
    }

    public List<String> getUnreachableNodesWithPort()
    {
        return stringify(Gossiper.instance.getUnreachableMembers(), true);
    }

    @Override
    public String[] getAllDataFileLocations()
    {
        return getCanonicalPaths(DatabaseDescriptor.getAllDataFileLocations());
    }

    private String[] getCanonicalPaths(String[] paths)
    {
        String[] locations = new String[paths.length];
        for (int i = 0; i < paths.length; i++)
            locations[i] = FileUtils.getCanonicalPath(paths[i]);
        return locations;
    }

    @Override
    public String[] getLocalSystemKeyspacesDataFileLocations()
    {
        return getCanonicalPaths(DatabaseDescriptor.getLocalSystemKeyspacesDataFileLocations());
    }

    @Override
    public String[] getNonLocalSystemKeyspacesDataFileLocations()
    {
        return getCanonicalPaths(DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations());
    }

    public String getCommitLogLocation()
    {
        return FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation());
    }

    public String getSavedCachesLocation()
    {
        return FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation());
    }

    private List<String> stringify(Iterable<InetAddressAndPort> endpoints, boolean withPort)
    {
        List<String> stringEndpoints = new ArrayList<>();
        for (InetAddressAndPort ep : endpoints)
        {
            stringEndpoints.add(ep.getHostAddress(withPort));
        }
        return stringEndpoints;
    }

    public int getCurrentGenerationNumber()
    {
        return Gossiper.instance.getCurrentGenerationNumber(getBroadcastAddressAndPort());
    }

    public int forceKeyspaceCleanup(String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return forceKeyspaceCleanup(0, keyspaceName, tables);
    }

    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        if (isLocalSystemKeyspace(keyspaceName))
            throw new RuntimeException("Cleanup of the system keyspace is neither necessary nor wise");

        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        logger.info("Starting {} on {}.{}", OperationType.CLEANUP, keyspaceName, Arrays.toString(tableNames));
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.forceCleanup(jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        logger.info("Completed {} with status {}", OperationType.CLEANUP, status);
        return status.statusCode;
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        IScrubber.Options options = IScrubber.options()
                                             .skipCorrupted(skipCorrupted)
                                             .checkData(checkData)
                                             .reinsertOverflowedTTLRows(reinsertOverflowedTTL)
                                             .build();
        return scrub(disableSnapshot, options, jobs, keyspaceName, tableNames);
    }

    public int scrub(boolean disableSnapshot, IScrubber.Options options, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        logger.info("Starting {} on {}.{}", OperationType.SCRUB, keyspaceName, Arrays.toString(tableNames));
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.scrub(disableSnapshot, options, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        logger.info("Completed {} with status {}", OperationType.SCRUB, status);
        return status.statusCode;
    }

    @Deprecated
    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return verify(extendedVerify, false, false, false, false, false, keyspaceName, tableNames);
    }

    public int verify(boolean extendedVerify, boolean checkVersion, boolean diskFailurePolicy, boolean mutateRepairStatus, boolean checkOwnsTokens, boolean quick, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(diskFailurePolicy)
                                             .extendedVerification(extendedVerify)
                                             .checkVersion(checkVersion)
                                             .mutateRepairStatus(mutateRepairStatus)
                                             .checkOwnsTokens(checkOwnsTokens)
                                             .quick(quick).build();
        logger.info("Staring {} on {}.{} with options = {}", OperationType.VERIFY, keyspaceName, Arrays.toString(tableNames), options);
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.verify(options);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        logger.info("Completed {} with status {}", OperationType.VERIFY, status);
        return status.statusCode;
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return upgradeSSTables(keyspaceName, excludeCurrentVersion, 0, tableNames);
    }

    public int upgradeSSTables(String keyspaceName,
                               final boolean skipIfCurrentVersion,
                               final long skipIfNewerThanTimestamp,
                               int jobs,
                               String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return rewriteSSTables(keyspaceName, skipIfCurrentVersion, skipIfNewerThanTimestamp, false, jobs, tableNames);
    }

    public int recompressSSTables(String keyspaceName,
                                  int jobs,
                                  String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return rewriteSSTables(keyspaceName, false, Long.MAX_VALUE, true, jobs, tableNames);
    }


    public int rewriteSSTables(String keyspaceName,
                               final boolean skipIfCurrentVersion,
                               final long skipIfNewerThanTimestamp,
                               final boolean skipIfCompressionMatches,
                               int jobs,
                               String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        logger.info("Starting {} on {}.{}", OperationType.UPGRADE_SSTABLES, keyspaceName, Arrays.toString(tableNames));
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, true, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.sstablesRewrite(skipIfCurrentVersion, skipIfNewerThanTimestamp, skipIfCompressionMatches, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        logger.info("Completed {} with status {}", OperationType.UPGRADE_SSTABLES, status);
        return status.statusCode;
    }

    public List<Pair<String, String>> getPreparedStatements()
    {
        List<Pair<String, String>> statements = new ArrayList<>();
        for (Entry<MD5Digest, QueryHandler.Prepared> e : QueryProcessor.instance.getPreparedStatements().entrySet())
            statements.add(Pair.create(e.getKey().toString(), e.getValue().rawCQLStatement));
        return statements;
    }

    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            cfStore.forceMajorCompaction(splitOutput);
        }
    }

    public int relocateSSTables(String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return relocateSSTables(0, keyspaceName, tableNames);
    }

    public int relocateSSTables(int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        logger.info("Starting {} on {}.{}", OperationType.RELOCATE, keyspaceName, Arrays.toString(tableNames));
        for (ColumnFamilyStore cfs : getValidColumnFamilies(false, false, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfs.relocateSSTables(jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        logger.info("Completed {} with status {}", OperationType.RELOCATE, status);
        return status.statusCode;
    }

    public int garbageCollect(String tombstoneOptionString, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        TombstoneOption tombstoneOption = TombstoneOption.valueOf(tombstoneOptionString);
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        logger.info("Starting {} on {}.{}", OperationType.GARBAGE_COLLECT, keyspaceName, Arrays.toString(tableNames));
        for (ColumnFamilyStore cfs : getValidColumnFamilies(false, false, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfs.garbageCollect(tombstoneOption, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        logger.info("Completed {} with status {}", OperationType.GARBAGE_COLLECT, status);
        return status.statusCode;
    }

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param options
     *            Map of options (skipFlush is the only supported option for now)
     * @param entities
     *            list of keyspaces / tables in the form of empty | ks1 ks2 ... | ks1.cf1,ks2.cf2,...
     */
    @Override
    public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException
    {
        DurationSpec.IntSecondsBound ttl = options.containsKey("ttl") ? new DurationSpec.IntSecondsBound(options.get("ttl")) : null;
        if (ttl != null)
        {
            int minAllowedTtlSecs = CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS.getInt();
            if (ttl.toSeconds() < minAllowedTtlSecs)
                throw new IllegalArgumentException(String.format("ttl for snapshot must be at least %d seconds", minAllowedTtlSecs));
        }

        boolean skipFlush = Boolean.parseBoolean(options.getOrDefault("skipFlush", "false"));
        if (entities != null && entities.length > 0 && entities[0].contains("."))
        {
            takeMultipleTableSnapshot(tag, skipFlush, ttl, entities);
        }
        else
        {
            takeSnapshot(tag, skipFlush, ttl, entities);
        }
    }

    /**
     * Takes the snapshot of a specific table. A snapshot name must be
     * specified.
     *
     * @param keyspaceName
     *            the keyspace which holds the specified table
     * @param tableName
     *            the table to snapshot
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     */
    public void takeTableSnapshot(String keyspaceName, String tableName, String tag)
            throws IOException
    {
        takeMultipleTableSnapshot(tag, false, null, keyspaceName + "." + tableName);
    }

    @Override
    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        Collection<Range<Token>> tokenRanges = createRepairRangeFrom(startToken, endToken);

        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            cfStore.forceCompactionForTokenRange(tokenRanges);
        }
    }

    @Override
    public void forceKeyspaceCompactionForPartitionKey(String keyspaceName, String partitionKey, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        // validate that the key parses before attempting compaction
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            try
            {
                getKeyFromPartition(keyspaceName, cfStore.name, partitionKey);
            }
            catch (Exception e)
            {
                // JMX can not handle exceptions defined outside of java.* and javax.*, so safer to rewrite the exception
                IllegalArgumentException exception = new IllegalArgumentException(String.format("Unable to parse partition key '%s' for table %s; %s", partitionKey, cfStore.metadata, e.getMessage()));
                exception.setStackTrace(e.getStackTrace());
                throw exception;
            }
        }
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            cfStore.forceCompactionForKey(getKeyFromPartition(keyspaceName, cfStore.name, partitionKey));
        }
    }

    /***
     * Forces compaction for a list of partition keys in a table
     * The method will ignore the gc_grace_seconds for the partitionKeysIgnoreGcGrace during the comapction,
     * in order to purge the tombstones and free up space quicker.
     * @param keyspaceName keyspace name
     * @param tableName table name
     * @param partitionKeysIgnoreGcGrace partition keys ignoring the gc_grace_seconds
     * @throws IOException on any I/O operation error
     * @throws ExecutionException when attempting to retrieve the result of a task that aborted by throwing an exception
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted, either before or during the activity
     */
    public void forceCompactionKeysIgnoringGcGrace(String keyspaceName,
                                                   String tableName, String... partitionKeysIgnoreGcGrace) throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfStore = getValidKeyspace(keyspaceName).getColumnFamilyStore(tableName);
        cfStore.forceCompactionKeysIgnoringGcGrace(partitionKeysIgnoreGcGrace);
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        takeSnapshot(tag, false, null, keyspaceNames);
    }

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param tableList
     *            list of tables from different keyspace in the form of ks1.cf1 ks2.cf2
     */
    public void takeMultipleTableSnapshot(String tag, String... tableList)
            throws IOException
    {
        takeMultipleTableSnapshot(tag, false, null, tableList);
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param skipFlush Skip blocking flush of memtable
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    private void takeSnapshot(String tag, boolean skipFlush, DurationSpec.IntSecondsBound ttl, String... keyspaceNames) throws IOException
    {
        if (operationMode() == Mode.JOINING)
            throw new IOException("Cannot snapshot until bootstrap completes");
        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Iterable<Keyspace> keyspaces;
        if (keyspaceNames.length == 0)
        {
            keyspaces = Keyspace.all();
        }
        else
        {
            ArrayList<Keyspace> t = new ArrayList<>(keyspaceNames.length);
            for (String keyspaceName : keyspaceNames)
                t.add(getValidKeyspace(keyspaceName));
            keyspaces = t;
        }

        // Do a check to see if this snapshot exists before we actually snapshot
        for (Keyspace keyspace : keyspaces)
            if (keyspace.snapshotExists(tag))
                throw new IOException("Snapshot " + tag + " already exists.");


        RateLimiter snapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();
        Instant creationTime = now();

        for (Keyspace keyspace : keyspaces)
        {
            keyspace.snapshot(tag, null, skipFlush, ttl, snapshotRateLimiter, creationTime);
        }
    }

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param skipFlush
     *            Skip blocking flush of memtable
     * @param tableList
     *            list of tables from different keyspace in the form of ks1.cf1 ks2.cf2
     */
    private void takeMultipleTableSnapshot(String tag, boolean skipFlush, DurationSpec.IntSecondsBound ttl, String... tableList)
            throws IOException
    {
        Map<Keyspace, List<String>> keyspaceColumnfamily = new HashMap<Keyspace, List<String>>();
        for (String table : tableList)
        {
            String splittedString[] = StringUtils.split(table, '.');
            if (splittedString.length == 2)
            {
                String keyspaceName = splittedString[0];
                String tableName = splittedString[1];

                if (keyspaceName == null)
                    throw new IOException("You must supply a keyspace name");
                if (operationMode() == Mode.JOINING)
                    throw new IOException("Cannot snapshot until bootstrap completes");

                if (tableName == null)
                    throw new IOException("You must supply a table name");
                if (tag == null || tag.equals(""))
                    throw new IOException("You must supply a snapshot name.");

                Keyspace keyspace = getValidKeyspace(keyspaceName);
                ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
                // As there can be multiple column family from same keyspace check if snapshot exist for that specific
                // columnfamily and not for whole keyspace

                if (columnFamilyStore.snapshotExists(tag))
                    throw new IOException("Snapshot " + tag + " already exists.");
                if (!keyspaceColumnfamily.containsKey(keyspace))
                {
                    keyspaceColumnfamily.put(keyspace, new ArrayList<String>());
                }

                // Add Keyspace columnfamily to map in order to support atomicity for snapshot process.
                // So no snapshot should happen if any one of the above conditions fail for any keyspace or columnfamily
                keyspaceColumnfamily.get(keyspace).add(tableName);

            }
            else
            {
                throw new IllegalArgumentException(
                        "Cannot take a snapshot on secondary index or invalid column family name. You must supply a column family name in the form of keyspace.columnfamily");
            }
        }

        RateLimiter snapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();
        Instant creationTime = now();

        for (Entry<Keyspace, List<String>> entry : keyspaceColumnfamily.entrySet())
        {
            for (String table : entry.getValue())
                entry.getKey().snapshot(tag, table, skipFlush, ttl, snapshotRateLimiter, creationTime);
        }
    }

    private void verifyKeyspaceIsValid(String keyspaceName)
    {
        if (null != VirtualKeyspaceRegistry.instance.getKeyspaceNullable(keyspaceName))
            throw new IllegalArgumentException("Cannot perform any operations against virtual keyspace " + keyspaceName);

        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
            throw new IllegalArgumentException("Keyspace " + keyspaceName + " does not exist");
    }

    private Keyspace getValidKeyspace(String keyspaceName)
    {
        verifyKeyspaceIsValid(keyspaceName);
        return Keyspace.open(keyspaceName);
    }

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames)
    {
        clearSnapshot(Collections.emptyMap(), tag, keyspaceNames);
    }

    public void clearSnapshot(Map<String, Object> options, String tag, String... keyspaceNames)
    {
        if (tag == null)
            tag = "";

        if (options == null)
            options = Collections.emptyMap();

        Set<String> keyspaces = new HashSet<>();
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            for (String keyspaceDir : new File(dataDir).tryListNames())
            {
                // Only add a ks if it has been specified as a param, assuming params were actually provided.
                if (keyspaceNames.length > 0 && !Arrays.asList(keyspaceNames).contains(keyspaceDir))
                    continue;
                keyspaces.add(keyspaceDir);
            }
        }

        Object olderThan = options.get("older_than");
        Object olderThanTimestamp = options.get("older_than_timestamp");

        final long clearOlderThanTimestamp;
        if (olderThan != null)
        {
            assert olderThan instanceof String : "it is expected that older_than is an instance of java.lang.String";
            clearOlderThanTimestamp = Clock.Global.currentTimeMillis() - new DurationSpec.LongSecondsBound((String) olderThan).toMilliseconds();
        }
        else if (olderThanTimestamp != null)
        {
            assert olderThanTimestamp instanceof String : "it is expected that older_than_timestamp is an instance of java.lang.String";
            try
            {
                clearOlderThanTimestamp = Instant.parse((String) olderThanTimestamp).toEpochMilli();
            }
            catch (DateTimeParseException ex)
            {
                throw new RuntimeException("Parameter older_than_timestamp has to be a valid instant in ISO format.");
            }
        }
        else
            clearOlderThanTimestamp = 0L;

        for (String keyspace : keyspaces)
            clearKeyspaceSnapshot(keyspace, tag, clearOlderThanTimestamp);

        if (logger.isDebugEnabled())
            logger.debug("Cleared out snapshot directories");
    }

    /**
     * Clear snapshots for a given keyspace.
     * @param keyspace keyspace to remove snapshots for
     * @param tag the user supplied snapshot name. If empty or null, all the snapshots will be cleaned
     * @param olderThanTimestamp if a snapshot was created before this timestamp, it will be cleared,
     *                           if its value is 0, this parameter is effectively ignored.
     */
    private void clearKeyspaceSnapshot(String keyspace, String tag, long olderThanTimestamp)
    {
        Set<TableSnapshot> snapshotsToClear = snapshotManager.loadSnapshots(keyspace)
                                                             .stream()
                                                             .filter(TableSnapshot.shouldClearSnapshot(tag, olderThanTimestamp))
                                                             .collect(Collectors.toSet());
        for (TableSnapshot snapshot : snapshotsToClear)
            snapshotManager.clearSnapshot(snapshot);
    }

    public Map<String, TabularData> getSnapshotDetails(Map<String, String> options)
    {
        boolean skipExpiring = options != null && Boolean.parseBoolean(options.getOrDefault("no_ttl", "false"));
        boolean includeEphemeral = options != null && Boolean.parseBoolean(options.getOrDefault("include_ephemeral", "false"));

        Map<String, TabularData> snapshotMap = new HashMap<>();

        for (TableSnapshot snapshot : snapshotManager.loadSnapshots())
        {
            if (skipExpiring && snapshot.isExpiring())
                continue;
            if (!includeEphemeral && snapshot.isEphemeral())
                continue;

            TabularDataSupport data = (TabularDataSupport) snapshotMap.get(snapshot.getTag());
            if (data == null)
            {
                data = new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE);
                snapshotMap.put(snapshot.getTag(), data);
            }

            SnapshotDetailsTabularData.from(snapshot, data);
        }

        return snapshotMap;
    }

    @Deprecated
    public Map<String, TabularData> getSnapshotDetails()
    {
        return getSnapshotDetails(ImmutableMap.of());
    }

    public long trueSnapshotsSize()
    {
        long total = 0;
        for (Keyspace keyspace : Keyspace.all())
        {
            if (isLocalSystemKeyspace(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                total += cfStore.trueSnapshotsSize();
            }
        }

        return total;
    }

    public void setSnapshotLinksPerSecond(long throttle)
    {
        logger.info("Setting snapshot throttle to {}", throttle);
        DatabaseDescriptor.setSnapshotLinksPerSecond(throttle);
    }

    public long getSnapshotLinksPerSecond()
    {
        return DatabaseDescriptor.getSnapshotLinksPerSecond();
    }

    public void refreshSizeEstimates() throws ExecutionException
    {
        cleanupSizeEstimates();
        FBUtilities.waitOnFuture(ScheduledExecutors.optionalTasks.submit(SizeEstimatesRecorder.instance));
    }

    public void cleanupSizeEstimates()
    {
        SystemKeyspace.clearAllEstimates();
    }

    /**
     * @param allowIndexes Allow index CF names to be passed in
     * @param autoAddIndexes Automatically add secondary indexes if a CF has them
     * @param keyspaceName keyspace
     * @param cfNames CFs
     * @throws java.lang.IllegalArgumentException when given CF name does not exist
     */
    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String keyspaceName, String... cfNames)
    {
        Keyspace keyspace = getValidKeyspace(keyspaceName);
        return keyspace.getValidColumnFamilies(allowIndexes, autoAddIndexes, cfNames);
    }

    /**
     * Flush all memtables for a keyspace and column families.
     * @param keyspaceName
     * @param tableNames
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            logger.debug("Forcing flush on keyspace {}, CF {}", keyspaceName, cfStore.name);
            cfStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }

    /**
     * Flush all memtables for a keyspace and column families.
     * @param keyspaceName
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName, ColumnFamilyStore.FlushReason reason) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName))
        {
            logger.debug("Forcing flush on keyspace {}, CF {}", keyspaceName, cfStore.name);
            cfStore.forceBlockingFlush(reason);
        }
    }

    public int repairAsync(String keyspace, Map<String, String> repairSpec)
    {
        return repair(keyspace, repairSpec, Collections.emptyList()).left;
    }

    public Pair<Integer, Future<?>> repair(String keyspace, Map<String, String> repairSpec, List<ProgressListener> listeners)
    {
        RepairOption option = RepairOption.parse(repairSpec, ClusterMetadata.current().partitioner);
        return repair(keyspace, option, listeners);
    }

    public Pair<Integer, Future<?>> repair(String keyspace, RepairOption option, List<ProgressListener> listeners)
    {
        // if ranges are not specified
        if (option.getRanges().isEmpty())
        {
            if (option.isPrimaryRange())
            {
                // when repairing only primary range, neither dataCenters nor hosts can be set
                if (option.getDataCenters().isEmpty() && option.getHosts().isEmpty())
                    option.getRanges().addAll(getPrimaryRanges(keyspace));
                    // except dataCenters only contain local DC (i.e. -local)
                else if (option.isInLocalDCOnly())
                    option.getRanges().addAll(getPrimaryRangesWithinDC(keyspace));
                else
                    throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
            }
            else
            {
                Iterables.addAll(option.getRanges(), getLocalReplicas(keyspace).onlyFull().ranges());
            }
        }
        if (option.getRanges().isEmpty() || Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor().allReplicas < 2)
            return Pair.create(0, ImmediateFuture.success(null));

        int cmd = nextRepairCommand.incrementAndGet();
        return Pair.create(cmd, repairCommandExecutor().submit(createRepairTask(cmd, keyspace, option, listeners)));
    }

    /**
     * Create collection of ranges that match ring layout from given tokens.
     *
     * @param beginToken beginning token of the range
     * @param endToken end token of the range
     * @return collection of ranges that match ring layout in TokenMetadata
     */
    @VisibleForTesting
    Collection<Range<Token>> createRepairRangeFrom(String beginToken, String endToken)
    {
        Token parsedBeginToken = getTokenFactory().fromString(beginToken);
        Token parsedEndToken = getTokenFactory().fromString(endToken);

        // Break up given range to match ring layout in TokenMetadata
        ArrayList<Range<Token>> repairingRange = new ArrayList<>();

        ArrayList<Token> tokens = new ArrayList<>(ClusterMetadata.current().tokenMap.tokens());
        if (!tokens.contains(parsedBeginToken))
        {
            tokens.add(parsedBeginToken);
        }
        if (!tokens.contains(parsedEndToken))
        {
            tokens.add(parsedEndToken);
        }
        // tokens now contain all tokens including our endpoints
        Collections.sort(tokens);

        int start = tokens.indexOf(parsedBeginToken), end = tokens.indexOf(parsedEndToken);
        for (int i = start; i != end; i = (i+1) % tokens.size())
        {
            Range<Token> range = new Range<>(tokens.get(i), tokens.get((i+1) % tokens.size()));
            repairingRange.add(range);
        }

        return repairingRange;
    }

    public TokenFactory getTokenFactory()
    {
        return ClusterMetadata.current().partitioner.getTokenFactory();
    }

    private FutureTask<Object> createRepairTask(final int cmd, final String keyspace, final RepairOption options, List<ProgressListener> listeners)
    {
        if (!options.getDataCenters().isEmpty() && !options.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter()))
        {
            throw new IllegalArgumentException("the local data center must be part of the repair; requested " + options.getDataCenters() + " but DC is " + DatabaseDescriptor.getLocalDataCenter());
        }
        Set<String> existingDatacenters = ClusterMetadata.current().directory.allDatacenterEndpoints().keys().elementSet();
        List<String> datacenters = new ArrayList<>(options.getDataCenters());
        if (!existingDatacenters.containsAll(datacenters))
        {
            datacenters.removeAll(existingDatacenters);
            throw new IllegalArgumentException("data center(s) " + datacenters.toString() + " not found");
        }

        RepairCoordinator task = new RepairCoordinator(this, cmd, options, keyspace);
        task.addProgressListener(progressSupport);
        for (ProgressListener listener : listeners)
            task.addProgressListener(listener);

        if (options.isTraced())
            return new FutureTaskWithResources<>(() -> ExecutorLocals::clear, task);
        return new FutureTask<>(task);
    }

    private void tryRepairPaxosForTopologyChange(String reason)
    {
        try
        {
            startRepairPaxosForTopologyChange(reason).get();
        }
        catch (InterruptedException e)
        {
            logger.error("Error during paxos repair", e);
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            logger.error("Error during paxos repair", e);
            throw new RuntimeException(e);
        }
    }

    public void repairPaxosForTopologyChange(String reason)
    {
        if (getSkipPaxosRepairOnTopologyChange() || !Paxos.useV2())
        {
            logger.info("skipping paxos repair for {}. skip_paxos_repair_on_topology_change is set", reason);
            return;
        }

        logger.info("repairing paxos for {}", reason);

        int retries = 0;
        int maxRetries = PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_RETRIES.getInt();
        int delaySec = PAXOS_REPAIR_ON_TOPOLOGY_CHANGE_RETRY_DELAY_SECONDS.getInt();

        boolean completed = false;
        while (!completed)
        {
            try
            {
                tryRepairPaxosForTopologyChange(reason);
                completed = true;
            }
            catch (Exception e)
            {
                if (retries >= maxRetries)
                    throw e;

                retries++;
                int sleep = delaySec * retries;
                logger.info("Sleeping {} seconds before retrying paxos repair...", sleep);
                Uninterruptibles.sleepUninterruptibly(sleep, TimeUnit.SECONDS);
                logger.info("Retrying paxos repair for {}. Retry {}/{}", reason, retries, maxRetries);
            }
        }

        logger.info("paxos repair for {} complete", reason);
    }

    @VisibleForTesting
    public Future<?> startRepairPaxosForTopologyChange(String reason)
    {
        logger.info("repairing paxos for {}", reason);

        List<Future<?>> futures = new ArrayList<>();

        Keyspaces keyspaces = Schema.instance.getNonLocalStrategyKeyspaces();
        for (String ksName : keyspaces.names())
        {
            if (SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(ksName))
                continue;

            if (DatabaseDescriptor.skipPaxosRepairOnTopologyChangeKeyspaces().contains(ksName))
                continue;

            Collection<Range<Token>> ranges = getLocalAndPendingRanges(ksName);
            futures.add(ActiveRepairService.instance().repairPaxosForTopologyChange(ksName, ranges, reason));
        }

        return FutureCombiner.allOf(futures);
    }

    public Future<?> autoRepairPaxos(TableId tableId)
    {
        TableMetadata table = Schema.instance.getTableMetadata(tableId);
        if (table == null)
            return ImmediateFuture.success(null);

        Collection<Range<Token>> ranges = getLocalAndPendingRanges(table.keyspace);
        PaxosCleanupLocalCoordinator coordinator = PaxosCleanupLocalCoordinator.createForAutoRepair(tableId, ranges);
        ScheduledExecutors.optionalTasks.submit(coordinator::start);
        return coordinator;
    }

    public void forceTerminateAllRepairSessions()
    {
        ActiveRepairService.instance().terminateSessions();
    }

    @Nullable
    public List<String> getParentRepairStatus(int cmd)
    {
        Pair<ParentRepairStatus, List<String>> pair = ActiveRepairService.instance().getRepairStatus(cmd);
        return pair == null ? null :
               ImmutableList.<String>builder().add(pair.left.name()).addAll(pair.right).build();
    }

    @Deprecated
    @Override
    public void setRepairSessionMaxTreeDepth(int depth)
    {
        DatabaseDescriptor.setRepairSessionMaxTreeDepth(depth);
    }

    @Deprecated
    @Override
    public int getRepairSessionMaxTreeDepth()
    {
        return DatabaseDescriptor.getRepairSessionMaxTreeDepth();
    }

    @Override
    public void setRepairSessionMaximumTreeDepth(int depth)
    {
        try
        {
            DatabaseDescriptor.setRepairSessionMaxTreeDepth(depth);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /*
     * In CASSANDRA-17668, JMX setters that did not throw standard exceptions were deprecated in favor of ones that do.
     * For consistency purposes, the respective getter "getRepairSessionMaxTreeDepth" was also deprecated and replaced
     * by this method.
     */
    @Override
    public int getRepairSessionMaximumTreeDepth()
    {
        return DatabaseDescriptor.getRepairSessionMaxTreeDepth();
    }

    /* End of MBean interface methods */

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param cf Column family name
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    @Deprecated
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key)
    {
        EndpointsForToken replicas = getNaturalReplicasForToken(keyspaceName, cf, key);
        List<InetAddress> inetList = new ArrayList<>(replicas.size());
        replicas.forEach(r -> inetList.add(r.endpoint().getAddress()));
        return inetList;
    }

    public List<String> getNaturalEndpointsWithPort(String keyspaceName, String cf, String key)
    {
        return Replicas.stringify(getNaturalReplicasForToken(keyspaceName, cf, key), true);
    }

    @Deprecated
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key)
    {
        EndpointsForToken replicas = getNaturalReplicasForToken(keyspaceName, key);
        List<InetAddress> inetList = new ArrayList<>(replicas.size());
        replicas.forEach(r -> inetList.add(r.endpoint().getAddress()));
        return inetList;
    }

    public List<String> getNaturalEndpointsWithPort(String keyspaceName, ByteBuffer key)
    {
        EndpointsForToken replicas = getNaturalReplicasForToken(keyspaceName, key);
        return Replicas.stringify(replicas, true);
    }

    public EndpointsForToken getNaturalReplicasForToken(String keyspaceName, String cf, String key)
    {
        return getNaturalReplicasForToken(keyspaceName, partitionKeyToBytes(keyspaceName, cf, key));
    }

    public DecoratedKey getKeyFromPartition(String keyspaceName, String table, String partitionKey)
    {
        return ClusterMetadata.current().partitioner.decorateKey(partitionKeyToBytes(keyspaceName, table, partitionKey));
    }

    private static ByteBuffer partitionKeyToBytes(String keyspaceName, String cf, String key)
    {
        KeyspaceMetadata ksMetaData = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (ksMetaData == null)
            throw new IllegalArgumentException("Unknown keyspace '" + keyspaceName + "'");

        TableMetadata metadata = ksMetaData.getTableOrViewNullable(cf);
        if (metadata == null)
            throw new IllegalArgumentException("Unknown table '" + cf + "' in keyspace '" + keyspaceName + "'");

        return metadata.partitionKeyType.fromString(key);
    }

    @Override
    public String getToken(String keyspaceName, String table, String key)
    {
        return ClusterMetadata.current().partitioner.getToken(partitionKeyToBytes(keyspaceName, table, key)).toString();
    }

    public EndpointsForToken getNaturalReplicasForToken(String keyspaceName, ByteBuffer key)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Token token = metadata.partitioner.getToken(key);
        KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspaceName);
        return metadata.placements.get(keyspaceMetadata.params.replication).reads.forToken(token);
    }

    public boolean isEndpointValidForWrite(String keyspace, Token token)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspace);
        return keyspaceMetadata != null && metadata.placements.get(keyspaceMetadata.params.replication).reads.forToken(token).containsSelf();
    }

    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception
    {
        LoggingSupportFactory.getLoggingSupport().setLoggingLevel(classQualifier, rawLevel);
    }

    /**
     * @return the runtime logging levels for all the configured loggers
     */
    @Override
    public Map<String,String> getLoggingLevels()
    {
        return LoggingSupportFactory.getLoggingSupport().getLoggingLevels();
    }

    /**
     * @return list of Token ranges (_not_ keys!) together with estimated key count,
     *      breaking up the data this node is responsible for into pieces of roughly keysPerSplit
     */
    public List<Pair<Range<Token>, Long>> getSplits(String keyspaceName, String cfName, Range<Token> range, int keysPerSplit)
    {
        Keyspace t = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        List<DecoratedKey> keys = keySamples(Collections.singleton(cfs), range);

        long totalRowCountEstimate = cfs.estimatedKeysForRange(range);

        // splitCount should be much smaller than number of key samples, to avoid huge sampling error
        int minSamplesPerSplit = 4;
        int maxSplitCount = keys.size() / minSamplesPerSplit + 1;
        int splitCount = Math.max(1, Math.min(maxSplitCount, (int)(totalRowCountEstimate / keysPerSplit)));

        List<Token> tokens = keysToTokens(range, keys);
        return getSplits(tokens, splitCount, cfs);
    }

    private List<Pair<Range<Token>, Long>> getSplits(List<Token> tokens, int splitCount, ColumnFamilyStore cfs)
    {
        double step = (double) (tokens.size() - 1) / splitCount;
        Token prevToken = tokens.get(0);
        List<Pair<Range<Token>, Long>> splits = Lists.newArrayListWithExpectedSize(splitCount);
        for (int i = 1; i <= splitCount; i++)
        {
            int index = (int) Math.round(i * step);
            Token token = tokens.get(index);
            Range<Token> range = new Range<>(prevToken, token);
            // always return an estimate > 0 (see CASSANDRA-7322)
            splits.add(Pair.create(range, Math.max(cfs.metadata().params.minIndexInterval, cfs.estimatedKeysForRange(range))));
            prevToken = token;
        }
        return splits;
    }

    private List<Token> keysToTokens(Range<Token> range, List<DecoratedKey> keys)
    {
        List<Token> tokens = Lists.newArrayListWithExpectedSize(keys.size() + 2);
        tokens.add(range.left);
        for (DecoratedKey key : keys)
            tokens.add(key.getToken());
        tokens.add(range.right);
        return tokens;
    }

    private List<DecoratedKey> keySamples(Iterable<ColumnFamilyStore> cfses, Range<Token> range)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfses)
            Iterables.addAll(keys, cfs.keySamples(range));
        FBUtilities.sortSampledKeys(keys, range);
        return keys;
    }

    public void maybeHandoverCMS(ClusterMetadata metadata, InetAddressAndPort toRemove)
    {
        Set<InetAddressAndPort> cmsMembers = metadata.placements.get(ReplicationParams.meta()).reads.byEndpoint().keySet();
        if (cmsMembers.contains(toRemove))
        {
            if (metadata.directory.peerIds().size() == cmsMembers.size())
                throw new IllegalStateException(String.format("Cannot complete operation as it will remove %1$s from the " +
                                                              "Cluster Metadata Service, but there are no available " +
                                                              "candidates to replace it. To continue, either expand the " +
                                                              "cluster first or manually remove %1$s from the CMS to " +
                                                              "temporarily reduce the replication factor of the CMS " +
                                                              "keyspace. This operation may then be retried.",
                                                              toRemove));

            boolean nominated = false;
            Set<InetAddressAndPort> tried = new HashSet<>();
            for (Entry<NodeId, NodeAddresses> e : metadata.directory.addresses.entrySet())
            {
                InetAddressAndPort addr = e.getValue().broadcastAddress;
                if (!cmsMembers.contains(addr))
                {
                    logger.info("Nominating an alternative CMS node ({}) before decommission.", addr);
                    try
                    {
                        tried.add(addr);
                        AddToCMS.initiate(e.getKey(), addr);
                        nominated = true;
                        break;
                    }
                    catch (IllegalStateException t)
                    {
                        logger.error("Could not successfully nominate " + addr, t);
                    }
                }
            }
            // TODO: implement a test for unbootstrap and alternative nomination with alternatives being down
            // besides, this _still_ can easily nominate a down node.
            if (!nominated)
                throw new IllegalStateException(String.format("Could not nominate an alternative CMS node. Tried:%s", tried));

            // We can force removal from the CMS as it doesn't alter the size of the service
            Epoch epoch = ClusterMetadataService.instance().commit(new RemoveFromCMS(toRemove, true)).epoch;
            // Awaiting on the progress barrier will leave a log message in case it could not collect a majority. But we do not
            // want to block the operation at that point, since for the purpose of executing CMS operations, we have already
            // stopped being a CMS node, and for the purpose of either continuing or starting a leave sequence, we will not
            // be able to collect a majority of CMS nodes during commit.
            new ProgressBarrier(epoch, metadata.directory.location(metadata.myNodeId()), EntireRange.affectedRanges).await();
        }
    }

    public void decommission(boolean force)
    {
        decommission(force, true);
    }

    @VisibleForTesting
    public void decommission(boolean force, boolean shutdownNetworking)
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't decommission until that is done.");
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();

        maybeHandoverCMS(metadata, getBroadcastAddressAndPort());
        InProgressSequence<?> inProgress = metadata.inProgressSequences.get(self);

        if (inProgress == null)
        {
            logger.info("starting decom with {} {}", metadata.epoch, self);
            ClusterMetadataService.instance().commit(new PrepareLeave(self,
                                                                      force,
                                                                      ClusterMetadataService.instance().placementProvider(),
                                                                      LeaveStreams.Kind.UNBOOTSTRAP),
                                                     (metadata_) -> null,
                                                     (metadata_, code, reason) -> {
                                                         InProgressSequence<?> sequence = metadata_.inProgressSequences.get(self);
                                                         // We might have discovered a sequence we ourselves committed but got no response for
                                                         if (sequence == null || sequence.kind() != InProgressSequences.Kind.LEAVE)
                                                         {
                                                             throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting leave sequence.",
                                                                                                           reason));
                                                         }
                                                         return null;
                                                     });
        }
        else if (!InProgressSequences.isLeave(inProgress))
        {
            throw new IllegalArgumentException("Can not decomission a node that has an in-progress sequence");
        }

        finishInProgressSequences(self);
        if (shutdownNetworking)
            shutdownNetworking();
    }

    public void shutdownNetworking()
    {
        shutdownClientServers();
        Gossiper.instance.stop();
        try
        {
            MessagingService.instance().shutdown();
        }
        catch (IOError ioe)
        {
            logger.info("failed to shutdown message service", ioe);
        }

        Stage.shutdownNow();
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.DECOMMISSIONED);
        transientMode = Optional.of(Mode.DECOMMISSIONED);
        logger.info("{}", Mode.DECOMMISSIONED);
        // let op be responsible for killing the process
    }

    public Future<?> streamHints()
    {
        return HintsService.instance.transferHints(this::getPreferredHintsStreamTarget);
    }

    private static EndpointsForRange getStreamCandidates(Collection<InetAddressAndPort> endpoints)
    {
        endpoints = endpoints.stream()
                             .filter(endpoint -> FailureDetector.instance.isAlive(endpoint) && !getBroadcastAddressAndPort().equals(endpoint))
                             .collect(Collectors.toList());

        return SystemReplicas.getSystemReplicas(endpoints);
    }
    /**
     * Find the best target to stream hints to. Currently the closest peer according to the snitch
     */
    private UUID getPreferredHintsStreamTarget()
    {
        ClusterMetadata metadata = ClusterMetadata.current();

        Set<InetAddressAndPort> endpoints = metadata.directory.states.entrySet().stream()
                                                                            .filter(e -> e.getValue() != NodeState.LEAVING)
                                                                            .map(e -> metadata.directory.endpoint(e.getKey()))
                                                                            .collect(toSet());

        EndpointsForRange candidates = getStreamCandidates(endpoints);
        if (candidates.isEmpty())
        {
            logger.warn("Unable to stream hints since no live endpoints seen");
            throw new RuntimeException("Unable to stream hints since no live endpoints seen");
        }
        else
        {
            // stream to the closest peer as chosen by the snitch
            candidates = DatabaseDescriptor.getEndpointSnitch().sortedByProximity(getBroadcastAddressAndPort(), candidates);
            InetAddressAndPort hintsDestinationHost = candidates.get(0).endpoint();
            return ClusterMetadata.current().directory.peerId(hintsDestinationHost).toUUID();
        }
    }

    public void move(String newToken)
    {
        try
        {
            getTokenFactory().validate(newToken);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
        move(getTokenFactory().fromString(newToken));
    }

    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param newToken new token to boot to, or if null, find balanced token to boot to
     *
     * @throws IOException on any I/O operation error
     */
    private void move(Token newToken)
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't move until that is done.");

        if (newToken == null)
            throw new IllegalArgumentException("Can't move to the undefined (null) token.");

        if (ClusterMetadata.current().tokenMap.tokens().contains(newToken))
            throw new IllegalArgumentException(String.format("target token %s is already owned by another node.", newToken));

        // address of the current node
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();
        // This doesn't make any sense in a vnodes environment.
        if (metadata.tokenMap.tokens(self).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }

        ClusterMetadataService.instance().commit(new PrepareMove(self,
                                                                 Collections.singleton(newToken),
                                                                 ClusterMetadataService.instance().placementProvider(),
                                                                 true),
                                                 (metadata_) -> null,
                                                 (metadata_, code, reason) -> {
                                                     InProgressSequence<?> sequence = metadata_.inProgressSequences.get(self);
                                                     // We might have discovered a startup sequence we ourselves committed but got no response for
                                                     if (sequence == null || sequence.kind() != InProgressSequences.Kind.MOVE)
                                                     {
                                                         throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting leave sequence.",
                                                                                                       reason));
                                                     }
                                                     return null;
                                                 });
        finishInProgressSequences(self);

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", getLocalTokens().iterator().next());
    }

    public String getRemovalStatus()
    {
        return getRemovalStatus(false);
    }

    public String getRemovalStatusWithPort()
    {
        return getRemovalStatus(true);
    }

    /**
     * Get the status of a token removal.
     */
    private String getRemovalStatus(boolean withPort)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        StringBuilder sb = new StringBuilder();
        boolean found = false;
        for (Map.Entry<NodeId, NodeState> stateEntry : metadata.directory.states.entrySet())
        {
            NodeId nodeId = stateEntry.getKey();
            NodeState state = stateEntry.getValue();
            if (state == LEAVING)
            {
                InProgressSequence<?> seq = metadata.inProgressSequences.get(nodeId);
                if (seq != null && seq.kind() == InProgressSequences.Kind.REMOVE)
                {
                    sb.append("Removing node ").append(nodeId).append(" (").append(metadata.directory.endpoint(nodeId)).append(')').append(": ").append(seq.status());
                    found = true;
                }
            }
        }
        if (!found)
            sb.append("No removals in progress.");
        return sb.toString();
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeNode() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
        throw new IllegalStateException("Can't force remove completion, abort the remove operation and retry.");
    }

    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeNode should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString Host ID for the node
     */
    public void removeNode(String hostIdString)
    {
        removeNode(hostIdString, false);
    }

    public void removeNode(String hostIdString, boolean force)
    {
        NodeId toRemove = NodeId.fromString(hostIdString);
        removeNode(toRemove, force);
    }

    public void removeNode(NodeId toRemove, boolean force)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (toRemove.equals(metadata.myNodeId()))
            throw new UnsupportedOperationException("Cannot remove self");
        InetAddressAndPort endpoint = metadata.directory.endpoint(toRemove);
        if (endpoint == null)
            throw new UnsupportedOperationException("Host ID not found.");
        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");

        NodeState removeState = metadata.directory.peerState(toRemove);
        if (removeState == null)
            throw new UnsupportedOperationException("Node to be removed is not a member of the token ring");
        if (removeState == LEAVING)
            logger.warn("Node {} is already leaving or being removed, continuing removal anyway", endpoint);

        if (metadata.inProgressSequences.contains(toRemove))
            throw new IllegalArgumentException("Can not remove a node that has an in-progress sequence");

        maybeHandoverCMS(metadata, endpoint);

        logger.info("starting removenode with {} {}", metadata.epoch, toRemove);

        ClusterMetadataService.instance().commit(new PrepareLeave(toRemove,
                                                                  force,
                                                                  ClusterMetadataService.instance().placementProvider(),
                                                                  LeaveStreams.Kind.REMOVENODE),
                                                 (metadata_) -> null,
                                                 (metadata_, code, reason) -> {
                                                     InProgressSequence<?> sequence = metadata_.inProgressSequences.get(toRemove);
                                                     // We might have discovered a startup sequence we ourselves committed but got no response for
                                                     if (sequence == null || sequence.kind() != InProgressSequences.Kind.REMOVE)
                                                     {
                                                         throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting removenode sequence.",
                                                                                                       reason));
                                                     }
                                                     return null;
                                                 });
        finishInProgressSequences(toRemove);
    }

    public void assassinateEndpoint(String address)
    {
        try
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
            NodeId nodeId = ClusterMetadata.current().directory.peerId(endpoint);
            ClusterMetadataService.instance().commit(new Assassinate(nodeId,
                                                                     ClusterMetadataService.instance().placementProvider()),
                                                     (metadata_) -> null,
                                                     (metadata_, code, reason) -> {
                                                         if (metadata_.directory.peerIds().contains(nodeId))
                                                         {
                                                             throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting assassinate node.",
                                                                                                           reason));
                                                         }
                                                         return null;
                                                     });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Could not assassinate an unresolvable endpoint");
        }
    }

    public void confirmReplication(InetAddressAndPort node)
    {
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!replicatingNodes.isEmpty())
        {
            replicatingNodes.remove(node);
        }
        else
        {
            logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
        }
    }

    // TODO - still necessary?
    public void markDecommissionFailed()
    {
        logger.info(DECOMMISSION_FAILED.toString());
        transientMode = Optional.of(DECOMMISSION_FAILED);
    }

    /*
    - Use system_views.local to get information about the node (todo: we might still need a jmx endpoint for that since you can't run cql queries on drained etc nodes)
     */
    @Deprecated
    public String getOperationMode()
    {
        return operationMode().toString();
    }

    public Mode operationMode()
    {
        if (!isInitialized())
            return Mode.STARTING;

        if (transientMode.isPresent())
            return transientMode.get();

        NodeState nodeState = ClusterMetadata.current().myNodeState();
        switch (nodeState)
        {
            case REGISTERED:
            case BOOT_REPLACING:
            case BOOTSTRAPPING:
                return Mode.JOINING;
            case JOINED:
                return Mode.NORMAL;
            case LEAVING:
                return Mode.LEAVING;
            case LEFT:
                return Mode.DECOMMISSIONED;
            case MOVING:
                return Mode.MOVING;
        }
        throw new IllegalStateException("Bad node state: " + nodeState);
    }

    public boolean isStarting()
    {
        return operationMode() == Mode.STARTING;
    }

    public boolean isMoving()
    {
        return operationMode() == Mode.MOVING;
    }

    public boolean isJoining()
    {
        return operationMode() == Mode.JOINING;
    }

    public boolean isDrained()
    {
        return operationMode() == Mode.DRAINED;
    }

    public boolean isDraining()
    {
        return operationMode() == Mode.DRAINING;
    }

    public boolean isNormal()
    {
        return operationMode() == Mode.NORMAL;
    }

    public boolean isDecommissioned()
    {
        return operationMode == DECOMMISSIONED;
    }

    public boolean isDecommissionFailed()
    {
        return operationMode == DECOMMISSION_FAILED;
    }

    public boolean isDecommissioning()
    {
        return operationMode == Mode.LEAVING || operationMode == DECOMMISSION_FAILED;
    }

    public boolean isBootstrapFailed()
    {
        return operationMode == JOINING_FAILED;
    }

    public String getDrainProgress()
    {
        return String.format("Drained %s/%s ColumnFamilies", remainingCFs, totalCFs);
    }

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     */
    public synchronized void drain() throws IOException, InterruptedException, ExecutionException
    {
        drain(false);
    }

    protected synchronized void drain(boolean isFinalShutdown) throws IOException, InterruptedException, ExecutionException
    {
        if (Stage.areMutationExecutorsTerminated())
        {
            if (!isFinalShutdown)
                logger.warn("Cannot drain node (did it already happen?)");
            return;
        }

        assert !isShutdown;
        isShutdown = true;

        Throwable preShutdownHookThrowable = Throwables.perform(null, preShutdownHooks.stream().map(h -> h::run));
        if (preShutdownHookThrowable != null)
            logger.error("Attempting to continue draining after pre-shutdown hooks returned exception", preShutdownHookThrowable);

        try
        {
            String msg = "starting drain process";
            if (!isFinalShutdown)
                logger.info(msg);
            else
                logger.debug(msg);
            transientMode = Optional.of(Mode.DRAINING);

            try
            {
                /* not clear this is reasonable time, but propagated from prior embedded behaviour */
                BatchlogManager.instance.shutdownAndWait(1L, MINUTES);
            }
            catch (TimeoutException t)
            {
                logger.error("Batchlog manager timed out shutting down", t);
            }

            snapshotManager.stop();
            HintsService.instance.pauseDispatch();

            if (daemon != null)
                shutdownClientServers();
            ScheduledExecutors.optionalTasks.shutdown();
            Gossiper.instance.stop();
            ActiveRepairService.instance().stop();

            if (!isFinalShutdown)
            {
                logger.debug("shutting down MessageService");
                transientMode = Optional.of(Mode.DRAINING);
            }

            // In-progress writes originating here could generate hints to be written,
            // which is currently scheduled on the mutation stage. So shut down MessagingService
            // before mutation stage, so we can get all the hints saved before shutting down.
            try
            {
                MessagingService.instance().shutdown();
            }
            catch (Throwable t)
            {
                // prevent messaging service timing out shutdown from aborting
                // drain process; otherwise drain and/or shutdown might throw
                logger.error("Messaging service timed out shutting down", t);
            }

            if (!isFinalShutdown)
            {
                logger.debug("clearing mutation stage");
                transientMode = Optional.of(Mode.DRAINING);
            }
            Stage.shutdownAndAwaitMutatingExecutors(false,
                                                    DRAIN_EXECUTOR_TIMEOUT_MS.getInt(), TimeUnit.MILLISECONDS);

            StorageProxy.instance.verifyNoHintsInProgress();

            if (!isFinalShutdown)
            {
                logger.debug("flushing column families");
                transientMode = Optional.of(Mode.DRAINING);
            }

            // we don't want to start any new compactions while we are draining
            disableAutoCompaction();

            // count CFs first, since forceFlush could block for the flushWriter to get a queue slot empty
            totalCFs = 0;
            for (Keyspace keyspace : Keyspace.nonSystem())
                totalCFs += keyspace.getColumnFamilyStores().size();
            remainingCFs = totalCFs;
            // flush
            List<Future<?>> flushes = new ArrayList<>();
            for (Keyspace keyspace : Keyspace.nonSystem())
            {
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    flushes.add(cfs.forceFlush(ColumnFamilyStore.FlushReason.DRAIN));
            }
            // wait for the flushes.
            // TODO this is a godawful way to track progress, since they flush in parallel.  a long one could
            // thus make several short ones "instant" if we wait for them later.
            for (Future f : flushes)
            {
                try
                {
                    FBUtilities.waitOnFuture(f);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    // don't let this stop us from shutting down the commitlog and other thread pools
                    logger.warn("Caught exception while waiting for memtable flushes during shutdown hook", t);
                }

                remainingCFs--;
            }

            // Interrupt ongoing compactions and shutdown CM to prevent further compactions.
            CompactionManager.instance.forceShutdown();
            // Flush the system tables after all other tables are flushed, just in case flushing modifies any system state
            // like CASSANDRA-5151. Don't bother with progress tracking since system data is tiny.
            // Flush system tables after stopping compactions since they modify
            // system tables (for example compactions can obsolete sstables and the tidiers in SSTableReader update
            // system tables, see SSTableReader.GlobalTidy)
            flushes.clear();
            for (Keyspace keyspace : Keyspace.system())
            {
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    flushes.add(cfs.forceFlush(ColumnFamilyStore.FlushReason.DRAIN));
            }
            FBUtilities.waitOnFutures(flushes);

            SnapshotManager.shutdownAndWait(1L, MINUTES);
            HintsService.instance.shutdownBlocking();

            // Interrupt ongoing compactions and shutdown CM to prevent further compactions.
            CompactionManager.instance.forceShutdown();

            // whilst we've flushed all the CFs, which will have recycled all completed segments, we want to ensure
            // there are no segments to replay, so we force the recycling of any remaining (should be at most one)
            CommitLog.instance.forceRecycleAllSegments();

            CommitLog.instance.shutdownBlocking();

            // wait for miscellaneous tasks like sstable and commitlog segment deletion
            ColumnFamilyStore.shutdownPostFlushExecutor();

            try
            {
                // we are not shutting down ScheduledExecutors#scheduledFastTasks to be still able to progress time
                // fast-tasks executor is shut down in StorageService's shutdown hook added to Runtime
                ExecutorUtils.shutdownNowAndWait(1, MINUTES,
                                                 ScheduledExecutors.nonPeriodicTasks,
                                                 ScheduledExecutors.scheduledTasks,
                                                 ScheduledExecutors.optionalTasks);
            }
            finally
            {
                if (!isFinalShutdown)
                    logger.info("{}", Mode.DRAINING);
                else
                    logger.debug("{}", Mode.DRAINING);
                transientMode = Optional.of(Mode.DRAINED);
            }
        }
        catch (Throwable t)
        {
            logger.error("Caught an exception while draining ", t);
        }
        finally
        {
            Throwable postShutdownHookThrowable = Throwables.perform(null, postShutdownHooks.stream().map(h -> h::run));
            if (postShutdownHookThrowable != null)
                logger.error("Post-shutdown hooks returned exception", postShutdownHookThrowable);
        }
    }

    @VisibleForTesting
    public void disableAutoCompaction()
    {
        for (Keyspace keyspace : Keyspace.all())
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                cfs.disableAutoCompaction();
    }

    /**
     * Add a runnable which will be called before shut down or drain. This is useful for other
     * applications running in the same JVM which may want to shut down first rather than time
     * out attempting to use Cassandra calls which will no longer work.
     * @param hook: the code to run
     * @return true on success, false if Cassandra is already shutting down, in which case the runnable
     * has NOT been added.
     */
    public synchronized boolean addPreShutdownHook(Runnable hook)
    {
        if (!isDraining() && !isDrained())
            return preShutdownHooks.add(hook);

        return false;
    }

    /**
     * Remove a preshutdown hook
     */
    public synchronized boolean removePreShutdownHook(Runnable hook)
    {
        return preShutdownHooks.remove(hook);
    }

    /**
     * Add a runnable which will be called after shutdown or drain. This is useful for other applications
     * running in the same JVM that Cassandra needs to work and should shut down later.
     * @param hook: the code to run
     * @return true on success, false if Cassandra is already shutting down, in which case the runnable has NOT been
     * added.
     */
    public synchronized boolean addPostShutdownHook(Runnable hook)
    {
        if (!isDraining() && !isDrained())
            return postShutdownHooks.add(hook);

        return false;
    }

    /**
     * Remove a postshutdownhook
     */
    public synchronized boolean removePostShutdownHook(Runnable hook)
    {
        return postShutdownHooks.remove(hook);
    }

    /**
     * Some services are shutdown during draining and we should not attempt to start them again.
     *
     * @param service - the name of the service we are trying to start.
     * @throws IllegalStateException - an exception that nodetool is able to convert into a message to display to the user
     */
    synchronized void checkServiceAllowedToStart(String service)
    {
        if (isDraining()) // when draining isShutdown is also true, so we check first to return a more accurate message
            throw new IllegalStateException(String.format("Unable to start %s because the node is draining.", service));

        if (isShutdown()) // do not rely on operationMode in case it gets changed to decomissioned or other
            throw new IllegalStateException(String.format("Unable to start %s because the node was drained.", service));

        if (!isNormal() && joinRing) // if the node is not joining the ring, it is gossipping-only member which is in STARTING state forever
            throw new IllegalStateException(String.format("Unable to start %s because the node is not in the normal state.", service));
    }

    // Never ever do this at home. Used by tests.
    @VisibleForTesting
    public IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = DatabaseDescriptor.setPartitionerUnsafe(newPartitioner);
        valueFactory = new VersionedValue.VersionedValueFactory(newPartitioner);
        return oldPartitioner;
    }

    public void truncate(String keyspace, String table) throws TimeoutException, IOException
    {
        verifyKeyspaceIsValid(keyspace);

        try
        {
            StorageProxy.truncateBlocking(keyspace, table);
        }
        catch (UnavailableException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    public Map<InetAddress, Float> getOwnership()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        List<Token> sortedTokens = metadata.tokenMap.tokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<>(metadata.tokenMap.partitioner().describeOwnership(sortedTokens));
        Map<InetAddress, Float> nodeMap = new LinkedHashMap<>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet())
        {
            NodeId nodeId = metadata.tokenMap.owner(entry.getKey());
            InetAddressAndPort endpoint = metadata.directory.endpoint(nodeId);
            Float tokenOwnership = entry.getValue();
            if (nodeMap.containsKey(endpoint.getAddress()))
                nodeMap.put(endpoint.getAddress(), nodeMap.get(endpoint.getAddress()) + tokenOwnership);
            else
                nodeMap.put(endpoint.getAddress(), tokenOwnership);
        }
        return nodeMap;
    }

    public Map<String, Float> getOwnershipWithPort()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        List<Token> sortedTokens = metadata.tokenMap.tokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<Token, Float>(metadata.tokenMap.partitioner().describeOwnership(sortedTokens));
        Map<String, Float> nodeMap = new LinkedHashMap<>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet())
        {
            NodeId nodeId = metadata.tokenMap.owner(entry.getKey());
            InetAddressAndPort endpoint = metadata.directory.endpoint(nodeId);
            Float tokenOwnership = entry.getValue();
            if (nodeMap.containsKey(endpoint.toString()))
                nodeMap.put(endpoint.toString(), nodeMap.get(endpoint.toString()) + tokenOwnership);
            else
                nodeMap.put(endpoint.toString(), tokenOwnership);
        }
        return nodeMap;
    }

    /**
     * Calculates ownership. If there are multiple DC's and the replication strategy is DC aware then ownership will be
     * calculated per dc, i.e. each DC will have total ring ownership divided amongst its nodes. Without replication
     * total ownership will be a multiple of the number of DC's and this value will then go up within each DC depending
     * on the number of replicas within itself. For DC unaware replication strategies, ownership without replication
     * will be 100%.
     *
     * @throws IllegalStateException when node is not configured properly.
     */
    private LinkedHashMap<InetAddressAndPort, Float> getEffectiveOwnership(String keyspace)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        AbstractReplicationStrategy strategy;
        if (keyspace != null)
        {
            if (isLocalSystemKeyspace(keyspace))
                throw new IllegalArgumentException("Ownership values for keyspaces with LocalStrategy are meaningless");

            KeyspaceMetadata keyspaceInstance = metadata.schema.getKeyspaces().getNullable(keyspace);
            if (keyspaceInstance == null)
                throw new IllegalArgumentException("The keyspace " + keyspace + ", does not exist");

            if (keyspaceInstance.replicationStrategy instanceof LocalStrategy)
                throw new IllegalStateException("Ownership values for keyspaces with LocalStrategy are meaningless");

            strategy = keyspaceInstance.replicationStrategy;
        }
        else
        {
            Set<String> userKeyspaces = metadata.schema.getKeyspaces()
                                                       .without(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
                                                       .names();

            if (userKeyspaces.size() > 0)
            {
                keyspace = userKeyspaces.iterator().next();
                AbstractReplicationStrategy replicationStrategy = Schema.instance.getKeyspaceInstance(keyspace).getReplicationStrategy();
                for (String keyspaceName : userKeyspaces)
                {
                    if (!Schema.instance.getKeyspaceInstance(keyspaceName).getReplicationStrategy().hasSameSettings(replicationStrategy))
                        throw new IllegalStateException("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
                }
            }

            if (keyspace == null)
            {
                keyspace = "system_traces";
            }

            Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
            if (keyspaceInstance == null)
                throw new IllegalStateException("The node does not have " + keyspace + " yet, probably still bootstrapping. Effective ownership information is meaningless.");
            strategy = keyspaceInstance.getReplicationStrategy();
        }

        if (strategy instanceof MetaStrategy)
        {
            LinkedHashMap<InetAddressAndPort, Float> ownership = Maps.newLinkedHashMap();
            metadata.placements.get(ReplicationParams.meta()).writes.byEndpoint().flattenValues().forEach((r) -> {
                ownership.put(r.endpoint(), 1.0f);
            });
            return ownership;
        }

        Collection<Collection<InetAddressAndPort>> endpointsGroupedByDc = new ArrayList<>();
        // mapping of dc's to nodes, use sorted map so that we get dcs sorted
        SortedMap<String, Collection<InetAddressAndPort>> sortedDcsToEndpoints = new TreeMap<>(ClusterMetadata.current().directory.allDatacenterEndpoints().asMap());
        for (Collection<InetAddressAndPort> endpoints : sortedDcsToEndpoints.values())
            endpointsGroupedByDc.add(endpoints);

        Map<Token, Float> tokenOwnership = metadata.partitioner.describeOwnership(metadata.tokenMap.tokens());
        LinkedHashMap<InetAddressAndPort, Float> finalOwnership = Maps.newLinkedHashMap();

        RangesByEndpoint endpointToRanges = strategy.getAddressReplicas(metadata);
        // calculate ownership per dc
        for (Collection<InetAddressAndPort> endpoints : endpointsGroupedByDc)
        {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (InetAddressAndPort endpoint : endpoints)
            {
                float ownership = 0.0f;
                for (Replica replica : endpointToRanges.get(endpoint))
                {
                    if (tokenOwnership.containsKey(replica.range().right))
                        ownership += tokenOwnership.get(replica.range().right);
                }
                finalOwnership.put(endpoint, ownership);
            }
        }
        return finalOwnership;
    }

    public LinkedHashMap<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException
    {
        LinkedHashMap<InetAddressAndPort, Float> result = getEffectiveOwnership(keyspace);
        LinkedHashMap<InetAddress, Float> asInets = new LinkedHashMap<>();
        result.entrySet().stream().forEachOrdered(entry -> asInets.put(entry.getKey().getAddress(), entry.getValue()));
        return asInets;
    }

    public LinkedHashMap<String, Float> effectiveOwnershipWithPort(String keyspace) throws IllegalStateException
    {
        LinkedHashMap<InetAddressAndPort, Float> result = getEffectiveOwnership(keyspace);
        LinkedHashMap<String, Float> asStrings = new LinkedHashMap<>();
        result.entrySet().stream().forEachOrdered(entry -> asStrings.put(entry.getKey().getHostAddressAndPort(), entry.getValue()));
        return asStrings;
    }

    public List<String> getKeyspaces()
    {
        return Lists.newArrayList(Schema.instance.distributedAndLocalKeyspaces().names());
    }

    public List<String> getNonSystemKeyspaces()
    {
        return Lists.newArrayList(Schema.instance.distributedKeyspaces().names());
    }

    public List<String> getNonLocalStrategyKeyspaces()
    {
        return Lists.newArrayList(Schema.instance.getNonLocalStrategyKeyspaces().names());
    }

    public Map<String, String> getViewBuildStatuses(String keyspace, String view, boolean withPort)
    {
        Map<UUID, String> coreViewStatus = SystemDistributedKeyspace.viewStatus(keyspace, view);
        Map<NodeId, NodeAddresses> hostIdToEndpoint = ClusterMetadata.current().directory.addresses;
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<NodeId, NodeAddresses> entry : hostIdToEndpoint.entrySet())
        {
            UUID hostId = entry.getKey().toUUID();
            InetAddressAndPort endpoint = entry.getValue().broadcastAddress;
            result.put(endpoint.toString(withPort),
                       coreViewStatus.getOrDefault(hostId, "UNKNOWN"));
        }

        return Collections.unmodifiableMap(result);
    }

    public Map<String, String> getViewBuildStatuses(String keyspace, String view)
    {
        return getViewBuildStatuses(keyspace, view, false);
    }

    public Map<String, String> getViewBuildStatusesWithPort(String keyspace, String view)
    {
        return getViewBuildStatuses(keyspace, view, true);
    }

    public void setDynamicUpdateInterval(int dynamicUpdateInterval)
    {
        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
        {

            try
            {
                updateSnitch(null, true, dynamicUpdateInterval, null, null);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public int getDynamicUpdateInterval()
    {
        return DatabaseDescriptor.getDynamicUpdateInterval();
    }

    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException
    {
        // apply dynamic snitch configuration
        if (dynamicUpdateInterval != null)
            DatabaseDescriptor.setDynamicUpdateInterval(dynamicUpdateInterval);
        if (dynamicResetInterval != null)
            DatabaseDescriptor.setDynamicResetInterval(dynamicResetInterval);
        if (dynamicBadnessThreshold != null)
            DatabaseDescriptor.setDynamicBadnessThreshold(dynamicBadnessThreshold);

        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();

        // new snitch registers mbean during construction
        if(epSnitchClassName != null)
        {

            // need to unregister the mbean _before_ the new dynamic snitch is instantiated (and implicitly initialized
            // and its mbean registered)
            if (oldSnitch instanceof DynamicEndpointSnitch)
                ((DynamicEndpointSnitch)oldSnitch).close();

            IEndpointSnitch newSnitch;
            try
            {
                newSnitch = DatabaseDescriptor.createEndpointSnitch(dynamic != null && dynamic, epSnitchClassName);
            }
            catch (ConfigurationException e)
            {
                throw new ClassNotFoundException(e.getMessage());
            }

            if (newSnitch instanceof DynamicEndpointSnitch)
            {
                logger.info("Created new dynamic snitch {} with update-interval={}, reset-interval={}, badness-threshold={}",
                            ((DynamicEndpointSnitch)newSnitch).subsnitch.getClass().getName(), DatabaseDescriptor.getDynamicUpdateInterval(),
                            DatabaseDescriptor.getDynamicResetInterval(), DatabaseDescriptor.getDynamicBadnessThreshold());
            }
            else
            {
                logger.info("Created new non-dynamic snitch {}", newSnitch.getClass().getName());
            }

            // point snitch references to the new instance
            DatabaseDescriptor.setEndpointSnitch(newSnitch);
        }
        else
        {
            if (oldSnitch instanceof DynamicEndpointSnitch)
            {
                logger.info("Applying config change to dynamic snitch {} with update-interval={}, reset-interval={}, badness-threshold={}",
                            ((DynamicEndpointSnitch)oldSnitch).subsnitch.getClass().getName(), DatabaseDescriptor.getDynamicUpdateInterval(),
                            DatabaseDescriptor.getDynamicResetInterval(), DatabaseDescriptor.getDynamicBadnessThreshold());

                DynamicEndpointSnitch snitch = (DynamicEndpointSnitch)oldSnitch;
                snitch.applyConfigChanges();
            }
        }
    }

    /**
     * Send data to the endpoints that will be responsible for it in the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    public Future<StreamState> streamRanges(Map<String, EndpointsByReplica> rangesToStreamByKeyspace)
    {
        // First, we build a list of ranges to stream to each host, per table
        Map<String, RangesByEndpoint> sessionsToStreamByKeyspace = new HashMap<>();

        for (Map.Entry<String, EndpointsByReplica> entry : rangesToStreamByKeyspace.entrySet())
        {
            String keyspace = entry.getKey();
            EndpointsByReplica rangesWithEndpoints = entry.getValue();

            if (rangesWithEndpoints.isEmpty())
                continue;

            //Description is always Unbootstrap? Is that right?
            Map<InetAddressAndPort, Set<Range<Token>>> transferredRangePerKeyspace = SystemKeyspace.getTransferredRanges("Unbootstrap",
                                                                                                                         keyspace,
                                                                                                                         ClusterMetadata.current().tokenMap.partitioner());
            RangesByEndpoint.Builder replicasPerEndpoint = new RangesByEndpoint.Builder();
            for (Map.Entry<Replica, Replica> endPointEntry : rangesWithEndpoints.flattenEntries())
            {
                Replica local = endPointEntry.getKey();
                Replica remote = endPointEntry.getValue();
                Set<Range<Token>> transferredRanges = transferredRangePerKeyspace.get(remote.endpoint());
                if (transferredRanges != null && transferredRanges.contains(local.range()))
                {
                    logger.debug("Skipping transferred range {} of keyspace {}, endpoint {}", local, keyspace, remote);
                    continue;
                }

                replicasPerEndpoint.put(remote.endpoint(), remote.decorateSubrange(local.range()));
            }

            sessionsToStreamByKeyspace.put(keyspace, replicasPerEndpoint.build());
        }

        StreamPlan streamPlan = new StreamPlan(StreamOperation.DECOMMISSION);

        // Vinculate StreamStateStore to current StreamPlan to update transferred ranges per StreamSession
        streamPlan.listeners(streamStateStore);

        for (Map.Entry<String, RangesByEndpoint> entry : sessionsToStreamByKeyspace.entrySet())
        {
            String keyspaceName = entry.getKey();
            RangesByEndpoint replicasPerEndpoint = entry.getValue();

            for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> rangesEntry : replicasPerEndpoint.asMap().entrySet())
            {
                RangesAtEndpoint replicas = rangesEntry.getValue();
                InetAddressAndPort newEndpoint = rangesEntry.getKey();

                // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                streamPlan.transferRanges(newEndpoint, keyspaceName, replicas);
            }
        }
        return streamPlan.execute();
    }

    public void bulkLoad(String directory)
    {
        try
        {
            bulkLoadInternal(directory).get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public String bulkLoadAsync(String directory)
    {
        return bulkLoadInternal(directory).planId.toString();
    }

    private StreamResultFuture bulkLoadInternal(String directory)
    {
        File dir = new File(directory);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Invalid directory " + directory);

        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            private String keyspace;

            public void init(String keyspace)
            {
                this.keyspace = keyspace;
                try
                {
                    for (Map.Entry<Range<Token>, EndpointsForRange> entry : getRangeToAddressMap(keyspace).entrySet())
                    {
                        Range<Token> range = entry.getKey();
                        EndpointsForRange replicas = entry.getValue();
                        Replicas.temporaryAssertFull(replicas);
                        for (InetAddressAndPort endpoint : replicas.endpoints())
                            addRangeForEndpoint(range, endpoint);
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            public TableMetadataRef getTableMetadata(String tableName)
            {
                return Schema.instance.getTableMetadataRef(keyspace, tableName);
            }
        };

        return new SSTableLoader(dir, client, new OutputHandler.LogOutput()).stream();
    }

    public void rescheduleFailedDeletions()
    {
        LifecycleTransaction.rescheduleFailedDeletions();
    }

    /**
     * #{@inheritDoc}
     */
    @Deprecated
    public void loadNewSSTables(String ksName, String cfName)
    {
        if (!isInitialized())
            throw new RuntimeException("Not yet initialized, can't load new sstables");
        verifyKeyspaceIsValid(ksName);
        ColumnFamilyStore.loadNewSSTables(ksName, cfName);
    }

    /**
     * #{@inheritDoc}
     */
    public List<String> sampleKeyRange() // do not rename to getter - see CASSANDRA-4452 for details
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (Keyspace keyspace : Keyspace.nonLocalStrategy())
        {
            for (Range<Token> range : getPrimaryRangesForEndpoint(keyspace.getName(), getBroadcastAddressAndPort()))
                keys.addAll(keySamples(keyspace.getColumnFamilyStores(), range));
        }

        List<String> sampledKeys = new ArrayList<>(keys.size());
        for (DecoratedKey key : keys)
            sampledKeys.add(key.getToken().toString());
        return sampledKeys;
    }

    @Override
    public Map<String, List<CompositeData>> samplePartitions(int duration, int capacity, int count, List<String> samplers) throws OpenDataException {
        return samplePartitions(null, duration, capacity, count, samplers);
    }

    /*
     * { "sampler_name": [ {table: "", count: i, error: i, value: ""}, ... ] }
     */
    @Override
    public Map<String, List<CompositeData>> samplePartitions(String keyspace, int durationMillis, int capacity, int count,
                                                             List<String> samplers) throws OpenDataException
    {
        ConcurrentHashMap<String, List<CompositeData>> result = new ConcurrentHashMap<>();
        Iterable<ColumnFamilyStore> tables = SamplingManager.getTables(keyspace, null);
        for (String sampler : samplers)
        {
            for (ColumnFamilyStore table : tables)
            {
                table.beginLocalSampling(sampler, capacity, durationMillis);
            }
        }
        Uninterruptibles.sleepUninterruptibly(durationMillis, MILLISECONDS);

        for (String sampler : samplers)
        {
            List<CompositeData> topk = new ArrayList<>();
            for (ColumnFamilyStore table : tables)
            {
                topk.addAll(table.finishLocalSampling(sampler, count));
            }
            Collections.sort(topk, new Ordering<CompositeData>()
            {
                public int compare(CompositeData left, CompositeData right)
                {
                    return Long.compare((long) right.get("count"), (long) left.get("count"));
                }
            });
            // sublist is not serializable for jmx
            topk = new ArrayList<>(topk.subList(0, Math.min(topk.size(), count)));
            result.put(sampler, topk);
        }
        return result;
    }

    @Override // Note from parent javadoc: ks and table are nullable
    public boolean startSamplingPartitions(String ks, String table, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        checkArgument(duration > 0, "Sampling duration %s must be positive.", duration);

        checkArgument(interval <= 0 || interval >= duration,
                                    "Sampling interval %s should be greater then or equals to duration %s if defined.",
                                    interval, duration);

        checkArgument(capacity > 0 && capacity <= 1024,
                                    "Sampling capacity %s must be positive and the max value is 1024 (inclusive).",
                                    capacity);

        checkArgument(count > 0 && count < capacity,
                                    "Sampling count %s must be positive and smaller than capacity %s.",
                                    count, capacity);

        checkArgument(!samplers.isEmpty(), "Samplers cannot be empty.");

        Set<Sampler.SamplerType> available = EnumSet.allOf(Sampler.SamplerType.class);
        samplers.forEach((x) -> checkArgument(available.contains(Sampler.SamplerType.valueOf(x)),
                                              "'%s' sampler is not available from: %s",
                                              x, Arrays.toString(Sampler.SamplerType.values())));
        return samplingManager.register(ks, table, duration, interval, capacity, count, samplers);
    }

    @Override
    public boolean stopSamplingPartitions(String ks, String table)
    {
        return samplingManager.unregister(ks, table);
    }

    @Override
    public List<String> getSampleTasks()
    {
        return samplingManager.allJobs();
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        String[] indices = asList(idxNames).stream()
                                           .map(p -> isIndexColumnFamily(p) ? getIndexName(p) : p)
                                           .collect(toList())
                                           .toArray(new String[idxNames.length]);

        ColumnFamilyStore.rebuildSecondaryIndex(ksName, cfName, indices);
    }

    public void resetLocalSchema() throws IOException
    {
        // TODO: remove method?
        //Schema.instance.resetLocalSchema();
    }

    public void reloadLocalSchema()
    {
        // TODO: remove method?
        //Schema.instance.reloadSchema();
    }

    public void setTraceProbability(double probability)
    {
        this.traceProbability = probability;
    }

    public double getTraceProbability()
    {
        return traceProbability;
    }

    public boolean shouldTraceProbablistically()
    {
        return traceProbability != 0 && ThreadLocalRandom.current().nextDouble() < traceProbability;
    }

    public void disableAutoCompaction(String ks, String... tables) throws IOException
    {
        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, tables))
        {
            cfs.disableAutoCompaction();
        }
    }

    public synchronized void enableAutoCompaction(String ks, String... tables) throws IOException
    {
        checkServiceAllowedToStart("auto compaction");

        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, tables))
        {
            cfs.enableAutoCompaction();
        }
    }

    public Map<String, Boolean> getAutoCompactionStatus(String ks, String... tables) throws IOException
    {
        Map<String, Boolean> status = new HashMap<String, Boolean>();
        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, tables))
            status.put(cfs.getTableName(), cfs.isAutoCompactionDisabled());
        return status;
    }

    /** Returns the name of the cluster */
    public String getClusterName()
    {
        return DatabaseDescriptor.getClusterName();
    }

    /** Returns the cluster partitioner */
    public String getPartitionerName()
    {
        return DatabaseDescriptor.getPartitionerName();
    }

    /** Negative number for disabled */
    public void setSSTablePreemptiveOpenIntervalInMB(int intervalInMB)
    {
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(intervalInMB);
    }

    /** This method can return negative number for disabled */
    public int getSSTablePreemptiveOpenIntervalInMB()
    {
        return DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
    }

    public boolean getMigrateKeycacheOnCompaction()
    {
        return DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
    }

    public void setMigrateKeycacheOnCompaction(boolean invalidateKeyCacheOnCompaction)
    {
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(invalidateKeyCacheOnCompaction);
    }

    public int getTombstoneWarnThreshold()
    {
        return DatabaseDescriptor.getTombstoneWarnThreshold();
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        DatabaseDescriptor.setTombstoneWarnThreshold(threshold);
        logger.info("updated tombstone_warn_threshold to {}", threshold);
    }

    public int getTombstoneFailureThreshold()
    {
        return DatabaseDescriptor.getTombstoneFailureThreshold();
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(threshold);
        logger.info("updated tombstone_failure_threshold to {}", threshold);
    }

    public int getCachedReplicaRowsWarnThreshold()
    {
        return DatabaseDescriptor.getCachedReplicaRowsWarnThreshold();
    }

    public void setCachedReplicaRowsWarnThreshold(int threshold)
    {
        DatabaseDescriptor.setCachedReplicaRowsWarnThreshold(threshold);
        logger.info("updated replica_filtering_protection.cached_rows_warn_threshold to {}", threshold);
    }

    public int getCachedReplicaRowsFailThreshold()
    {
        return DatabaseDescriptor.getCachedReplicaRowsFailThreshold();
    }

    public void setCachedReplicaRowsFailThreshold(int threshold)
    {
        DatabaseDescriptor.setCachedReplicaRowsFailThreshold(threshold);
        logger.info("updated replica_filtering_protection.cached_rows_fail_threshold to {}", threshold);
    }

    @Override
    public int getColumnIndexSizeInKiB()
    {
        return DatabaseDescriptor.getColumnIndexSizeInKiB();
    }

    @Override
    public void setColumnIndexSizeInKiB(int columnIndexSizeInKiB)
    {
        int oldValueInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        try
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(columnIndexSizeInKiB);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
        logger.info("Updated column_index_size to {} KiB (was {} KiB)", columnIndexSizeInKiB, oldValueInKiB);
    }

    @Deprecated
    @Override
    public void setColumnIndexSize(int columnIndexSizeInKB)
    {
        int oldValueInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(columnIndexSizeInKB);
        logger.info("Updated column_index_size to {} KiB (was {} KiB)", columnIndexSizeInKB, oldValueInKiB);
    }

    @Deprecated
    @Override
    public int getColumnIndexCacheSize()
    {
        return DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
    }

    @Deprecated
    @Override
    public void setColumnIndexCacheSize(int cacheSizeInKB)
    {
        DatabaseDescriptor.setColumnIndexCacheSize(cacheSizeInKB);
        logger.info("Updated column_index_cache_size to {}", cacheSizeInKB);
    }

    /*
     * In CASSANDRA-17668, JMX setters that did not throw standard exceptions were deprecated in favor of ones that do.
     * For consistency purposes, the respective getter "getColumnIndexCacheSize" was also deprecated and replaced by
     * this method.
     */
    @Override
    public int getColumnIndexCacheSizeInKiB()
    {
        return DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
    }

    @Override
    public void setColumnIndexCacheSizeInKiB(int cacheSizeInKiB)
    {
        try
        {
            DatabaseDescriptor.setColumnIndexCacheSize(cacheSizeInKiB);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
        logger.info("Updated column_index_cache_size to {}", cacheSizeInKiB);
    }

    public int getBatchSizeFailureThreshold()
    {
        return DatabaseDescriptor.getBatchSizeFailThresholdInKiB();
    }

    public void setBatchSizeFailureThreshold(int threshold)
    {
        DatabaseDescriptor.setBatchSizeFailThresholdInKiB(threshold);
        logger.info("updated batch_size_fail_threshold to {}", threshold);
    }

    @Deprecated
    @Override
    public int getBatchSizeWarnThreshold()
    {
        return DatabaseDescriptor.getBatchSizeWarnThresholdInKiB();
    }

    @Deprecated
    @Override
    public void setBatchSizeWarnThreshold(int threshold)
    {
        DatabaseDescriptor.setBatchSizeWarnThresholdInKiB(threshold);
        logger.info("Updated batch_size_warn_threshold to {}", threshold);
    }

    /*
     * In CASSANDRA-17668, JMX setters that did not throw standard exceptions were deprecated in favor of ones that do.
     * For consistency purposes, the respective getter "getBatchSizeWarnThreshold" was also deprecated and replaced by
     * this method.
     */
    @Override
    public int getBatchSizeWarnThresholdInKiB()
    {
        return DatabaseDescriptor.getBatchSizeWarnThresholdInKiB();
    }

    @Override
    public void setBatchSizeWarnThresholdInKiB(int thresholdInKiB)
    {
        try
        {
            DatabaseDescriptor.setBatchSizeWarnThresholdInKiB(thresholdInKiB);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }

        logger.info("Updated batch_size_warn_threshold to {}", thresholdInKiB);
    }

    public int getInitialRangeTombstoneListAllocationSize()
    {
        return DatabaseDescriptor.getInitialRangeTombstoneListAllocationSize();
    }

    public void setInitialRangeTombstoneListAllocationSize(int size)
    {
        if (size < 0 || size > 1024)
        {
            throw new IllegalStateException("Not updating initial_range_tombstone_allocation_size as it must be in the range [0, 1024] inclusive");
        }
        int originalSize = DatabaseDescriptor.getInitialRangeTombstoneListAllocationSize();
        DatabaseDescriptor.setInitialRangeTombstoneListAllocationSize(size);
        logger.info("Updated initial_range_tombstone_allocation_size from {} to {}", originalSize, size);
    }

    public double getRangeTombstoneResizeListGrowthFactor()
    {
        return DatabaseDescriptor.getRangeTombstoneListGrowthFactor();
    }

    public void setRangeTombstoneListResizeGrowthFactor(double growthFactor) throws IllegalStateException
    {
        if (growthFactor < 1.2 || growthFactor > 5)
        {
            throw new IllegalStateException("Not updating range_tombstone_resize_factor as growth factor must be in the range [1.2, 5.0] inclusive");
        }
        else
        {
            double originalGrowthFactor = DatabaseDescriptor.getRangeTombstoneListGrowthFactor();
            DatabaseDescriptor.setRangeTombstoneListGrowthFactor(growthFactor);
            logger.info("Updated range_tombstone_resize_factor from {} to {}", originalGrowthFactor, growthFactor);
        }
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        DatabaseDescriptor.setHintedHandoffThrottleInKiB(throttleInKB);
        logger.info("updated hinted_handoff_throttle to {} KiB", throttleInKB);
    }

    public boolean getTransferHintsOnDecommission()
    {
        return DatabaseDescriptor.getTransferHintsOnDecommission();
    }

    public void setTransferHintsOnDecommission(boolean enabled)
    {
        DatabaseDescriptor.setTransferHintsOnDecommission(enabled);
        logger.info("updated transfer_hints_on_decommission to {}", enabled);
    }

    @Override
    public void clearConnectionHistory()
    {
        daemon.clearConnectionHistory();
        logger.info("Cleared connection history");
    }
    public void disableAuditLog()
    {
        AuditLogManager.instance.disableAuditLog();
        logger.info("Auditlog is disabled");
    }

    @Deprecated
    public void enableAuditLog(String loggerName, String includedKeyspaces, String excludedKeyspaces, String includedCategories, String excludedCategories,
                               String includedUsers, String excludedUsers) throws ConfigurationException, IllegalStateException
    {
        enableAuditLog(loggerName, Collections.emptyMap(), includedKeyspaces, excludedKeyspaces, includedCategories, excludedCategories, includedUsers, excludedUsers,
                       Integer.MIN_VALUE, null, null, Long.MIN_VALUE, Integer.MIN_VALUE, null);
    }

    public void enableAuditLog(String loggerName, String includedKeyspaces, String excludedKeyspaces, String includedCategories, String excludedCategories,
                               String includedUsers, String excludedUsers, Integer maxArchiveRetries, Boolean block, String rollCycle,
                               Long maxLogSize, Integer maxQueueWeight, String archiveCommand) throws IllegalStateException
    {
        enableAuditLog(loggerName, Collections.emptyMap(), includedKeyspaces, excludedKeyspaces, includedCategories, excludedCategories, includedUsers, excludedUsers,
                       maxArchiveRetries, block, rollCycle, maxLogSize, maxQueueWeight, archiveCommand);
    }

    @Deprecated
    public void enableAuditLog(String loggerName, Map<String, String> parameters, String includedKeyspaces, String excludedKeyspaces, String includedCategories, String excludedCategories,
                               String includedUsers, String excludedUsers) throws ConfigurationException, IllegalStateException
    {
        enableAuditLog(loggerName, parameters, includedKeyspaces, excludedKeyspaces, includedCategories, excludedCategories, includedUsers, excludedUsers,
                       Integer.MIN_VALUE, null, null, Long.MIN_VALUE, Integer.MIN_VALUE, null);
    }

    public void enableAuditLog(String loggerName, Map<String, String> parameters, String includedKeyspaces, String excludedKeyspaces, String includedCategories, String excludedCategories,
                               String includedUsers, String excludedUsers, Integer maxArchiveRetries, Boolean block, String rollCycle,
                               Long maxLogSize, Integer maxQueueWeight, String archiveCommand) throws IllegalStateException
    {
        AuditLogOptions auditOptions = DatabaseDescriptor.getAuditLoggingOptions();
        if (archiveCommand != null && !auditOptions.allow_nodetool_archive_command)
            throw new ConfigurationException("Can't enable audit log archiving via nodetool unless audit_logging_options.allow_nodetool_archive_command is set to true");

        final AuditLogOptions options = new AuditLogOptions.Builder(auditOptions)
                                        .withEnabled(true)
                                        .withLogger(loggerName, parameters)
                                        .withIncludedKeyspaces(includedKeyspaces)
                                        .withExcludedKeyspaces(excludedKeyspaces)
                                        .withIncludedCategories(includedCategories)
                                        .withExcludedCategories(excludedCategories)
                                        .withIncludedUsers(includedUsers)
                                        .withExcludedUsers(excludedUsers)
                                        .withMaxArchiveRetries(maxArchiveRetries)
                                        .withBlock(block)
                                        .withRollCycle(rollCycle)
                                        .withMaxLogSize(maxLogSize)
                                        .withMaxQueueWeight(maxQueueWeight)
                                        .withArchiveCommand(archiveCommand)
                                        .build();

        AuditLogManager.instance.enable(options);
        logger.info("AuditLog is enabled with configuration: {}", options);
    }

    public boolean isAuditLogEnabled()
    {
        return AuditLogManager.instance.isEnabled();
    }

    public String getCorruptedTombstoneStrategy()
    {
        return DatabaseDescriptor.getCorruptedTombstoneStrategy().toString();
    }

    public void setCorruptedTombstoneStrategy(String strategy)
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.valueOf(strategy));
        logger.info("Setting corrupted tombstone strategy to {}", strategy);
    }

    @Override
    public long getNativeTransportMaxConcurrentRequestsInBytes()
    {
        return ClientResourceLimits.getGlobalLimit();
    }

    @Override
    public void setNativeTransportMaxConcurrentRequestsInBytes(long newLimit)
    {
        ClientResourceLimits.setGlobalLimit(newLimit);
    }

    @Override
    public long getNativeTransportMaxConcurrentRequestsInBytesPerIp()
    {
        return ClientResourceLimits.getEndpointLimit();
    }

    @Override
    public void setNativeTransportMaxConcurrentRequestsInBytesPerIp(long newLimit)
    {
        ClientResourceLimits.setEndpointLimit(newLimit);
    }

    @Override
    public int getNativeTransportMaxRequestsPerSecond()
    {
        return ClientResourceLimits.getNativeTransportMaxRequestsPerSecond();
    }

    @Override
    public void setNativeTransportMaxRequestsPerSecond(int newPerSecond)
    {
        ClientResourceLimits.setNativeTransportMaxRequestsPerSecond(newPerSecond);
    }

    @Override
    public void setNativeTransportRateLimitingEnabled(boolean enabled)
    {
        DatabaseDescriptor.setNativeTransportRateLimitingEnabled(enabled);
    }

    @Override
    public boolean getNativeTransportRateLimitingEnabled()
    {
        return DatabaseDescriptor.getNativeTransportRateLimitingEnabled();
    }

    @VisibleForTesting
    public void shutdownServer()
    {
        if (drainOnShutdown != null)
        {
            Runtime.getRuntime().removeShutdownHook(drainOnShutdown);
        }
    }

    @Override
    public void enableFullQueryLogger(String path, String rollCycle, Boolean blocking, int maxQueueWeight, long maxLogSize, String archiveCommand, int maxArchiveRetries)
    {
        FullQueryLoggerOptions fqlOptions = DatabaseDescriptor.getFullQueryLogOptions();
        path = path != null ? path : fqlOptions.log_dir;
        rollCycle = rollCycle != null ? rollCycle : fqlOptions.roll_cycle;
        blocking = blocking != null ? blocking : fqlOptions.block;
        maxQueueWeight = maxQueueWeight != Integer.MIN_VALUE ? maxQueueWeight : fqlOptions.max_queue_weight;
        maxLogSize = maxLogSize != Long.MIN_VALUE ? maxLogSize : fqlOptions.max_log_size;
        if (archiveCommand != null && !fqlOptions.allow_nodetool_archive_command)
            throw new ConfigurationException("Can't enable full query log archiving via nodetool unless full_query_logging_options.allow_nodetool_archive_command is set to true");
        archiveCommand = archiveCommand != null ? archiveCommand : fqlOptions.archive_command;
        maxArchiveRetries = maxArchiveRetries != Integer.MIN_VALUE ? maxArchiveRetries : fqlOptions.max_archive_retries;

        Preconditions.checkNotNull(path, "cassandra.yaml did not set log_dir and not set as parameter");
        FullQueryLogger.instance.enableWithoutClean(File.getPath(path), rollCycle, blocking, maxQueueWeight, maxLogSize, archiveCommand, maxArchiveRetries);
    }

    @Override
    public void resetFullQueryLogger()
    {
        FullQueryLogger.instance.reset(DatabaseDescriptor.getFullQueryLogOptions().log_dir);
    }

    @Override
    public void stopFullQueryLogger()
    {
        FullQueryLogger.instance.stop();
    }

    @Override
    public boolean isFullQueryLogEnabled()
    {
        return FullQueryLogger.instance.isEnabled();
    }

    @Override
    public CompositeData getFullQueryLoggerOptions()
    {
        return FullQueryLoggerOptionsCompositeData.toCompositeData(FullQueryLogger.instance.getFullQueryLoggerOptions());
    }

    @Override
    public Map<String, Set<InetAddress>> getOutstandingSchemaVersions()
    {
        throw new RuntimeException("Deprecated");
    }

    @Override
    public Map<String, Set<String>> getOutstandingSchemaVersionsWithPort()
    {
        throw new RuntimeException("Deprecated");
        //TODO
//        if (Schema.instance instanceof Schema)
//        {
//            SchemaUpdateHandler updateHandler = ((Schema) Schema.instance).updateHandler;
//        }
    }

    public boolean autoOptimiseIncRepairStreams()
    {
        return DatabaseDescriptor.autoOptimiseIncRepairStreams();
    }

    public void setAutoOptimiseIncRepairStreams(boolean enabled)
    {
        DatabaseDescriptor.setAutoOptimiseIncRepairStreams(enabled);
    }

    public boolean autoOptimiseFullRepairStreams()
    {
        return DatabaseDescriptor.autoOptimiseFullRepairStreams();
    }

    public void setAutoOptimiseFullRepairStreams(boolean enabled)
    {
        DatabaseDescriptor.setAutoOptimiseFullRepairStreams(enabled);
    }

    public boolean autoOptimisePreviewRepairStreams()
    {
        return DatabaseDescriptor.autoOptimisePreviewRepairStreams();
    }

    public void setAutoOptimisePreviewRepairStreams(boolean enabled)
    {
        DatabaseDescriptor.setAutoOptimisePreviewRepairStreams(enabled);
    }

    @Deprecated
    public int getTableCountWarnThreshold()
    {
        return (int) Converters.TABLE_COUNT_THRESHOLD_TO_GUARDRAIL.unconvert(Guardrails.instance.getTablesWarnThreshold());
    }

    @Deprecated
    public void setTableCountWarnThreshold(int value)
    {
        if (value < 0)
            throw new IllegalStateException("Table count warn threshold should be positive, not "+value);
        logger.info("Changing table count warn threshold from {} to {}", getTableCountWarnThreshold(), value);
        Guardrails.instance.setTablesThreshold((int) Converters.TABLE_COUNT_THRESHOLD_TO_GUARDRAIL.convert(value), 
                                               Guardrails.instance.getTablesFailThreshold());
    }

    @Deprecated
    public int getKeyspaceCountWarnThreshold()
    {
        return (int) Converters.KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL.unconvert(Guardrails.instance.getKeyspacesWarnThreshold());
    }

    @Deprecated
    public void setKeyspaceCountWarnThreshold(int value)
    {
        if (value < 0)
            throw new IllegalStateException("Keyspace count warn threshold should be positive, not "+value);
        logger.info("Changing keyspace count warn threshold from {} to {}", getKeyspaceCountWarnThreshold(), value);
        Guardrails.instance.setKeyspacesThreshold((int) Converters.KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL.convert(value),
                                                  Guardrails.instance.getKeyspacesFailThreshold());
    }

    @Override
    public void setCompactionTombstoneWarningThreshold(int count)
    {
        if (count < 0)
            throw new IllegalStateException("compaction tombstone warning threshold needs to be >= 0, not "+count);
        logger.info("Setting compaction_tombstone_warning_threshold to {}", count);
        Guardrails.instance.setPartitionTombstonesThreshold(count, Guardrails.instance.getPartitionTombstonesFailThreshold());
    }

    @Override
    public int getCompactionTombstoneWarningThreshold()
    {
        return Math.toIntExact(Guardrails.instance.getPartitionTombstonesWarnThreshold());
    }

    public void addSnapshot(TableSnapshot snapshot) {
        snapshotManager.addSnapshot(snapshot);
    }

    @Override
    public boolean getReadThresholdsEnabled()
    {
        return DatabaseDescriptor.getReadThresholdsEnabled();
    }

    @Override
    public void setReadThresholdsEnabled(boolean value)
    {
        DatabaseDescriptor.setReadThresholdsEnabled(value);
    }

    @Override
    public String getCoordinatorLargeReadWarnThreshold()
    {
        return toString(DatabaseDescriptor.getCoordinatorReadSizeWarnThreshold());
    }

    @Override
    public void setCoordinatorLargeReadWarnThreshold(String threshold)
    {
        DatabaseDescriptor.setCoordinatorReadSizeWarnThreshold(parseDataStorageSpec(threshold));
    }

    @Override
    public String getCoordinatorLargeReadAbortThreshold()
    {
        return toString(DatabaseDescriptor.getCoordinatorReadSizeFailThreshold());
    }

    @Override
    public void setCoordinatorLargeReadAbortThreshold(String threshold)
    {
        DatabaseDescriptor.setCoordinatorReadSizeFailThreshold(parseDataStorageSpec(threshold));
    }

    @Override
    public String getLocalReadTooLargeWarnThreshold()
    {
        return toString(DatabaseDescriptor.getLocalReadSizeWarnThreshold());
    }

    @Override
    public void setLocalReadTooLargeWarnThreshold(String threshold)
    {
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(parseDataStorageSpec(threshold));
    }

    @Override
    public String getLocalReadTooLargeAbortThreshold()
    {
        return toString(DatabaseDescriptor.getLocalReadSizeFailThreshold());
    }

    @Override
    public void setLocalReadTooLargeAbortThreshold(String threshold)
    {
        DatabaseDescriptor.setLocalReadSizeFailThreshold(parseDataStorageSpec(threshold));
    }

    @Override
    public String getRowIndexReadSizeWarnThreshold()
    {
        return toString(DatabaseDescriptor.getRowIndexReadSizeWarnThreshold());
    }

    @Override
    public void setRowIndexReadSizeWarnThreshold(String threshold)
    {
        DatabaseDescriptor.setRowIndexReadSizeWarnThreshold(parseDataStorageSpec(threshold));
    }

    @Override
    public String getRowIndexReadSizeAbortThreshold()
    {
        return toString(DatabaseDescriptor.getRowIndexReadSizeFailThreshold());
    }

    @Override
    public void setRowIndexReadSizeAbortThreshold(String threshold)
    {
        DatabaseDescriptor.setRowIndexReadSizeFailThreshold(parseDataStorageSpec(threshold));
    }

    private static String toString(DataStorageSpec value)
    {
        return value == null ? null : value.toString();
    }

    public void setDefaultKeyspaceReplicationFactor(int value)
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(value);
        logger.info("set default keyspace rf to {}", value);
    }

    private static DataStorageSpec.LongBytesBound parseDataStorageSpec(String threshold)
    {
        return threshold == null
               ? null
               : new DataStorageSpec.LongBytesBound(threshold);
    }

    public int getDefaultKeyspaceReplicationFactor()
    {
        return DatabaseDescriptor.getDefaultKeyspaceRF();
    }

    public boolean getSkipPaxosRepairOnTopologyChange()
    {
        return true;//TODO //DatabaseDescriptor.skipPaxosRepairOnTopologyChange();
    }

    public void setSkipPaxosRepairOnTopologyChange(boolean v)
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(v);
        logger.info("paxos skip topology change repair {} via jmx", v ? "enabled" : "disabled");
    }

    public String getSkipPaxosRepairOnTopologyChangeKeyspaces()
    {
        return Joiner.on(',').join(DatabaseDescriptor.skipPaxosRepairOnTopologyChangeKeyspaces());
    }

    public void setSkipPaxosRepairOnTopologyChangeKeyspaces(String v)
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeKeyspaces(v);
        logger.info("paxos skip topology change repair keyspaces set to  {} via jmx", v);
    }

    public boolean getPaxosAutoRepairsEnabled()
    {
        return PaxosState.uncommittedTracker().isAutoRepairsEnabled();
    }

    public void setPaxosAutoRepairsEnabled(boolean enabled)
    {
        PaxosState.uncommittedTracker().setAutoRepairsEnabled(enabled);
        logger.info("paxos auto repairs {} via jmx", enabled ? "enabled" : "disabled");
    }

    public boolean getPaxosStateFlushEnabled()
    {
        return PaxosState.uncommittedTracker().isStateFlushEnabled();
    }

    public void setPaxosStateFlushEnabled(boolean enabled)
    {
        PaxosState.uncommittedTracker().setStateFlushEnabled(enabled);
        logger.info("paxos state flush {} via jmx", enabled ? "enabled" : "disabled");
    }

    public List<String> getPaxosAutoRepairTables()
    {
        Set<TableId> tableIds = PaxosState.uncommittedTracker().tableIds();
        List<String> tables = new ArrayList<>(tableIds.size());
        for (TableId tableId : tableIds)
        {
            TableMetadata table = Schema.instance.getTableMetadata(tableId);
            if (table == null)
                continue;
            tables.add(table.keyspace + '.' + table.name);
        }
        return tables;
    }

    public long getPaxosPurgeGraceSeconds()
    {
        return DatabaseDescriptor.getPaxosPurgeGrace(SECONDS);
    }

    public void setPaxosPurgeGraceSeconds(long v)
    {
        DatabaseDescriptor.setPaxosPurgeGrace(v);
        logger.info("paxos purging grace seconds set to {} via jmx", v);
    }

    public String getPaxosOnLinearizabilityViolations()
    {
        return DatabaseDescriptor.paxosOnLinearizabilityViolations().toString();
    }

    public void setPaxosOnLinearizabilityViolations(String v)
    {
        DatabaseDescriptor.setPaxosOnLinearizabilityViolations(Config.PaxosOnLinearizabilityViolation.valueOf(v));
        logger.info("paxos on linearizability violations {} via jmx", v);
    }

    public String getPaxosStatePurging()
    {
        return DatabaseDescriptor.paxosStatePurging().name();
    }

    public void setPaxosStatePurging(String v)
    {
        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.valueOf(v));
        logger.info("paxos state purging {} via jmx", v);
    }

    public boolean getPaxosRepairEnabled()
    {
        return DatabaseDescriptor.paxosRepairEnabled();
    }

    public void setPaxosRepairEnabled(boolean enabled)
    {
        DatabaseDescriptor.setPaxosRepairEnabled(enabled);
        logger.info("paxos repair {} via jmx", enabled ? "enabled" : "disabled");
    }

    public boolean getPaxosDcLocalCommitEnabled()
    {
        return PaxosCommit.getEnableDcLocalCommit();
    }

    public void setPaxosDcLocalCommitEnabled(boolean enabled)
    {
        PaxosCommit.setEnableDcLocalCommit(enabled);
        logger.info("paxos dc local commit {} via jmx", enabled ? "enabled" : "disabled");
    }

    public String getPaxosBallotLowBound(String ksName, String tblName, String key)
    {
        Keyspace keyspace = Keyspace.open(ksName);
        if (keyspace == null)
            throw new IllegalArgumentException("Unknown keyspace '" + ksName + "'");

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tblName);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown table '" + tblName + "' in keyspace '" + ksName + "'");

        TableMetadata table = cfs.metadata.get();
        DecoratedKey dk = table.partitioner.decorateKey(table.partitionKeyType.fromString(key));
        return cfs.getPaxosRepairHistory().ballotForToken(dk.getToken()).toString();
    }

    public Long getRepairRpcTimeout()
    {
        return DatabaseDescriptor.getRepairRpcTimeout(MILLISECONDS);
    }

    public void setRepairRpcTimeout(Long timeoutInMillis)
    {
        Preconditions.checkState(timeoutInMillis > 0);
        DatabaseDescriptor.setRepairRpcTimeout(timeoutInMillis);
        logger.info("RepairRpcTimeout set to {}ms via JMX", timeoutInMillis);
    }
    public void evictHungRepairs()
    {
        logger.info("StorageService#clearPaxosRateLimiters called via jmx");
        Paxos.evictHungRepairs();
    }

    public void clearPaxosRepairs()
    {
        logger.info("StorageService#clearPaxosRepairs called via jmx");
        PaxosTableRepairs.clearRepairs();
    }

    public void setSkipPaxosRepairCompatibilityCheck(boolean v)
    {
        PaxosRepair.setSkipPaxosRepairCompatibilityCheck(v);
        logger.info("SkipPaxosRepairCompatibilityCheck set to {} via jmx", v);
    }

    public boolean getSkipPaxosRepairCompatibilityCheck()
    {
        return PaxosRepair.getSkipPaxosRepairCompatibilityCheck();
    }

    @Override
    public boolean topPartitionsEnabled()
    {
        return DatabaseDescriptor.topPartitionsEnabled();
    }

    @Override
    public int getMaxTopSizePartitionCount()
    {
        return DatabaseDescriptor.getMaxTopSizePartitionCount();
    }

    @Override
    public void setMaxTopSizePartitionCount(int value)
    {
        DatabaseDescriptor.setMaxTopSizePartitionCount(value);
    }

    @Override
    public int getMaxTopTombstonePartitionCount()
    {
        return DatabaseDescriptor.getMaxTopTombstonePartitionCount();
    }

    @Override
    public void setMaxTopTombstonePartitionCount(int value)
    {
        DatabaseDescriptor.setMaxTopTombstonePartitionCount(value);
    }

    @Override
    public String getMinTrackedPartitionSize()
    {
        return DatabaseDescriptor.getMinTrackedPartitionSizeInBytes().toString();
    }

    @Override
    public void setMinTrackedPartitionSize(String value)
    {
        DatabaseDescriptor.setMinTrackedPartitionSizeInBytes(parseDataStorageSpec(value));
    }

    @Override
    public long getMinTrackedPartitionTombstoneCount()
    {
        return DatabaseDescriptor.getMinTrackedPartitionTombstoneCount();
    }

    @Override
    public void setMinTrackedPartitionTombstoneCount(long value)
    {
        DatabaseDescriptor.setMinTrackedPartitionTombstoneCount(value);
    }

    @Override
    public void setSkipStreamDiskSpaceCheck(boolean value)
    {
        if (value != DatabaseDescriptor.getSkipStreamDiskSpaceCheck())
            logger.info("Changing skip_stream_disk_space_check from {} to {}", DatabaseDescriptor.getSkipStreamDiskSpaceCheck(), value);
        DatabaseDescriptor.setSkipStreamDiskSpaceCheck(value);
    }

    @Override
    public boolean getSkipStreamDiskSpaceCheck()
    {
        return DatabaseDescriptor.getSkipStreamDiskSpaceCheck();
    }

    @Override
    public void addToCms(List<String> ignoredEndpoints)
    {
        ClusterMetadataService.instance().addToCms(ignoredEndpoints);
    }

    @Override
    public void removeFromCms(boolean force)
    {
        ClusterMetadataService.instance().removeFromCms(force);
    }

    @Override
    public Map<String, String> describeCMS()
    {
        Map<String, String> info = new HashMap<>();
        ClusterMetadata metadata = ClusterMetadata.current();
        ClusterMetadataService service = ClusterMetadataService.instance();
        String members = metadata.fullCMSMembers().stream().sorted().map(Object::toString).collect(Collectors.joining(","));
        info.put("MEMBERS", members);
        info.put("IS_MEMBER", Boolean.toString(service.isCurrentMember(FBUtilities.getBroadcastAddressAndPort())));
        info.put("SERVICE_STATE", ClusterMetadataService.state(metadata).toString());
        info.put("IS_MIGRATING", Boolean.toString(service.isMigrating()));
        info.put("EPOCH", Long.toString(metadata.epoch.getEpoch()));
        info.put("LOCAL_PENDING", Integer.toString(ClusterMetadataService.instance().log().pendingBufferSize()));
        info.put("COMMITS_PAUSED", Boolean.toString(service.commitsPaused()));
        return info;
    }

    @Override
    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException
    {
        if (!skipNotificationListeners)
            super.removeNotificationListener(listener);
    }

    @Override
    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException
    {
        if (!skipNotificationListeners)
            super.removeNotificationListener(listener, filter, handback);
    }

    @Override
    public void addNotificationListener(NotificationListener listener,
                                        NotificationFilter filter,
                                        Object handback) throws java.lang.IllegalArgumentException
    {
        if (!skipNotificationListeners)
            super.addNotificationListener(listener, filter, handback);
    }

    @Override
    public void sealPeriod()
    {
        logger.info("Sealing current period in metadata log");
        long period = ClusterMetadataService.instance().sealPeriod().period;
        logger.info("Current period {} is sealed", period);
    }

    @Override
    public void unsafeRevertClusterMetadata(long epoch)
    {
        if (!DatabaseDescriptor.getUnsafeTCMMode())
            throw new IllegalStateException("Cluster is not running unsafe TCM mode, can't revert epoch");
        ClusterMetadataService.instance().revertToEpoch(Epoch.create(epoch));
    }

    @Override
    public String dumpClusterMetadata(long epoch, long transformToEpoch, String version) throws IOException
    {
        return ClusterMetadataService.instance().dumpClusterMetadata(Epoch.create(epoch), Epoch.create(transformToEpoch), org.apache.cassandra.tcm.serialization.Version.valueOf(version));
    }

    @Override
    public String dumpClusterMetadata() throws IOException
    {
        return dumpClusterMetadata(Epoch.EMPTY.getEpoch(), ClusterMetadata.current().epoch.getEpoch() + 1000, org.apache.cassandra.tcm.serialization.Version.V0.toString());
    }

    @Override
    public void unsafeLoadClusterMetadata(String file) throws IOException
    {
        if (!DatabaseDescriptor.getUnsafeTCMMode())
            throw new IllegalStateException("Cluster is not running unsafe TCM mode, can't load cluster metadata " + file);
        ClusterMetadataService.instance().loadClusterMetadata(file);
    }

    @Override
    public void replayAndWait()
    {
        ClusterMetadataService.instance().fetchLogFromCMS();
    }

    @Override
    public void setCommitsPaused(boolean paused)
    {
        if (paused)
            ClusterMetadataService.instance().pauseCommits();
        else
            ClusterMetadataService.instance().resumeCommits();
    }

    @Override
    public boolean getCommitsPaused()
    {
        return ClusterMetadataService.instance().commitsPaused();
    }
}
