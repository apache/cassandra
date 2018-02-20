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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.management.*;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.jmx.JMXConfiguratorMBean;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.hook.DelayingShutdownHook;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthSchemaChangeListener;
import org.apache.cassandra.batchlog.BatchRemoveVerbHandler;
import org.apache.cassandra.batchlog.BatchStoreVerbHandler;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.hints.HintVerbHandler;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.*;
import org.apache.cassandra.repair.*;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaPullVerbHandler;
import org.apache.cassandra.schema.SchemaPushVerbHandler;
import org.apache.cassandra.schema.SchemaVersionVerbHandler;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXProgressSupport;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.index.SecondaryIndexManager.getIndexName;
import static org.apache.cassandra.index.SecondaryIndexManager.isIndexColumnFamily;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService extends NotificationBroadcasterSupport implements IEndpointStateChangeSubscriber, StorageServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    public static final int RING_DELAY = getRingDelay(); // delay after which we assume ring has stablized

    private final JMXProgressSupport progressSupport = new JMXProgressSupport(this);

    private static int getRingDelay()
    {
        String newdelay = System.getProperty("cassandra.ring_delay_ms");
        if (newdelay != null)
        {
            logger.info("Overriding RING_DELAY to {}ms", newdelay);
            return Integer.parseInt(newdelay);
        }
        else
            return 30 * 1000;
    }

    /* This abstraction maintains the token/endpoint metadata information */
    private TokenMetadata tokenMetadata = new TokenMetadata();

    public volatile VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(tokenMetadata.partitioner);

    private Thread drainOnShutdown = null;
    private volatile boolean isShutdown = false;
    private final List<Runnable> preShutdownHooks = new ArrayList<>();
    private final List<Runnable> postShutdownHooks = new ArrayList<>();

    public static final StorageService instance = new StorageService();

    @Deprecated
    public boolean isInShutdownHook()
    {
        return isShutdown();
    }

    public boolean isShutdown()
    {
        return isShutdown;
    }

    public Collection<Range<Token>> getLocalRanges(String keyspaceName)
    {
        return getRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddressAndPort());
    }

    public List<Range<Token>> getLocalAndPendingRanges(String ks)
    {
        InetAddressAndPort broadcastAddress = FBUtilities.getBroadcastAddressAndPort();
        Keyspace keyspace = Keyspace.open(ks);
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.addAll(keyspace.getReplicationStrategy().getAddressRanges().get(broadcastAddress));
        ranges.addAll(getTokenMetadata().getPendingRanges(ks, broadcastAddress));
        return Range.normalize(ranges);
    }

    public Collection<Range<Token>> getPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, FBUtilities.getBroadcastAddressAndPort());
    }

    public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace)
    {
        return getPrimaryRangeForEndpointWithinDC(keyspace, FBUtilities.getBroadcastAddressAndPort());
    }

    private final Set<InetAddressAndPort> replicatingNodes = Sets.newConcurrentHashSet();
    private CassandraDaemon daemon;

    private InetAddressAndPort removingNode;

    /* Are we starting this node in bootstrap mode? */
    private volatile boolean isBootstrapMode;

    /* we bootstrap but do NOT join the ring unless told to do so */
    private boolean isSurveyMode = Boolean.parseBoolean(System.getProperty("cassandra.write_survey", "false"));
    /* true if node is rebuilding and receiving data */
    private final AtomicBoolean isRebuilding = new AtomicBoolean();
    private final AtomicBoolean isDecommissioning = new AtomicBoolean();

    private volatile boolean initialized = false;
    private volatile boolean joined = false;
    private volatile boolean gossipActive = false;
    private final AtomicBoolean authSetupCalled = new AtomicBoolean(false);
    private volatile boolean authSetupComplete = false;

    /* the probability for tracing any particular request, 0 disables tracing and 1 enables for all */
    private double traceProbability = 0.0;

    private static enum Mode { STARTING, NORMAL, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED }
    private volatile Mode operationMode = Mode.STARTING;

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();

    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    private final ObjectName jmxObjectName;

    private Collection<Token> bootstrapTokens = null;

    // true when keeping strict consistency while bootstrapping
    private static final boolean useStrictConsistency = Boolean.parseBoolean(System.getProperty("cassandra.consistent.rangemovement", "true"));
    private static final boolean allowSimultaneousMoves = Boolean.parseBoolean(System.getProperty("cassandra.consistent.simultaneousmoves.allow","false"));
    private static final boolean joinRing = Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true"));
    private boolean replacing;

    private final StreamStateStore streamStateStore = new StreamStateStore();

    /** This method updates the local token on disk  */
    public void setTokens(Collection<Token> tokens)
    {
        assert tokens != null && !tokens.isEmpty() : "Node needs at least one token.";
        if (logger.isDebugEnabled())
            logger.debug("Setting tokens to {}", tokens);
        SystemKeyspace.updateTokens(tokens);
        Collection<Token> localTokens = getLocalTokens();
        setGossipTokens(localTokens);
        tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddressAndPort());
        setMode(Mode.NORMAL, false);
    }

    public void setGossipTokens(Collection<Token> tokens)
    {
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<Pair<ApplicationState, VersionedValue>>();
        states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
        states.add(Pair.create(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(tokens)));
        states.add(Pair.create(ApplicationState.STATUS, valueFactory.normal(tokens)));
        Gossiper.instance.addLocalApplicationStates(states);
    }

    public StorageService()
    {
        // use dedicated executor for sending JMX notifications
        super(Executors.newSingleThreadExecutor());

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            jmxObjectName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            mbs.registerMBean(this, jmxObjectName);
            mbs.registerMBean(StreamManager.instance, new ObjectName(StreamManager.OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        ReadCommandVerbHandler readHandler = new ReadCommandVerbHandler();

        /* register the verb handlers */
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.MUTATION, new MutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.READ_REPAIR, new ReadRepairVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.READ, readHandler);
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.RANGE_SLICE, readHandler);
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.PAGED_RANGE, readHandler);
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.COUNTER_MUTATION, new CounterMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.TRUNCATE, new TruncateVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.PAXOS_PREPARE, new PrepareVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.PAXOS_PROPOSE, new ProposeVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.PAXOS_COMMIT, new CommitVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.HINT, new HintVerbHandler());

        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.REPLICATION_FINISHED, new ReplicationFinishedVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.REQUEST_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.INTERNAL_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.REPAIR_MESSAGE, new RepairMessageVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_SHUTDOWN, new GossipShutdownVerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.DEFINITIONS_UPDATE, new SchemaPushVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.SCHEMA_CHECK, new SchemaVersionVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.MIGRATION_REQUEST, new SchemaPullVerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.SNAPSHOT, new SnapshotVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.ECHO, new EchoVerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.BATCH_STORE, new BatchStoreVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.BATCH_REMOVE, new BatchRemoveVerbHandler());
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
        if (gossipActive)
        {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            gossipActive = false;
        }
    }

    // should only be called via JMX
    public synchronized void startGossiping()
    {
        if (!gossipActive)
        {
            checkServiceAllowedToStart("gossip");

            logger.warn("Starting gossip by operator request");
            Collection<Token> tokens = SystemKeyspace.getSavedTokens();

            boolean validTokens = tokens != null && !tokens.isEmpty();

            // shouldn't be called before these are set if we intend to join the ring/are in the process of doing so
            if (joined || joinRing)
                assert validTokens : "Cannot start gossiping for a node intended to join without valid tokens";

            if (validTokens)
                setGossipTokens(tokens);

            Gossiper.instance.forceNewerGeneration();
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
            gossipActive = true;
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

    public void stopTransports()
    {
        if (isGossipActive())
        {
            logger.error("Stopping gossiper");
            stopGossiping();
        }
        if (isNativeTransportRunning())
        {
            logger.error("Stopping native transport");
            stopNativeTransport();
        }
    }

    /**
     * Set the Gossip flag RPC_READY to false and then
     * shutdown the client services (thrift and CQL).
     *
     * Note that other nodes will do this for us when
     * they get the Gossip shutdown message, so even if
     * we don't get time to broadcast this, it is not a problem.
     *
     * See {@link Gossiper#markAsShutdown(InetAddressAndPort)}
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
        StageManager.shutdownNow();
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    public boolean isGossipActive()
    {
        return gossipActive;
    }

    public boolean isDaemonSetupCompleted()
    {
        return daemon == null
               ? false
               : daemon.setupCompleted();
    }

    public void stopDaemon()
    {
        if (daemon == null)
            throw new IllegalStateException("No configured daemon");
        daemon.deactivate();
    }

    private synchronized UUID prepareForReplacement() throws ConfigurationException
    {
        if (SystemKeyspace.bootstrapComplete())
            throw new RuntimeException("Cannot replace address with a node that is already bootstrapped");

        if (!joinRing)
            throw new ConfigurationException("Cannot set both join_ring=false and attempt to replace a node");

        if (!DatabaseDescriptor.isAutoBootstrap() && !Boolean.getBoolean("cassandra.allow_unsafe_replace"))
            throw new RuntimeException("Replacing a node without bootstrapping risks invalidating consistency " +
                                       "guarantees as the expected data may not be present until repair is run. " +
                                       "To perform this operation, please restart with " +
                                       "-Dcassandra.allow_unsafe_replace=true");

        InetAddressAndPort replaceAddress = DatabaseDescriptor.getReplaceAddress();
        logger.info("Gathering node replacement information for {}", replaceAddress);
        Map<InetAddressAndPort, EndpointState> epStates = Gossiper.instance.doShadowRound();
        // as we've completed the shadow round of gossip, we should be able to find the node we're replacing
        if (epStates.get(replaceAddress) == null)
            throw new RuntimeException(String.format("Cannot replace_address %s because it doesn't exist in gossip", replaceAddress));

        try
        {
            VersionedValue tokensVersionedValue = epStates.get(replaceAddress).getApplicationState(ApplicationState.TOKENS);
            if (tokensVersionedValue == null)
                throw new RuntimeException(String.format("Could not find tokens for %s to replace", replaceAddress));

            bootstrapTokens = TokenSerializer.deserialize(tokenMetadata.partitioner, new DataInputStream(new ByteArrayInputStream(tokensVersionedValue.toBytes())));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        UUID localHostId = SystemKeyspace.getLocalHostId();

        if (isReplacingSameAddress())
        {
            localHostId = Gossiper.instance.getHostId(replaceAddress, epStates);
            SystemKeyspace.setLocalHostId(localHostId); // use the replacee's host Id as our own so we receive hints, etc
        }

        return localHostId;
    }

    private synchronized void checkForEndpointCollision(UUID localHostId) throws ConfigurationException
    {
        if (Boolean.getBoolean("cassandra.allow_unsafe_join"))
        {
            logger.warn("Skipping endpoint collision check as cassandra.allow_unsafe_join=true");
            return;
        }

        logger.debug("Starting shadow gossip round to check for endpoint collision");
        Map<InetAddressAndPort, EndpointState> epStates = Gossiper.instance.doShadowRound();
        // If bootstrapping, check whether any previously known status for the endpoint makes it unsafe to do so.
        // If not bootstrapping, compare the host id for this endpoint learned from gossip (if any) with the local
        // one, which was either read from system.local or generated at startup. If a learned id is present &
        // doesn't match the local, then the node needs replacing
        if (!Gossiper.instance.isSafeForStartup(FBUtilities.getBroadcastAddressAndPort(), localHostId, shouldBootstrap(), epStates))
        {
            throw new RuntimeException(String.format("A node with address %s already exists, cancelling join. " +
                                                     "Use cassandra.replace_address if you want to replace this node.",
                                                     FBUtilities.getBroadcastAddressAndPort()));
        }

        if (shouldBootstrap() && useStrictConsistency && !allowSimultaneousMoves())
        {
            for (Map.Entry<InetAddressAndPort, EndpointState> entry : epStates.entrySet())
            {
                // ignore local node or empty status
                if (entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()) || (entry.getValue().getApplicationState(ApplicationState.STATUS_WITH_PORT) == null & entry.getValue().getApplicationState(ApplicationState.STATUS) == null))
                    continue;

                VersionedValue value = entry.getValue().getApplicationState(ApplicationState.STATUS_WITH_PORT);
                if (value == null)
                {
                    value = entry.getValue().getApplicationState(ApplicationState.STATUS);
                }

                String[] pieces = splitValue(value);
                assert (pieces.length > 0);
                String state = pieces[0];
                if (state.equals(VersionedValue.STATUS_BOOTSTRAPPING) || state.equals(VersionedValue.STATUS_LEAVING) || state.equals(VersionedValue.STATUS_MOVING))
                    throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");
            }
        }
    }

    private boolean allowSimultaneousMoves()
    {
        return allowSimultaneousMoves && DatabaseDescriptor.getNumTokens() == 1;
    }

    // for testing only
    public void unsafeInitialize() throws ConfigurationException
    {
        initialized = true;
        gossipActive = true;
        Gossiper.instance.register(this);
        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        if (!MessagingService.instance().isListening())
            MessagingService.instance().listen();
    }

    public void populateTokenMetadata()
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true")))
        {
            logger.info("Populating token metadata from system tables");
            Multimap<InetAddressAndPort, Token> loadedTokens = SystemKeyspace.loadTokens();
            if (!shouldBootstrap()) // if we have not completed bootstrapping, we should not add ourselves as a normal token
                loadedTokens.putAll(FBUtilities.getBroadcastAddressAndPort(), SystemKeyspace.getSavedTokens());
            for (InetAddressAndPort ep : loadedTokens.keySet())
                tokenMetadata.updateNormalTokens(loadedTokens.get(ep), ep);

            logger.info("Token metadata: {}", tokenMetadata);
        }
    }

    public synchronized void initServer() throws ConfigurationException
    {
        initServer(RING_DELAY);
    }

    public synchronized void initServer(int delay) throws ConfigurationException
    {
        logger.info("Cassandra version: {}", FBUtilities.getReleaseVersionString());
        logger.info("CQL supported versions: {} (default: {})",
                StringUtils.join(ClientState.getCQLSupportedVersion(), ", "), ClientState.DEFAULT_CQL_VERSION);
        logger.info("Native protocol supported versions: {} (default: {})",
                    StringUtils.join(ProtocolVersion.supportedVersions(), ", "), ProtocolVersion.CURRENT);

        try
        {
            // Ensure StorageProxy is initialized on start-up; see CASSANDRA-3797.
            Class.forName("org.apache.cassandra.service.StorageProxy");
            // also IndexSummaryManager, which is otherwise unreferenced
            Class.forName("org.apache.cassandra.io.sstable.IndexSummaryManager");
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

                if (FBUtilities.isWindows)
                    WindowsTimer.endTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());

                // Cleanup logback
                DelayingShutdownHook logbackHook = new DelayingShutdownHook();
                logbackHook.setContext((LoggerContext)LoggerFactory.getILoggerFactory());
                logbackHook.run();
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);

        replacing = isReplacing();

        if (!Boolean.parseBoolean(System.getProperty("cassandra.start_gossip", "true")))
        {
            logger.info("Not starting gossip as requested.");
            // load ring state in preparation for starting gossip later
            loadRingState();
            initialized = true;
            return;
        }

        prepareToJoin();

        // Has to be called after the host id has potentially changed in prepareToJoin().
        try
        {
            CacheService.instance.counterCache.loadSavedAsync().get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Error loading counter cache", t);
        }

        if (joinRing)
        {
            joinTokenRing(delay);
        }
        else
        {
            Collection<Token> tokens = SystemKeyspace.getSavedTokens();
            if (!tokens.isEmpty())
            {
                tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddressAndPort());
                // order is important here, the gossiper can fire in between adding these two states.  It's ok to send TOKENS without STATUS, but *not* vice versa.
                List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<Pair<ApplicationState, VersionedValue>>();
                states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
                states.add(Pair.create(ApplicationState.STATUS_WITH_PORT, valueFactory.hibernate(true)));
                states.add(Pair.create(ApplicationState.STATUS, valueFactory.hibernate(true)));
                Gossiper.instance.addLocalApplicationStates(states);
            }
            doAuthSetup();
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }

        initialized = true;
    }

    private void loadRingState()
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true")))
        {
            logger.info("Loading persisted ring state");
            Multimap<InetAddressAndPort, Token> loadedTokens = SystemKeyspace.loadTokens();
            Map<InetAddressAndPort, UUID> loadedHostIds = SystemKeyspace.loadHostIds();
            for (InetAddressAndPort ep : loadedTokens.keySet())
            {
                if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    // entry has been mistakenly added, delete it
                    SystemKeyspace.removeEndpoint(ep);
                }
                else
                {
                    if (loadedHostIds.containsKey(ep))
                        tokenMetadata.updateHostId(loadedHostIds.get(ep), ep);
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }
    }

    private boolean isReplacing()
    {
        if (System.getProperty("cassandra.replace_address_first_boot", null) != null && SystemKeyspace.bootstrapComplete())
        {
            logger.info("Replace address on first boot requested; this node is already bootstrapped");
            return false;
        }
        return DatabaseDescriptor.getReplaceAddress() != null;
    }

    /**
     * In the event of forceful termination we need to remove the shutdown hook to prevent hanging (OOM for instance)
     */
    public void removeShutdownHook()
    {
        if (drainOnShutdown != null)
            Runtime.getRuntime().removeShutdownHook(drainOnShutdown);

        if (FBUtilities.isWindows)
            WindowsTimer.endTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
    }

    private boolean shouldBootstrap()
    {
        return DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && !isSeed();
    }

    public static boolean isSeed()
    {
        return DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort());
    }

    private void prepareToJoin() throws ConfigurationException
    {
        if (!joined)
        {
            Map<ApplicationState, VersionedValue> appStates = new EnumMap<>(ApplicationState.class);

            if (SystemKeyspace.wasDecommissioned())
            {
                if (Boolean.getBoolean("cassandra.override_decommission"))
                {
                    logger.warn("This node was decommissioned, but overriding by operator request.");
                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                }
                else
                    throw new ConfigurationException("This node was decommissioned and will not rejoin the ring unless cassandra.override_decommission=true has been set, or all existing data is removed and the node is bootstrapped again");
            }

            if (DatabaseDescriptor.getReplaceTokens().size() > 0 || DatabaseDescriptor.getReplaceNode() != null)
                throw new RuntimeException("Replace method removed; use cassandra.replace_address instead");

            if (!MessagingService.instance().isListening())
                MessagingService.instance().listen();

            UUID localHostId = SystemKeyspace.getLocalHostId();

            if (replacing)
            {
                localHostId = prepareForReplacement();
                appStates.put(ApplicationState.TOKENS, valueFactory.tokens(bootstrapTokens));

                if (!DatabaseDescriptor.isAutoBootstrap())
                {
                    // Will not do replace procedure, persist the tokens we're taking over locally
                    // so that they don't get clobbered with auto generated ones in joinTokenRing
                    SystemKeyspace.updateTokens(bootstrapTokens);
                }
                else if (isReplacingSameAddress())
                {
                    //only go into hibernate state if replacing the same address (CASSANDRA-8523)
                    logger.warn("Writes will not be forwarded to this node during replacement because it has the same address as " +
                                "the node to be replaced ({}). If the previous node has been down for longer than max_hint_window_in_ms, " +
                                "repair must be run after the replacement process in order to make this node consistent.",
                                DatabaseDescriptor.getReplaceAddress());
                    appStates.put(ApplicationState.STATUS_WITH_PORT, valueFactory.hibernate(true));
                    appStates.put(ApplicationState.STATUS, valueFactory.hibernate(true));
                }
            }
            else
            {
                checkForEndpointCollision(localHostId);
            }

            // have to start the gossip service before we can see any info on other nodes.  this is necessary
            // for bootstrap to get the load info it needs.
            // (we won't be part of the storage ring though until we add a counterId to our state, below.)
            // Seed the host ID-to-endpoint map with our own ID.
            getTokenMetadata().updateHostId(localHostId, FBUtilities.getBroadcastAddressAndPort());
            appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId));
            appStates.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueFactory.nativeaddressAndPort(FBUtilities.getBroadcastNativeAddressAndPort()));
            appStates.put(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(FBUtilities.getJustBroadcastNativeAddress()));
            appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

            // load the persisted ring state. This used to be done earlier in the init process,
            // but now we always perform a shadow round when preparing to join and we have to
            // clear endpoint states after doing that.
            loadRingState();

            logger.info("Starting up server gossip");
            Gossiper.instance.register(this);
            Gossiper.instance.start(SystemKeyspace.incrementAndGetGeneration(), appStates); // needed for node-ring gathering.
            gossipActive = true;
            // gossip snitch infos (local DC and rack)
            gossipSnitchInfo();
            // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)
            Schema.instance.updateVersionAndAnnounce(); // Ensure we know our own actual Schema UUID in preparation for updates
            LoadBroadcaster.instance.startBroadcasting();
            HintsService.instance.startDispatch();
            BatchlogManager.instance.start();
        }
    }

    public void waitForSchema(int delay)
    {
        // first sleep the delay to make sure we see all our peers
        for (int i = 0; i < delay; i += 1000)
        {
            // if we see schema, we can proceed to the next check directly
            if (!Schema.instance.isEmpty())
            {
                logger.debug("current schema version: {}", Schema.instance.getVersion());
                break;
            }
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        // if our schema hasn't matched yet, wait until it has
        // we do this by waiting for all in-flight migration requests and responses to complete
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        if (!MigrationManager.isReadyForBootstrap())
        {
            setMode(Mode.JOINING, "waiting for schema information to complete", true);
            MigrationManager.waitUntilReadyForBootstrap();
        }
    }

    private void joinTokenRing(int delay) throws ConfigurationException
    {
        joined = true;

        // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
        // If we are a seed, or if the user manually sets auto_bootstrap to false,
        // we'll skip streaming data from other nodes and jump directly into the ring.
        //
        // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
        // which is useful for both new users and testing.
        //
        // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
        // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
        Set<InetAddressAndPort> current = new HashSet<>();
        if (logger.isDebugEnabled())
        {
            logger.debug("Bootstrap variables: {} {} {} {}",
                         DatabaseDescriptor.isAutoBootstrap(),
                         SystemKeyspace.bootstrapInProgress(),
                         SystemKeyspace.bootstrapComplete(),
                         DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort()));
        }
        if (DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort()))
        {
            logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
        }

        boolean dataAvailable = true; // make this to false when bootstrap streaming failed
        boolean bootstrap = shouldBootstrap();
        if (bootstrap)
        {
            if (SystemKeyspace.bootstrapInProgress())
                logger.warn("Detected previous bootstrap failure; retrying");
            else
                SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.IN_PROGRESS);
            setMode(Mode.JOINING, "waiting for ring information", true);
            waitForSchema(delay);
            setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);
            setMode(Mode.JOINING, "waiting for pending range calculation", true);
            PendingRangeCalculatorService.instance.blockUntilFinished();
            setMode(Mode.JOINING, "calculation complete, ready to bootstrap", true);

            logger.debug("... got ring + schema info");

            if (useStrictConsistency && !allowSimultaneousMoves() &&
                    (
                        tokenMetadata.getBootstrapTokens().valueSet().size() > 0 ||
                        tokenMetadata.getSizeOfLeavingEndpoints() > 0 ||
                        tokenMetadata.getSizeOfMovingEndpoints() > 0
                    ))
            {
                String bootstrapTokens = StringUtils.join(tokenMetadata.getBootstrapTokens().valueSet(), ',');
                String leavingTokens = StringUtils.join(tokenMetadata.getLeavingEndpoints(), ',');
                String movingTokens = StringUtils.join(tokenMetadata.getMovingEndpoints().stream().map(e -> e.right).toArray(), ',');
                throw new UnsupportedOperationException(String.format("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true. Nodes detected, bootstrapping: %s; leaving: %s; moving: %s;", bootstrapTokens, leavingTokens, movingTokens));
            }

            // get bootstrap tokens
            if (!replacing)
            {
                if (tokenMetadata.isMember(FBUtilities.getBroadcastAddressAndPort()))
                {
                    String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                setMode(Mode.JOINING, "getting bootstrap token", true);
                bootstrapTokens = BootStrapper.getBootstrapTokens(tokenMetadata, FBUtilities.getBroadcastAddressAndPort(), delay);
            }
            else
            {
                if (!isReplacingSameAddress())
                {
                    try
                    {
                        // Sleep additionally to make sure that the server actually is not alive
                        // and giving it more time to gossip if alive.
                        Thread.sleep(LoadBroadcaster.BROADCAST_INTERVAL);
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }

                    // check for operator errors...
                    for (Token token : bootstrapTokens)
                    {
                        InetAddressAndPort existing = tokenMetadata.getEndpoint(token);
                        if (existing != null)
                        {
                            long nanoDelay = delay * 1000000L;
                            if (Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp() > (System.nanoTime() - nanoDelay))
                                throw new UnsupportedOperationException("Cannot replace a live node... ");
                            current.add(existing);
                        }
                        else
                        {
                            throw new UnsupportedOperationException("Cannot replace token " + token + " which does not exist!");
                        }
                    }
                }
                else
                {
                    try
                    {
                        Thread.sleep(RING_DELAY);
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }

                }
                setMode(Mode.JOINING, "Replacing a node with token(s): " + bootstrapTokens, true);
            }

            dataAvailable = bootstrap(bootstrapTokens);
        }
        else
        {
            bootstrapTokens = SystemKeyspace.getSavedTokens();
            if (bootstrapTokens.isEmpty())
            {
                bootstrapTokens = BootStrapper.getBootstrapTokens(tokenMetadata, FBUtilities.getBroadcastAddressAndPort(), delay);
            }
            else
            {
                if (bootstrapTokens.size() != DatabaseDescriptor.getNumTokens())
                    throw new ConfigurationException("Cannot change the number of tokens from " + bootstrapTokens.size() + " to " + DatabaseDescriptor.getNumTokens());
                else
                    logger.info("Using saved tokens {}", bootstrapTokens);
            }
        }

        // if we don't have system_traces keyspace at this point, then create it manually
        maybeAddOrUpdateKeyspace(TraceKeyspace.metadata());
        maybeAddOrUpdateKeyspace(SystemDistributedKeyspace.metadata());

        if (!isSurveyMode)
        {
            if (dataAvailable)
            {
                finishJoiningRing(bootstrap, bootstrapTokens);
                // remove the existing info about the replaced node.
                if (!current.isEmpty())
                {
                    for (InetAddressAndPort existing : current)
                        Gossiper.instance.replacedEndpoint(existing);
                }
            }
            else
            {
                logger.warn("Some data streaming failed. Use nodetool to check bootstrap state and resume. For more, see `nodetool help bootstrap`. {}", SystemKeyspace.getBootstrapState());
            }
        }
        else
        {
            logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
        }
    }

    public static boolean isReplacingSameAddress()
    {
        InetAddressAndPort replaceAddress = DatabaseDescriptor.getReplaceAddress();
        return replaceAddress != null && replaceAddress.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    public void gossipSnitchInfo()
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        String dc = snitch.getDatacenter(FBUtilities.getBroadcastAddressAndPort());
        String rack = snitch.getRack(FBUtilities.getBroadcastAddressAndPort());
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC, StorageService.instance.valueFactory.datacenter(dc));
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, StorageService.instance.valueFactory.rack(rack));
    }

    public void joinRing() throws IOException
    {
        SystemKeyspace.BootstrapState state = SystemKeyspace.getBootstrapState();
        joinRing(state.equals(SystemKeyspace.BootstrapState.IN_PROGRESS));
    }

    private synchronized void joinRing(boolean resumedBootstrap) throws IOException
    {
        if (!joined)
        {
            logger.info("Joining ring by operator request");
            try
            {
                joinTokenRing(0);
            }
            catch (ConfigurationException e)
            {
                throw new IOException(e.getMessage());
            }
        }
        else if (isSurveyMode)
        {
            logger.info("Leaving write survey mode and joining ring at operator request");
            finishJoiningRing(resumedBootstrap, SystemKeyspace.getSavedTokens());
            isSurveyMode = false;
        }
    }

    private void executePreJoinTasks(boolean bootstrap)
    {
        StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                .filter(cfs -> Schema.instance.getUserKeyspaces().contains(cfs.keyspace.getName()))
                .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(bootstrap));
    }

    private void finishJoiningRing(boolean didBootstrap, Collection<Token> tokens)
    {
        // start participating in the ring.
        setMode(Mode.JOINING, "Finish joining ring", true);
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
        executePreJoinTasks(didBootstrap);
        setTokens(tokens);

        assert tokenMetadata.sortedTokens().size() > 0;
        doAuthSetup();
    }

    private void doAuthSetup()
    {
        if (!authSetupCalled.getAndSet(true))
        {
            maybeAddOrUpdateKeyspace(AuthKeyspace.metadata());

            DatabaseDescriptor.getRoleManager().setup();
            DatabaseDescriptor.getAuthenticator().setup();
            DatabaseDescriptor.getAuthorizer().setup();
            Schema.instance.registerListener(new AuthSchemaChangeListener());
            authSetupComplete = true;
        }
    }

    public boolean isAuthSetupComplete()
    {
        return authSetupComplete;
    }

    private void maybeAddKeyspace(KeyspaceMetadata ksm)
    {
        try
        {
            /*
             * We use timestamp of 0, intentionally, so that varying timestamps wouldn't cause schema mismatches on
             * newly added nodes.
             *
             * Having the initial/default timestamp as 0 also allows users to make and persist changes to replication
             * of our replicated system keyspaces.
             *
             * In case that we need to make incompatible changes to those kesypaces/tables, we'd need to bump the timestamp
             * on per-keyspace/per-table basis. So far we've never needed to.
             */
            MigrationManager.announceNewKeyspace(ksm, 0, false);
        }
        catch (AlreadyExistsException e)
        {
            logger.debug("Attempted to create new keyspace {}, but it already exists", ksm.name);
        }
    }

    /**
     * Ensure the schema of a pseudo-system keyspace (a distributed system keyspace: traces, auth and the so-called distributedKeyspace),
     * is up to date with what we expected (creating it if it doesn't exist and updating tables that may have been upgraded).
     */
    private void maybeAddOrUpdateKeyspace(KeyspaceMetadata expected)
    {
        // Note that want to deal with the keyspace and its table a bit differently: for the keyspace definition
        // itself, we want to create it if it doesn't exist yet, but if it does exist, we don't want to modify it,
        // because user can modify the definition to change the replication factor (#6016) and we don't want to
        // override it. For the tables however, we have to deal with the fact that new version can add new columns
        // (#8162 being an example), so even if the table definition exists, we still need to force the "current"
        // version of the schema, the one the node will be expecting.

        KeyspaceMetadata defined = Schema.instance.getKeyspaceMetadata(expected.name);
        // If the keyspace doesn't exist, create it
        if (defined == null)
        {
            maybeAddKeyspace(expected);
            defined = Schema.instance.getKeyspaceMetadata(expected.name);
        }

        // While the keyspace exists, it might miss table or have outdated one
        // There is also the potential for a race, as schema migrations add the bare
        // keyspace into Schema.instance before adding its tables, so double check that
        // all the expected tables are present
        for (TableMetadata expectedTable : expected.tables)
        {
            TableMetadata definedTable = defined.tables.get(expectedTable.name).orElse(null);
            if (definedTable == null || !definedTable.equals(expectedTable))
                MigrationManager.forceAnnounceNewTable(expectedTable);
        }
    }

    public boolean isJoined()
    {
        return tokenMetadata.isMember(FBUtilities.getBroadcastAddressAndPort()) && !isSurveyMode;
    }

    public void rebuild(String sourceDc)
    {
        rebuild(sourceDc, null, null, null);
    }

    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources)
    {
        // check ongoing rebuild
        if (!isRebuilding.compareAndSet(false, true))
        {
            throw new IllegalStateException("Node is still rebuilding. Check nodetool netstats.");
        }

        // check the arguments
        if (keyspace == null && tokens != null)
        {
            throw new IllegalArgumentException("Cannot specify tokens without keyspace.");
        }

        logger.info("rebuild from dc: {}, {}, {}", sourceDc == null ? "(any dc)" : sourceDc,
                    keyspace == null ? "(All keyspaces)" : keyspace,
                    tokens == null ? "(All tokens)" : tokens);

        try
        {
            RangeStreamer streamer = new RangeStreamer(tokenMetadata,
                                                       null,
                                                       FBUtilities.getBroadcastAddressAndPort(),
                                                       StreamOperation.REBUILD,
                                                       useStrictConsistency && !replacing,
                                                       DatabaseDescriptor.getEndpointSnitch(),
                                                       streamStateStore,
                                                       false,
                                                       DatabaseDescriptor.getStreamingConnectionsPerHost());
            streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));
            if (sourceDc != null)
                streamer.addSourceFilter(new RangeStreamer.SingleDatacenterFilter(DatabaseDescriptor.getEndpointSnitch(), sourceDc));

            if (keyspace == null)
            {
                for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
                    streamer.addRanges(keyspaceName, getLocalRanges(keyspaceName));
            }
            else if (tokens == null)
            {
                streamer.addRanges(keyspace, getLocalRanges(keyspace));
            }
            else
            {
                Token.TokenFactory factory = getTokenFactory();
                List<Range<Token>> ranges = new ArrayList<>();
                Pattern rangePattern = Pattern.compile("\\(\\s*(-?\\w+)\\s*,\\s*(-?\\w+)\\s*\\]");
                try (Scanner tokenScanner = new Scanner(tokens))
                {
                    while (tokenScanner.findInLine(rangePattern) != null)
                    {
                        MatchResult range = tokenScanner.match();
                        Token startToken = factory.fromString(range.group(1));
                        Token endToken = factory.fromString(range.group(2));
                        logger.info("adding range: ({},{}]", startToken, endToken);
                        ranges.add(new Range<>(startToken, endToken));
                    }
                    if (tokenScanner.hasNext())
                        throw new IllegalArgumentException("Unexpected string: " + tokenScanner.next());
                }

                // Ensure all specified ranges are actually ranges owned by this host
                Collection<Range<Token>> localRanges = getLocalRanges(keyspace);
                for (Range<Token> specifiedRange : ranges)
                {
                    boolean foundParentRange = false;
                    for (Range<Token> localRange : localRanges)
                    {
                        if (localRange.contains(specifiedRange))
                        {
                            foundParentRange = true;
                            break;
                        }
                    }
                    if (!foundParentRange)
                    {
                        throw new IllegalArgumentException(String.format("The specified range %s is not a range that is owned by this node. Please ensure that all token ranges specified to be rebuilt belong to this node.", specifiedRange.toString()));
                    }
                }

                if (specificSources != null)
                {
                    String[] stringHosts = specificSources.split(",");
                    Set<InetAddressAndPort> sources = new HashSet<>(stringHosts.length);
                    for (String stringHost : stringHosts)
                    {
                        try
                        {
                            InetAddressAndPort endpoint = InetAddressAndPort.getByName(stringHost);
                            if (FBUtilities.getBroadcastAddressAndPort().equals(endpoint))
                            {
                                throw new IllegalArgumentException("This host was specified as a source for rebuilding. Sources for a rebuild can only be other nodes in the cluster.");
                            }
                            sources.add(endpoint);
                        }
                        catch (UnknownHostException ex)
                        {
                            throw new IllegalArgumentException("Unknown host specified " + stringHost, ex);
                        }
                    }
                    streamer.addSourceFilter(new RangeStreamer.WhitelistedSourcesFilter(sources));
                }

                streamer.addRanges(keyspace, ranges);
            }

            StreamResultFuture resultFuture = streamer.fetchAsync();
            // wait for result
            resultFuture.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting on rebuild streaming");
        }
        catch (ExecutionException e)
        {
            // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
            logger.error("Error while rebuilding node", e.getCause());
            throw new RuntimeException("Error while rebuilding node: " + e.getCause().getMessage());
        }
        finally
        {
            // rebuild is done (successfully or not)
            isRebuilding.set(false);
        }
    }

    public void setRpcTimeout(long value)
    {
        DatabaseDescriptor.setRpcTimeout(value);
        logger.info("set rpc timeout to {} ms", value);
    }

    public long getRpcTimeout()
    {
        return DatabaseDescriptor.getRpcTimeout();
    }

    public void setReadRpcTimeout(long value)
    {
        DatabaseDescriptor.setReadRpcTimeout(value);
        logger.info("set read rpc timeout to {} ms", value);
    }

    public long getReadRpcTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public void setRangeRpcTimeout(long value)
    {
        DatabaseDescriptor.setRangeRpcTimeout(value);
        logger.info("set range rpc timeout to {} ms", value);
    }

    public long getRangeRpcTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    public void setWriteRpcTimeout(long value)
    {
        DatabaseDescriptor.setWriteRpcTimeout(value);
        logger.info("set write rpc timeout to {} ms", value);
    }

    public long getWriteRpcTimeout()
    {
        return DatabaseDescriptor.getWriteRpcTimeout();
    }

    public void setCounterWriteRpcTimeout(long value)
    {
        DatabaseDescriptor.setCounterWriteRpcTimeout(value);
        logger.info("set counter write rpc timeout to {} ms", value);
    }

    public long getCounterWriteRpcTimeout()
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout();
    }

    public void setCasContentionTimeout(long value)
    {
        DatabaseDescriptor.setCasContentionTimeout(value);
        logger.info("set cas contention rpc timeout to {} ms", value);
    }

    public long getCasContentionTimeout()
    {
        return DatabaseDescriptor.getCasContentionTimeout();
    }

    public void setTruncateRpcTimeout(long value)
    {
        DatabaseDescriptor.setTruncateRpcTimeout(value);
        logger.info("set truncate rpc timeout to {} ms", value);
    }

    public long getTruncateRpcTimeout()
    {
        return DatabaseDescriptor.getTruncateRpcTimeout();
    }

    public void setStreamThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(value);
        logger.info("setstreamthroughput: throttle set to {}", value);
    }

    public int getStreamThroughputMbPerSec()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
    }

    public void setInterDCStreamThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(value);
        logger.info("setinterdcstreamthroughput: throttle set to {}", value);
    }

    public int getInterDCStreamThroughputMbPerSec()
    {
        return DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec();
    }


    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
        CompactionManager.instance.setRate(value);
    }

    public int getBatchlogReplayThrottleInKB()
    {
        return DatabaseDescriptor.getBatchlogReplayThrottleInKB();
    }

    public void setBatchlogReplayThrottleInKB(int throttleInKB)
    {
        DatabaseDescriptor.setBatchlogReplayThrottleInKB(throttleInKB);
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

    public int getConcurrentValidators()
    {
        return DatabaseDescriptor.getConcurrentValidations();
    }

    public void setConcurrentValidators(int value)
    {
        DatabaseDescriptor.setConcurrentValidations(value);
        CompactionManager.instance.setConcurrentValidations(DatabaseDescriptor.getConcurrentValidations());
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

    private void setMode(Mode m, boolean log)
    {
        setMode(m, null, log);
    }

    private void setMode(Mode m, String msg, boolean log)
    {
        operationMode = m;
        String logMsg = msg == null ? m.toString() : String.format("%s: %s", m, msg);
        if (log)
            logger.info(logMsg);
        else
            logger.debug(logMsg);
    }

    /**
     * Bootstrap node by fetching data from other nodes.
     * If node is bootstrapping as a new node, then this also announces bootstrapping to the cluster.
     *
     * This blocks until streaming is done.
     *
     * @param tokens bootstrapping tokens
     * @return true if bootstrap succeeds.
     */
    private boolean bootstrap(final Collection<Token> tokens)
    {
        isBootstrapMode = true;
        SystemKeyspace.updateTokens(tokens); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping

        if (!replacing || !isReplacingSameAddress())
        {
            // if not an existing token then bootstrap
            List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>();
            states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
            states.add(Pair.create(ApplicationState.STATUS_WITH_PORT, replacing?
                                                            valueFactory.bootReplacingWithPort(DatabaseDescriptor.getReplaceAddress()) :
                                                            valueFactory.bootstrapping(tokens)));
            states.add(Pair.create(ApplicationState.STATUS, replacing?
                                                            valueFactory.bootReplacing(DatabaseDescriptor.getReplaceAddress().address) :
                                                            valueFactory.bootstrapping(tokens)));
            Gossiper.instance.addLocalApplicationStates(states);
            setMode(Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
            Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS);
        }
        else
        {
            // Dont set any state for the node which is bootstrapping the existing token...
            tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddressAndPort());
            SystemKeyspace.removeEndpoint(DatabaseDescriptor.getReplaceAddress());
        }
        if (!Gossiper.instance.seenAnySeed())
            throw new IllegalStateException("Unable to contact any seeds!");

        if (Boolean.getBoolean("cassandra.reset_bootstrap_progress"))
        {
            logger.info("Resetting bootstrap progress to start fresh");
            SystemKeyspace.resetAvailableRanges();
        }

        // Force disk boundary invalidation now that local tokens are set
        invalidateDiskBoundaries();

        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        BootStrapper bootstrapper = new BootStrapper(FBUtilities.getBroadcastAddressAndPort(), tokens, tokenMetadata);
        bootstrapper.addProgressListener(progressSupport);
        ListenableFuture<StreamState> bootstrapStream = bootstrapper.bootstrap(streamStateStore, useStrictConsistency && !replacing); // handles token update
        Futures.addCallback(bootstrapStream, new FutureCallback<StreamState>()
        {
            @Override
            public void onSuccess(StreamState streamState)
            {
                bootstrapFinished();
                logger.info("Bootstrap completed! for the tokens {}", tokens);
            }

            @Override
            public void onFailure(Throwable e)
            {
                logger.warn("Error during bootstrap.", e);
            }
        });
        try
        {
            bootstrapStream.get();
            return true;
        }
        catch (Throwable e)
        {
            logger.error("Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.", e);
            return false;
        }
    }

    private void invalidateDiskBoundaries()
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.invalidateDiskBoundaries();
                }
            }
        }
    }

    /**
     * All MVs have been created during bootstrap, so mark them as built
     */
    private void markViewsAsBuilt() {
        for (String keyspace : Schema.instance.getUserKeyspaces())
        {
            for (ViewMetadata view: Schema.instance.getKeyspaceMetadata(keyspace).views)
                SystemKeyspace.finishViewBuildStatus(view.keyspace, view.name);
        }
    }

    /**
     * Called when bootstrap did finish successfully
     */
    private void bootstrapFinished() {
        markViewsAsBuilt();
        isBootstrapMode = false;
    }

    public boolean resumeBootstrap()
    {
        if (isBootstrapMode && SystemKeyspace.bootstrapInProgress())
        {
            logger.info("Resuming bootstrap...");

            // get bootstrap tokens saved in system keyspace
            final Collection<Token> tokens = SystemKeyspace.getSavedTokens();
            // already bootstrapped ranges are filtered during bootstrap
            BootStrapper bootstrapper = new BootStrapper(FBUtilities.getBroadcastAddressAndPort(), tokens, tokenMetadata);
            bootstrapper.addProgressListener(progressSupport);
            ListenableFuture<StreamState> bootstrapStream = bootstrapper.bootstrap(streamStateStore, useStrictConsistency && !replacing); // handles token update
            Futures.addCallback(bootstrapStream, new FutureCallback<StreamState>()
            {
                @Override
                public void onSuccess(StreamState streamState)
                {
                    bootstrapFinished();
                    // start participating in the ring.
                    // pretend we are in survey mode so we can use joinRing() here
                    isSurveyMode = true;
                    try
                    {
                        progressSupport.progress("bootstrap", ProgressEvent.createNotification("Joining ring..."));
                        joinRing(true);
                    }
                    catch (IOException ignore)
                    {
                        // joinRing with survey mode does not throw IOException
                    }
                    progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
                    logger.info("Resume complete");
                }

                @Override
                public void onFailure(Throwable e)
                {
                    String message = "Error during bootstrap: ";
                    if (e instanceof ExecutionException && e.getCause() != null)
                    {
                        message += e.getCause().getMessage();
                    }
                    else
                    {
                        message += e.getMessage();
                    }
                    logger.error(message, e);
                    progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.ERROR, 1, 1, message));
                    progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
                }
            });
            return true;
        }
        else
        {
            logger.info("Resuming bootstrap is requested, but the node is already bootstrapped.");
            return false;
        }
    }

    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata;
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
        for (Map.Entry<Range<Token>,List<InetAddressAndPort>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey().asList(), stringify(entry.getValue(), withPort));
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
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return FBUtilities.getBroadcastNativeAddressAndPort().toString(withPort);
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT) != null)
        {
            try
            {
                InetAddressAndPort address = InetAddressAndPort.getByName(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value);
                return address.getHostAddress(withPort);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS) == null)
            return endpoint.address.getHostAddress() + ":" + DatabaseDescriptor.getNativeTransportPort();
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value + ":" + DatabaseDescriptor.getNativeTransportPort();
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
        for (Map.Entry<Range<Token>, List<InetAddressAndPort>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<>(entry.getValue().size());
            for (InetAddressAndPort endpoint: entry.getValue())
            {
                rpcaddrs.add(getNativeaddress(endpoint, withPort));
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
            keyspace = Schema.instance.getNonLocalStrategyKeyspaces().get(0);

        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, Collection<InetAddressAndPort>> entry : tokenMetadata.getPendingRangesMM(keyspace).asMap().entrySet())
        {
            List<InetAddressAndPort> l = new ArrayList<>(entry.getValue());
            map.put(entry.getKey().asList(), stringify(l, withPort));
        }
        return map;
    }

    public Map<Range<Token>, List<InetAddressAndPort>> getRangeToAddressMap(String keyspace)
    {
        return getRangeToAddressMap(keyspace, tokenMetadata.sortedTokens());
    }

    public Map<Range<Token>, List<InetAddressAndPort>> getRangeToAddressMapInLocalDC(String keyspace)
    {
        Predicate<InetAddressAndPort> isLocalDC = new Predicate<InetAddressAndPort>()
        {
            public boolean apply(InetAddressAndPort address)
            {
                return isLocalDC(address);
            }
        };

        Map<Range<Token>, List<InetAddressAndPort>> origMap = getRangeToAddressMap(keyspace, getTokensInLocalDC());
        Map<Range<Token>, List<InetAddressAndPort>> filteredMap = Maps.newHashMap();
        for (Map.Entry<Range<Token>, List<InetAddressAndPort>> entry : origMap.entrySet())
        {
            List<InetAddressAndPort> endpointsInLocalDC = Lists.newArrayList(Collections2.filter(entry.getValue(), isLocalDC));
            filteredMap.put(entry.getKey(), endpointsInLocalDC);
        }

        return filteredMap;
    }

    private List<Token> getTokensInLocalDC()
    {
        List<Token> filteredTokens = Lists.newArrayList();
        for (Token token : tokenMetadata.sortedTokens())
        {
            InetAddressAndPort endpoint = tokenMetadata.getEndpoint(token);
            if (isLocalDC(endpoint))
                filteredTokens.add(token);
        }
        return filteredTokens;
    }

    private boolean isLocalDC(InetAddressAndPort targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());
        return remoteDC.equals(localDC);
    }

    private Map<Range<Token>, List<InetAddressAndPort>> getRangeToAddressMap(String keyspace, List<Token> sortedTokens)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonLocalStrategyKeyspaces().get(0);

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

    /**
     * The same as {@code describeRing(String)} but considers only the part of the ring formed by nodes in the local DC.
     */
    public List<TokenRange> describeLocalRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, true, false);
    }

    private List<TokenRange> describeRing(String keyspace, boolean includeOnlyLocalDC, boolean withPort) throws InvalidRequestException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspace))
            throw new InvalidRequestException("No such keyspace: " + keyspace);

        if (keyspace == null || Keyspace.open(keyspace).getReplicationStrategy() instanceof LocalStrategy)
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);

        List<TokenRange> ranges = new ArrayList<>();
        Token.TokenFactory tf = getTokenFactory();

        Map<Range<Token>, List<InetAddressAndPort>> rangeToAddressMap =
                includeOnlyLocalDC
                        ? getRangeToAddressMapInLocalDC(keyspace)
                        : getRangeToAddressMap(keyspace);

        for (Map.Entry<Range<Token>, List<InetAddressAndPort>> entry : rangeToAddressMap.entrySet())
            ranges.add(TokenRange.create(tf, entry.getKey(), entry.getValue(), withPort));

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
        Map<Token, InetAddressAndPort> mapInetAddress = tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap();
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<>(mapInetAddress.size());
        List<Token> tokens = new ArrayList<>(mapInetAddress.keySet());
        Collections.sort(tokens);
        for (Token token : tokens)
        {
            mapString.put(token.toString(), mapInetAddress.get(token).getHostAddress(withPort));
        }
        return mapString;
    }

    public String getLocalHostId()
    {
        return getTokenMetadata().getHostId(FBUtilities.getBroadcastAddressAndPort()).toString();
    }

    public UUID getLocalHostUUID()
    {
        return getTokenMetadata().getHostId(FBUtilities.getBroadcastAddressAndPort());
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
        for (Map.Entry<InetAddressAndPort, UUID> entry : getTokenMetadata().getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(withPort), entry.getValue().toString());
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
        for (Map.Entry<InetAddressAndPort, UUID> entry : getTokenMetadata().getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getValue().toString(), entry.getKey().getHostAddress(withPort));
        return mapOut;
    }

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range<Token>, List<InetAddressAndPort>> constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges)
    {
        Map<Range<Token>, List<InetAddressAndPort>> rangeToEndpointMap = new HashMap<>(ranges.size());
        for (Range<Token> range : ranges)
        {
            rangeToEndpointMap.put(range, Keyspace.open(keyspace).getReplicationStrategy().getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
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
        if (state == ApplicationState.STATUS || state == ApplicationState.STATUS_WITH_PORT)
        {
            String[] pieces = splitValue(value);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName)
            {
                case VersionedValue.STATUS_BOOTSTRAPPING_REPLACE:
                    handleStateBootreplacing(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_BOOTSTRAPPING:
                    handleStateBootstrap(endpoint);
                    break;
                case VersionedValue.STATUS_NORMAL:
                    handleStateNormal(endpoint, VersionedValue.STATUS_NORMAL);
                    break;
                case VersionedValue.SHUTDOWN:
                    handleStateNormal(endpoint, VersionedValue.SHUTDOWN);
                    break;
                case VersionedValue.REMOVING_TOKEN:
                case VersionedValue.REMOVED_TOKEN:
                    handleStateRemoving(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_LEAVING:
                    handleStateLeaving(endpoint);
                    break;
                case VersionedValue.STATUS_LEFT:
                    handleStateLeft(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_MOVING:
                    handleStateMoving(endpoint, pieces);
                    break;
            }
        }
        else
        {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState))
            {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            if (getTokenMetadata().isMember(endpoint))
            {
                switch (state)
                {
                    case RELEASE_VERSION:
                        SystemKeyspace.updatePeerInfo(endpoint, "release_version", value.value);
                        break;
                    case DC:
                        updateTopology(endpoint);
                        SystemKeyspace.updatePeerInfo(endpoint, "data_center", value.value);
                        break;
                    case RACK:
                        updateTopology(endpoint);
                        SystemKeyspace.updatePeerInfo(endpoint, "rack", value.value);
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
                    case SCHEMA:
                        SystemKeyspace.updatePeerInfo(endpoint, "schema_version", UUID.fromString(value.value));
                        MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
                        break;
                    case HOST_ID:
                        SystemKeyspace.updatePeerInfo(endpoint, "host_id", UUID.fromString(value.value));
                        break;
                    case RPC_READY:
                        notifyRpcChange(endpoint, epState.isRpcReady());
                        break;
                    case NET_VERSION:
                        updateNetVersion(endpoint, value);
                        break;
                }
            }
        }
    }

    private static String[] splitValue(VersionedValue value)
    {
        return value.value.split(VersionedValue.DELIMITER_STR, -1);
    }

    private void updateNetVersion(InetAddressAndPort endpoint, VersionedValue value)
    {
        try
        {
            MessagingService.instance().setVersion(endpoint, Integer.parseInt(value.value));
        }
        catch (NumberFormatException e)
        {
            throw new AssertionError("Got invalid value for NET_VERSION application state: " + value.value);
        }
    }

    public void updateTopology(InetAddressAndPort endpoint)
    {
        if (getTokenMetadata().isMember(endpoint))
        {
            getTokenMetadata().updateTopology(endpoint);
        }
    }

    public void updateTopology()
    {
        getTokenMetadata().updateTopology();
    }

    private void updatePeerInfo(InetAddressAndPort endpoint)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        InetAddress native_address = null;
        int native_port = DatabaseDescriptor.getNativeTransportPort();

        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.states())
        {
            switch (entry.getKey())
            {
                case RELEASE_VERSION:
                    SystemKeyspace.updatePeerInfo(endpoint, "release_version", entry.getValue().value);
                    break;
                case DC:
                    SystemKeyspace.updatePeerInfo(endpoint, "data_center", entry.getValue().value);
                    break;
                case RACK:
                    SystemKeyspace.updatePeerInfo(endpoint, "rack", entry.getValue().value);
                    break;
                case RPC_ADDRESS:
                    try
                    {
                        native_address = InetAddress.getByName(entry.getValue().value);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case NATIVE_ADDRESS_AND_PORT:
                    try
                    {
                        InetAddressAndPort address = InetAddressAndPort.getByName(entry.getValue().value);
                        native_address = address.address;
                        native_port = address.port;
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case SCHEMA:
                    SystemKeyspace.updatePeerInfo(endpoint, "schema_version", UUID.fromString(entry.getValue().value));
                    break;
                case HOST_ID:
                    SystemKeyspace.updatePeerInfo(endpoint, "host_id", UUID.fromString(entry.getValue().value));
                    break;
            }
        }

        //Some tests won't set all the states
        if (native_address != null)
        {
            SystemKeyspace.updatePeerNativeAddress(endpoint,
                                                   InetAddressAndPort.getByAddressOverrideDefaults(native_address,
                                                                                                   native_port));
        }
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

    private void notifyJoined(InetAddressAndPort endpoint)
    {
        if (!isStatus(endpoint, VersionedValue.STATUS_NORMAL))
            return;

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onJoinCluster(endpoint);
    }

    private void notifyMoved(InetAddressAndPort endpoint)
    {
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onMove(endpoint);
    }

    private void notifyLeft(InetAddressAndPort endpoint)
    {
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(endpoint);
    }

    private boolean isStatus(InetAddressAndPort endpoint, String status)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        return state != null && state.getStatus().equals(status);
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
     * @param value - true indicates that RPC is ready, false indicates the opposite.
     */
    public void setRpcReady(boolean value)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        // if value is false we're OK with a null state, if it is true we are not.
        assert !value || state != null;

        if (state != null)
            Gossiper.instance.addLocalApplicationState(ApplicationState.RPC_READY, valueFactory.rpcReady(value));
    }

    private Collection<Token> getTokensFor(InetAddressAndPort endpoint)
    {
        try
        {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (state == null)
                return Collections.emptyList();

            VersionedValue versionedValue = state.getApplicationState(ApplicationState.TOKENS);
            if (versionedValue == null)
                return Collections.emptyList();

            return TokenSerializer.deserialize(tokenMetadata.partitioner, new DataInputStream(new ByteArrayInputStream(versionedValue.toBytes())));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    private void handleStateBootstrap(InetAddressAndPort endpoint)
    {
        Collection<Token> tokens;
        // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata.isMember(endpoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetadata.isLeaving(endpoint))
                logger.info("Node {} state jump to bootstrap", endpoint);
            tokenMetadata.removeEndpoint(endpoint);
        }

        tokenMetadata.addBootstrapTokens(tokens, endpoint);
        PendingRangeCalculatorService.instance.update();

        tokenMetadata.updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
    }

    private void handleStateBootreplacing(InetAddressAndPort newNode, String[] pieces)
    {
        InetAddressAndPort oldNode;
        try
        {
            oldNode = InetAddressAndPort.getByName(pieces[1]);
        }
        catch (Exception e)
        {
            logger.error("Node {} tried to replace malformed endpoint {}.", newNode, pieces[1], e);
            return;
        }

        if (FailureDetector.instance.isAlive(oldNode))
        {
            throw new RuntimeException(String.format("Node %s is trying to replace alive node %s.", newNode, oldNode));
        }

        Optional<InetAddressAndPort> replacingNode = tokenMetadata.getReplacingNode(newNode);
        if (replacingNode.isPresent() && !replacingNode.get().equals(oldNode))
        {
            throw new RuntimeException(String.format("Node %s is already replacing %s but is trying to replace %s.",
                                                     newNode, replacingNode.get(), oldNode));
        }

        Collection<Token> tokens = getTokensFor(newNode);

        if (logger.isDebugEnabled())
            logger.debug("Node {} is replacing {}, tokens {}", newNode, oldNode, tokens);

        tokenMetadata.addReplaceTokens(tokens, newNode, oldNode);
        PendingRangeCalculatorService.instance.update();

        tokenMetadata.updateHostId(Gossiper.instance.getHostId(newNode), newNode);
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    private void handleStateNormal(final InetAddressAndPort endpoint, final String status)
    {
        Collection<Token> tokens = getTokensFor(endpoint);
        Set<Token> tokensToUpdateInMetadata = new HashSet<>();
        Set<Token> tokensToUpdateInSystemKeyspace = new HashSet<>();
        Set<InetAddressAndPort> endpointsToRemove = new HashSet<>();

        if (logger.isDebugEnabled())
            logger.debug("Node {} state {}, token {}", endpoint, status, tokens);

        if (tokenMetadata.isMember(endpoint))
            logger.info("Node {} state jump to {}", endpoint, status);

        if (tokens.isEmpty() && status.equals(VersionedValue.STATUS_NORMAL))
            logger.error("Node {} is in state normal but it has no tokens, state: {}",
                         endpoint,
                         Gossiper.instance.getEndpointStateForEndpoint(endpoint));

        Optional<InetAddressAndPort> replacingNode = tokenMetadata.getReplacingNode(endpoint);
        if (replacingNode.isPresent())
        {
            assert !endpoint.equals(replacingNode.get()) : "Pending replacement endpoint with same address is not supported";
            logger.info("Node {} will complete replacement of {} for tokens {}", endpoint, replacingNode.get(), tokens);
            if (FailureDetector.instance.isAlive(replacingNode.get()))
            {
                logger.error("Node {} cannot complete replacement of alive node {}.", endpoint, replacingNode.get());
                return;
            }
            endpointsToRemove.add(replacingNode.get());
        }

        Optional<InetAddressAndPort> replacementNode = tokenMetadata.getReplacementNode(endpoint);
        if (replacementNode.isPresent())
        {
            logger.warn("Node {} is currently being replaced by node {}.", endpoint, replacementNode.get());
        }

        updatePeerInfo(endpoint);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
        UUID hostId = Gossiper.instance.getHostId(endpoint);
        InetAddressAndPort existing = tokenMetadata.getEndpointForHostId(hostId);
        if (replacing && isReplacingSameAddress() && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null
            && (hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress()))))
            logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
        else
        {
            if (existing != null && !existing.equals(endpoint))
            {
                if (existing.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                    tokenMetadata.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                }
                else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0)
                {
                    logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing, endpoint, endpoint);
                    tokenMetadata.removeEndpoint(existing);
                    endpointsToRemove.add(existing);
                    tokenMetadata.updateHostId(hostId, endpoint);
                }
                else
                {
                    logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, endpoint, endpoint);
                    tokenMetadata.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                }
            }
            else
                tokenMetadata.updateHostId(hostId, endpoint);
        }

        for (final Token token : tokens)
        {
            // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
            InetAddressAndPort currentOwner = tokenMetadata.getEndpoint(token);
            if (currentOwner == null)
            {
                logger.debug("New node {} at token {}", endpoint, token);
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            }
            else if (endpoint.equals(currentOwner))
            {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            }
            else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0)
            {
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);

                // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                Multimap<InetAddressAndPort, Token> epToTokenCopy = getTokenMetadata().getEndpointToTokenMapForReading();
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).size() < 1)
                    endpointsToRemove.add(currentOwner);

                logger.info("Nodes {} and {} have the same token {}.  {} is the new owner",
                            endpoint,
                            currentOwner,
                            token,
                            endpoint);
            }
            else
            {
                logger.info("Nodes {} and {} have the same token {}.  Ignoring {}",
                            endpoint,
                            currentOwner,
                            token,
                            endpoint);
            }
        }

        // capture because updateNormalTokens clears moving and member status
        boolean isMember = tokenMetadata.isMember(endpoint);
        boolean isMoving = tokenMetadata.isMoving(endpoint);
        tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (InetAddressAndPort ep : endpointsToRemove)
        {
            removeEndpoint(ep);
            if (replacing && DatabaseDescriptor.getReplaceAddress().equals(ep))
                Gossiper.instance.replacementQuarantine(ep); // quarantine locally longer than normally; see CASSANDRA-8260
        }
        if (!tokensToUpdateInSystemKeyspace.isEmpty())
            SystemKeyspace.updateTokens(endpoint, tokensToUpdateInSystemKeyspace);

        if (isMoving || operationMode == Mode.MOVING)
        {
            tokenMetadata.removeFromMoving(endpoint);
            notifyMoved(endpoint);
        }
        else if (!isMember) // prior to this, the node was not a member
        {
            notifyJoined(endpoint);
        }

        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    private void handleStateLeaving(InetAddressAndPort endpoint)
    {
        Collection<Token> tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata.isMember(endpoint))
        {
            logger.info("Node {} state jump to leaving", endpoint);
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }
        else if (!tokenMetadata.getTokens(endpoint).containsAll(tokens))
        {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata.addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddressAndPort endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Collection<Token> tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state left, tokens {}", endpoint, tokens);

        excise(tokens, endpoint, extractExpireTime(pieces));
    }

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    private void handleStateMoving(InetAddressAndPort endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Token token = getTokenFactory().fromString(pieces[1]);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state moving, new token {}", endpoint, token);

        tokenMetadata.addMovingEndpoint(token, endpoint);

        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(InetAddressAndPort endpoint, String[] pieces)
    {
        assert (pieces.length > 0);

        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
            try
            {
                drain();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return;
        }
        if (tokenMetadata.isMember(endpoint))
        {
            String state = pieces[0];
            Collection<Token> removeTokens = tokenMetadata.getTokens(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state))
            {
                excise(removeTokens, endpoint, extractExpireTime(pieces));
            }
            else if (VersionedValue.REMOVING_TOKEN.equals(state))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

                // Note that the endpoint is being removed
                tokenMetadata.addLeavingEndpoint(endpoint);
                PendingRangeCalculatorService.instance.update();

                // find the endpoint coordinating this removal that we need to notify when we're done
                String[] coordinator = splitValue(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR));
                UUID hostId = UUID.fromString(coordinator[1]);
                // grab any data we are now responsible for and notify responsible node
                restoreReplicaCount(endpoint, tokenMetadata.getEndpointForHostId(hostId));
            }
        }
        else // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        {
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
            removeEndpoint(endpoint);
        }
    }

    private void excise(Collection<Token> tokens, InetAddressAndPort endpoint)
    {
        logger.info("Removing tokens {} for {}", tokens, endpoint);

        UUID hostId = tokenMetadata.getHostId(endpoint);
        if (hostId != null && tokenMetadata.isMember(endpoint))
            HintsService.instance.excise(hostId);

        removeEndpoint(endpoint);
        tokenMetadata.removeEndpoint(endpoint);
        if (!tokens.isEmpty())
            tokenMetadata.removeBootstrapTokens(tokens);
        notifyLeft(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    private void excise(Collection<Token> tokens, InetAddressAndPort endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tokens, endpoint);
    }

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    private void removeEndpoint(InetAddressAndPort endpoint)
    {
        Gossiper.instance.removeEndpoint(endpoint);
        SystemKeyspace.removeEndpoint(endpoint);
    }

    protected void addExpireTimeIfFound(InetAddressAndPort endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    protected long extractExpireTime(String[] pieces)
    {
        return Long.parseLong(pieces[2]);
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param keyspaceName the keyspace ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    private Multimap<InetAddressAndPort, Range<Token>> getNewSourceRanges(String keyspaceName, Set<Range<Token>> ranges)
    {
        InetAddressAndPort myAddress = FBUtilities.getBroadcastAddressAndPort();
        Multimap<Range<Token>, InetAddressAndPort> rangeAddresses = Keyspace.open(keyspaceName).getReplicationStrategy().getRangeAddresses(tokenMetadata.cloneOnlyTokenMap());
        Multimap<InetAddressAndPort, Range<Token>> sourceRanges = HashMultimap.create();
        IFailureDetector failureDetector = FailureDetector.instance;

        // find alive sources for our new ranges
        for (Range<Token> range : ranges)
        {
            Collection<InetAddressAndPort> possibleRanges = rangeAddresses.get(range);
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            List<InetAddressAndPort> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);

            assert (!sources.contains(myAddress));

            for (InetAddressAndPort source : sources)
            {
                if (failureDetector.isAlive(source))
                {
                    sourceRanges.put(source, range);
                    break;
                }
            }
        }
        return sourceRanges;
    }

    /**
     * Sends a notification to a node indicating we have finished replicating data.
     *
     * @param remote node to send notification to
     */
    private void sendReplicationNotification(InetAddressAndPort remote)
    {
        // notify the remote token
        MessageOut msg = new MessageOut(MessagingService.Verb.REPLICATION_FINISHED);
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger.isDebugEnabled())
            logger.debug("Notifying {} of replication completion\n", remote);
        while (failureDetector.isAlive(remote))
        {
            AsyncOneResponse iar = MessagingService.instance().sendRR(msg, remote);
            try
            {
                iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                return; // done
            }
            catch(TimeoutException e)
            {
                // try again
            }
        }
    }

    /**
     * Called when an endpoint is removed from the ring. This function checks
     * whether this node becomes responsible for new ranges as a
     * consequence and streams data if needed.
     *
     * This is rather ineffective, but it does not matter so much
     * since this is called very seldom
     *
     * @param endpoint the node that left
     */
    private void restoreReplicaCount(InetAddressAndPort endpoint, final InetAddressAndPort notifyEndpoint)
    {
        Multimap<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();

        InetAddressAndPort myAddress = FBUtilities.getBroadcastAddressAndPort();

        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
        {
            Multimap<Range<Token>, InetAddressAndPort> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
            Set<Range<Token>> myNewRanges = new HashSet<>();
            for (Map.Entry<Range<Token>, InetAddressAndPort> entry : changedRanges.entries())
            {
                if (entry.getValue().equals(myAddress))
                    myNewRanges.add(entry.getKey());
            }
            Multimap<InetAddressAndPort, Range<Token>> sourceRanges = getNewSourceRanges(keyspaceName, myNewRanges);
            for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : sourceRanges.asMap().entrySet())
            {
                rangesToFetch.put(keyspaceName, entry);
            }
        }

        StreamPlan stream = new StreamPlan(StreamOperation.RESTORE_REPLICA_COUNT);
        for (String keyspaceName : rangesToFetch.keySet())
        {
            for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : rangesToFetch.get(keyspaceName))
            {
                InetAddressAndPort source = entry.getKey();
                InetAddressAndPort preferred = SystemKeyspace.getPreferredIP(source);
                Collection<Range<Token>> ranges = entry.getValue();
                if (logger.isDebugEnabled())
                    logger.debug("Requesting from {} ranges {}", source, StringUtils.join(ranges, ", "));
                stream.requestRanges(source, preferred, keyspaceName, ranges);
            }
        }
        StreamResultFuture future = stream.execute();
        Futures.addCallback(future, new FutureCallback<StreamState>()
        {
            public void onSuccess(StreamState finalState)
            {
                sendReplicationNotification(notifyEndpoint);
            }

            public void onFailure(Throwable t)
            {
                logger.warn("Streaming to restore replica count failed", t);
                // We still want to send the notification
                sendReplicationNotification(notifyEndpoint);
            }
        });
    }

    // needs to be modified to accept either a keyspace or ARS.
    private Multimap<Range<Token>, InetAddressAndPort> getChangedRangesForLeaving(String keyspaceName, InetAddressAndPort endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range<Token>> ranges = getRangesForEndpoint(keyspaceName, endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} ranges [{}]", endpoint, StringUtils.join(ranges, ", "));

        Map<Range<Token>, List<InetAddressAndPort>> currentReplicaEndpoints = new HashMap<>(ranges.size());

        // Find (for each range) all nodes that store replicas for these ranges as well
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap(); // don't do this in the loop! #7758
        for (Range<Token> range : ranges)
            currentReplicaEndpoints.put(range, Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints(range.right, metadata));

        TokenMetadata temp = tokenMetadata.cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removenode
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range<Token>, InetAddressAndPort> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoints list, but
        // not in the currentReplicaEndpoints list, will be needing the
        // range.
        for (Range<Token> range : ranges)
        {
            Collection<InetAddressAndPort> newReplicaEndpoints = Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints(range.right, temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger.isDebugEnabled())
                if (newReplicaEndpoints.isEmpty())
                    logger.debug("Range {} already in all replicas", range);
                else
                    logger.debug("Range {} will be responsibility of {}", range, StringUtils.join(newReplicaEndpoints, ", "));
            changedRanges.putAll(range, newReplicaEndpoints);
        }

        return changedRanges;
    }

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.states())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
        MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
    }

    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        MigrationManager.instance.scheduleSchemaPull(endpoint, state);

        if (tokenMetadata.isMember(endpoint))
            notifyUp(endpoint);
    }

    public void onRemove(InetAddressAndPort endpoint)
    {
        tokenMetadata.removeEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        MessagingService.instance().convict(endpoint);
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


    public String getLoadString()
    {
        return FileUtils.stringifyFileSize(StorageMetrics.load.getCount());
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
        map.put(withPort ? FBUtilities.getJustBroadcastAddress().getHostAddress() : FBUtilities.getBroadcastAddressAndPort().toString(), getLoadString());
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
        return tokenMetadata.getEndpointForHostId(hostId);
    }

    @Nullable
    public UUID getHostIdForEndpoint(InetAddressAndPort address)
    {
        return tokenMetadata.getHostId(address);
    }

    /* These methods belong to the MBean interface */

    public List<String> getTokens()
    {
        return getTokens(FBUtilities.getBroadcastAddressAndPort());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException
    {
        return getTokens(InetAddressAndPort.getByName(endpoint));
    }

    private List<String> getTokens(InetAddressAndPort endpoint)
    {
        List<String> strTokens = new ArrayList<>();
        for (Token tok : getTokenMetadata().getTokens(endpoint))
            strTokens.add(tok.toString());
        return strTokens;
    }

    public String getReleaseVersion()
    {
        return FBUtilities.getReleaseVersionString();
    }

    public String getSchemaVersion()
    {
        return Schema.instance.getVersion().toString();
    }

    @Deprecated
    public List<String> getLeavingNodes()
    {
        return stringify(tokenMetadata.getLeavingEndpoints(), false);
    }

    public List<String> getLeavingNodesWithPort()
    {
        return stringify(tokenMetadata.getLeavingEndpoints(), true);
    }

    @Deprecated
    public List<String> getMovingNodes()
    {
        List<String> endpoints = new ArrayList<>();

        for (Pair<Token, InetAddressAndPort> node : tokenMetadata.getMovingEndpoints())
        {
            endpoints.add(node.right.address.getHostAddress());
        }

        return endpoints;
    }

    public List<String> getMovingNodesWithPort()
    {
        List<String> endpoints = new ArrayList<>();

        for (Pair<Token, InetAddressAndPort> node : tokenMetadata.getMovingEndpoints())
        {
            endpoints.add(node.right.toString());
        }

        return endpoints;
    }


    public List<String> getJoiningNodes()
    {
        return stringify(tokenMetadata.getBootstrapTokens().valueSet(), false);
    }

    @Deprecated
    public List<String> getJoiningNodesWithPort()
    {
        return stringify(tokenMetadata.getBootstrapTokens().valueSet(), true);
    }

    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers(), false);
    }

    @Deprecated
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

            if (tokenMetadata.isMember(ep))
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

    public String[] getAllDataFileLocations()
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        for (int i = 0; i < locations.length; i++)
            locations[i] = FileUtils.getCanonicalPath(locations[i]);
        return locations;
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
        return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddressAndPort());
    }

    public int forceKeyspaceCleanup(String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return forceKeyspaceCleanup(0, keyspaceName, tables);
    }

    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        if (SchemaConstants.isLocalSystemKeyspace(keyspaceName))
            throw new RuntimeException("Cleanup of the system keyspace is neither necessary nor wise");

        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, tables))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.forceCleanup(jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, true, 0, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, checkData, 0, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, checkData, false, jobs, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tables))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, checkData, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
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
        Verifier.Options options = Verifier.options().invokeDiskFailurePolicy(diskFailurePolicy)
                                                     .extendedVerification(extendedVerify)
                                                     .checkVersion(checkVersion)
                                                     .mutateRepairStatus(mutateRepairStatus)
                                                     .checkOwnsTokens(checkOwnsTokens)
                                                     .quick(quick).build();
        logger.info("Verifying {}.{} with options = {}", keyspaceName, Arrays.toString(tableNames), options);
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.verify(options);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return upgradeSSTables(keyspaceName, excludeCurrentVersion, 0, tableNames);
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, true, keyspaceName, tableNames))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.sstablesRewrite(excludeCurrentVersion, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            cfStore.forceMajorCompaction(splitOutput);
        }
    }

    public int relocateSSTables(String keyspaceName, String ... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        return relocateSSTables(0, keyspaceName, columnFamilies);
    }

    public int relocateSSTables(int jobs, String keyspaceName, String ... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfs : getValidColumnFamilies(false, false, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfs.relocateSSTables(jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public int garbageCollect(String tombstoneOptionString, int jobs, String keyspaceName, String ... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        TombstoneOption tombstoneOption = TombstoneOption.valueOf(tombstoneOptionString);
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfs : getValidColumnFamilies(false, false, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfs.garbageCollect(tombstoneOption, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
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
        boolean skipFlush = Boolean.parseBoolean(options.getOrDefault("skipFlush", "false"));

        if (entities != null && entities.length > 0 && entities[0].contains("."))
        {
            takeMultipleTableSnapshot(tag, skipFlush, entities);
        }
        else
        {
            takeSnapshot(tag, skipFlush, entities);
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
        takeMultipleTableSnapshot(tag, false, keyspaceName + "." + tableName);
    }

    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        Collection<Range<Token>> tokenRanges = createRepairRangeFrom(startToken, endToken);

        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, tableNames))
        {
            cfStore.forceCompactionForTokenRange(tokenRanges);
        }
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        takeSnapshot(tag, false, keyspaceNames);
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
        takeMultipleTableSnapshot(tag, false, tableList);
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param skipFlush Skip blocking flush of memtable
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    private void takeSnapshot(String tag, boolean skipFlush, String... keyspaceNames) throws IOException
    {
        if (operationMode == Mode.JOINING)
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


        for (Keyspace keyspace : keyspaces)
            keyspace.snapshot(tag, null, skipFlush);
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
    private void takeMultipleTableSnapshot(String tag, boolean skipFlush, String... tableList)
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
                if (operationMode.equals(Mode.JOINING))
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

        for (Entry<Keyspace, List<String>> entry : keyspaceColumnfamily.entrySet())
        {
            for (String table : entry.getValue())
                entry.getKey().snapshot(tag, table, skipFlush);
        }

    }

    private Keyspace getValidKeyspace(String keyspaceName) throws IOException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
        {
            throw new IOException("Keyspace " + keyspaceName + " does not exist");
        }
        return Keyspace.open(keyspaceName);
    }

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        if(tag == null)
            tag = "";

        Set<String> keyspaces = new HashSet<>();
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            for(String keyspaceDir : new File(dataDir).list())
            {
                // Only add a ks if it has been specified as a param, assuming params were actually provided.
                if (keyspaceNames.length > 0 && !Arrays.asList(keyspaceNames).contains(keyspaceDir))
                    continue;
                keyspaces.add(keyspaceDir);
            }
        }

        for (String keyspace : keyspaces)
            Keyspace.clearSnapshot(tag, keyspace);

        if (logger.isDebugEnabled())
            logger.debug("Cleared out snapshot directories");
    }

    public Map<String, TabularData> getSnapshotDetails()
    {
        Map<String, TabularData> snapshotMap = new HashMap<>();
        for (Keyspace keyspace : Keyspace.all())
        {
            if (SchemaConstants.isLocalSystemKeyspace(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                for (Map.Entry<String, Pair<Long,Long>> snapshotDetail : cfStore.getSnapshotDetails().entrySet())
                {
                    TabularDataSupport data = (TabularDataSupport)snapshotMap.get(snapshotDetail.getKey());
                    if (data == null)
                    {
                        data = new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE);
                        snapshotMap.put(snapshotDetail.getKey(), data);
                    }

                    SnapshotDetailsTabularData.from(snapshotDetail.getKey(), keyspace.getName(), cfStore.getTableName(), snapshotDetail, data);
                }
            }
        }
        return snapshotMap;
    }

    public long trueSnapshotsSize()
    {
        long total = 0;
        for (Keyspace keyspace : Keyspace.all())
        {
            if (SchemaConstants.isLocalSystemKeyspace(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                total += cfStore.trueSnapshotsSize();
            }
        }

        return total;
    }

    public void refreshSizeEstimates() throws ExecutionException
    {
        FBUtilities.waitOnFuture(ScheduledExecutors.optionalTasks.submit(SizeEstimatesRecorder.instance));
    }

    /**
     * @param allowIndexes Allow index CF names to be passed in
     * @param autoAddIndexes Automatically add secondary indexes if a CF has them
     * @param keyspaceName keyspace
     * @param cfNames CFs
     * @throws java.lang.IllegalArgumentException when given CF name does not exist
     */
    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String keyspaceName, String... cfNames) throws IOException
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
            cfStore.forceBlockingFlush();
        }
    }

    public int repairAsync(String keyspace, Map<String, String> repairSpec)
    {
        RepairOption option = RepairOption.parse(repairSpec, tokenMetadata.partitioner);
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
                option.getRanges().addAll(getLocalRanges(keyspace));
            }
        }
        if (option.getRanges().isEmpty() || Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor() < 2)
            return 0;

        int cmd = nextRepairCommand.incrementAndGet();
        ActiveRepairService.repairCommandExecutor.execute(createRepairTask(cmd, keyspace, option));
        return cmd;
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

        ArrayList<Token> tokens = new ArrayList<>(tokenMetadata.sortedTokens());
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
        return tokenMetadata.partitioner.getTokenFactory();
    }

    private FutureTask<Object> createRepairTask(final int cmd, final String keyspace, final RepairOption options)
    {
        if (!options.getDataCenters().isEmpty() && !options.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter()))
        {
            throw new IllegalArgumentException("the local data center must be part of the repair");
        }

        RepairRunnable task = new RepairRunnable(this, cmd, options, keyspace);
        task.addProgressListener(progressSupport);
        if (options.isTraced())
        {
            Runnable r = () ->
            {
                try
                {
                    task.run();
                }
                finally
                {
                    ExecutorLocals.set(null);
                }
            };
            return new FutureTask<>(r, null);
        }
        return new FutureTask<>(task, null);
    }

    public void forceTerminateAllRepairSessions()
    {
        ActiveRepairService.instance.terminateSessions();
    }


    @Nullable
    public List<String> getParentRepairStatus(int cmd)
    {
        Pair<ActiveRepairService.ParentRepairStatus, List<String>> pair = ActiveRepairService.instance.getRepairStatus(cmd);
        return pair == null ? null :
               ImmutableList.<String>builder().add(pair.left.name()).addAll(pair.right).build();
    }

    /* End of MBean interface methods */

    /**
     * Get the "primary ranges" for the specified keyspace and endpoint.
     * "Primary ranges" are the ranges that the node is responsible for storing replica primarily.
     * The node that stores replica primarily is defined as the first node returned
     * by {@link AbstractReplicationStrategy#calculateNaturalEndpoints}.
     *
     * @param keyspace Keyspace name to check primary ranges
     * @param ep endpoint we are interested in.
     * @return primary ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddressAndPort ep)
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = new HashSet<>();
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        for (Token token : metadata.sortedTokens())
        {
            List<InetAddressAndPort> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            if (endpoints.size() > 0 && endpoints.get(0).equals(ep))
                primaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
        }
        return primaryRanges;
    }

    /**
     * Get the "primary ranges" within local DC for the specified keyspace and endpoint.
     *
     * @see #getPrimaryRangesForEndpoint(String, InetAddressAndPort)
     * @param keyspace Keyspace name to check primary ranges
     * @param referenceEndpoint endpoint we are interested in.
     * @return primary ranges within local DC for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangeForEndpointWithinDC(String keyspace, InetAddressAndPort referenceEndpoint)
    {
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(referenceEndpoint);
        Collection<InetAddressAndPort> localDcNodes = metadata.getTopology().getDatacenterEndpoints().get(localDC);
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();

        Collection<Range<Token>> localDCPrimaryRanges = new HashSet<>();
        for (Token token : metadata.sortedTokens())
        {
            List<InetAddressAndPort> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            for (InetAddressAndPort endpoint : endpoints)
            {
                if (localDcNodes.contains(endpoint))
                {
                    if (endpoint.equals(referenceEndpoint))
                    {
                        localDCPrimaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
                    }
                    break;
                }
            }
        }

        return localDCPrimaryRanges;
    }

    /**
     * Get all ranges an endpoint is responsible for (by keyspace)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range<Token>> getRangesForEndpoint(String keyspaceName, InetAddressAndPort ep)
    {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
    }

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range<Token>> getAllRanges(List<Token> sortedTokens)
    {
        if (logger.isTraceEnabled())
            logger.trace("computing ranges for {}", StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        int size = sortedTokens.size();
        List<Range<Token>> ranges = new ArrayList<>(size + 1);
        for (int i = 1; i < size; ++i)
        {
            Range<Token> range = new Range<>(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range<Token> range = new Range<>(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

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
        KeyspaceMetadata ksMetaData = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (ksMetaData == null)
            throw new IllegalArgumentException("Unknown keyspace '" + keyspaceName + "'");

        TableMetadata metadata = ksMetaData.getTableOrViewNullable(cf);
        if (metadata == null)
            throw new IllegalArgumentException("Unknown table '" + cf + "' in keyspace '" + keyspaceName + "'");

        return getNaturalEndpoints(keyspaceName, tokenMetadata.partitioner.getToken(metadata.partitionKeyType.fromString(key))).stream().map(i -> i.address).collect(toList());
    }

    public List<String> getNaturalEndpointsWithPort(String keyspaceName, String cf, String key)
    {
        KeyspaceMetadata ksMetaData = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (ksMetaData == null)
            throw new IllegalArgumentException("Unknown keyspace '" + keyspaceName + "'");

        TableMetadata metadata = ksMetaData.getTableOrViewNullable(cf);
        if (metadata == null)
            throw new IllegalArgumentException("Unknown table '" + cf + "' in keyspace '" + keyspaceName + "'");

        return stringify(getNaturalEndpoints(keyspaceName, tokenMetadata.partitioner.getToken(metadata.partitionKeyType.fromString(key))), true);
    }


    @Deprecated
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key)
    {
        return getNaturalEndpoints(keyspaceName, tokenMetadata.partitioner.getToken(key)).stream().map(i -> i.address).collect(toList());
    }

    public List<String> getNaturalEndpointsWithPort(String keyspaceName, ByteBuffer key)
    {
        return stringify(getNaturalEndpoints(keyspaceName, tokenMetadata.partitioner.getToken(key)), true);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddressAndPort> getNaturalEndpoints(String keyspaceName, RingPosition pos)
    {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getNaturalEndpoints(pos);
    }

    /**
     * Returns the endpoints currently responsible for storing the token plus pending ones
     */
    public Iterable<InetAddressAndPort> getNaturalAndPendingEndpoints(String keyspaceName, Token token)
    {
        return Iterables.concat(getNaturalEndpoints(keyspaceName, token), tokenMetadata.pendingEndpointsFor(token, keyspaceName));
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspace keyspace name also known as keyspace
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddressAndPort> getLiveNaturalEndpoints(Keyspace keyspace, ByteBuffer key)
    {
        return getLiveNaturalEndpoints(keyspace, tokenMetadata.decorateKey(key));
    }

    public List<InetAddressAndPort> getLiveNaturalEndpoints(Keyspace keyspace, RingPosition pos)
    {
        List<InetAddressAndPort> liveEps = new ArrayList<>();
        getLiveNaturalEndpoints(keyspace, pos, liveEps);
        return liveEps;
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspace keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @param liveEps the list of endpoints to mutate
     */
    public void getLiveNaturalEndpoints(Keyspace keyspace, RingPosition pos, List<InetAddressAndPort> liveEps)
    {
        List<InetAddressAndPort> endpoints = keyspace.getReplicationStrategy().getNaturalEndpoints(pos);

        for (InetAddressAndPort endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }
    }

    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception
    {
        ch.qos.logback.classic.Logger logBackLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(classQualifier);

        // if both classQualifer and rawLevel are empty, reload from configuration
        if (StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            JMXConfiguratorMBean jmxConfiguratorMBean = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                    new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"),
                    JMXConfiguratorMBean.class);
            jmxConfiguratorMBean.reloadDefaultConfiguration();
            return;
        }
        // classQualifer is set, but blank level given
        else if (StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            if (logBackLogger.getLevel() != null || hasAppenders(logBackLogger))
                logBackLogger.setLevel(null);
            return;
        }

        ch.qos.logback.classic.Level level = ch.qos.logback.classic.Level.toLevel(rawLevel);
        logBackLogger.setLevel(level);
        logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", level, classQualifier, rawLevel, rawLevel);
    }

    /**
     * @return the runtime logging levels for all the configured loggers
     */
    @Override
    public Map<String,String>getLoggingLevels()
    {
        Map<String, String> logLevelMaps = Maps.newLinkedHashMap();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (ch.qos.logback.classic.Logger logger : lc.getLoggerList())
        {
            if(logger.getLevel() != null || hasAppenders(logger))
                logLevelMaps.put(logger.getName(), logger.getLevel().toString());
        }
        return logLevelMaps;
    }

    private boolean hasAppenders(ch.qos.logback.classic.Logger logger)
    {
        Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
        return it.hasNext();
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

    /**
     * Broadcast leaving status and update local tokenMetadata accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.leaving(getLocalTokens()));
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.leaving(getLocalTokens()));
        tokenMetadata.addLeavingEndpoint(FBUtilities.getBroadcastAddressAndPort());
        PendingRangeCalculatorService.instance.update();
    }

    public void decommission(boolean force) throws InterruptedException
    {
        TokenMetadata metadata = tokenMetadata.cloneAfterAllLeft();
        if (operationMode != Mode.LEAVING)
        {
            if (!tokenMetadata.isMember(FBUtilities.getBroadcastAddressAndPort()))
                throw new UnsupportedOperationException("local node is not a member of the token ring yet");
            if (metadata.getAllEndpoints().size() < 2)
                    throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
            if (operationMode != Mode.NORMAL)
                throw new UnsupportedOperationException("Node in " + operationMode + " state; wait for status to become normal or restart");
        }
        if (!isDecommissioning.compareAndSet(false, true))
            throw new IllegalStateException("Node is still decommissioning. Check nodetool netstats.");

        if (logger.isDebugEnabled())
            logger.debug("DECOMMISSIONING");

        try
        {
            PendingRangeCalculatorService.instance.blockUntilFinished();

            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());

            if (operationMode != Mode.LEAVING) // If we're already decommissioning there is no point checking RF/pending ranges
            {
                int rf, numNodes;
                for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
                {
                    if (!force)
                    {
                        Keyspace keyspace = Keyspace.open(keyspaceName);
                        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                        {
                            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
                            rf = strategy.getReplicationFactor(dc);
                            numNodes = metadata.getTopology().getDatacenterEndpoints().get(dc).size();
                        }
                        else
                        {
                            numNodes = metadata.getAllEndpoints().size();
                            rf = keyspace.getReplicationStrategy().getReplicationFactor();
                        }

                        if (numNodes <= rf)
                            throw new UnsupportedOperationException("Not enough live nodes to maintain replication factor in keyspace "
                                                                    + keyspaceName + " (RF = " + rf + ", N = " + numNodes + ")."
                                                                    + " Perform a forceful decommission to ignore.");
                    }
                    if (tokenMetadata.getPendingRanges(keyspaceName, FBUtilities.getBroadcastAddressAndPort()).size() > 0)
                        throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
                }
            }

            startLeaving();
            long timeout = Math.max(RING_DELAY, BatchlogManager.instance.getBatchlogTimeout());
            setMode(Mode.LEAVING, "sleeping " + timeout + " ms for batch processing and pending range setup", true);
            Thread.sleep(timeout);

            Runnable finishLeaving = new Runnable()
            {
                public void run()
                {
                    shutdownClientServers();
                    Gossiper.instance.stop();
                    try
                    {
                        MessagingService.instance().shutdown();
                    }
                    catch (IOError ioe)
                    {
                        logger.info("failed to shutdown message service: {}", ioe);
                    }
                    StageManager.shutdownNow();
                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.DECOMMISSIONED);
                    setMode(Mode.DECOMMISSIONED, true);
                    // let op be responsible for killing the process
                }
            };
            unbootstrap(finishLeaving);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Node interrupted while decommissioning");
        }
        catch (ExecutionException e)
        {
            logger.error("Error while decommissioning node ", e.getCause());
            throw new RuntimeException("Error while decommissioning node: " + e.getCause().getMessage());
        }
        finally
        {
            isDecommissioning.set(false);
        }
    }

    private void leaveRing()
    {
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP);
        tokenMetadata.removeEndpoint(FBUtilities.getBroadcastAddressAndPort());
        PendingRangeCalculatorService.instance.update();

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.left(getLocalTokens(),Gossiper.computeExpireTime()));
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.left(getLocalTokens(),Gossiper.computeExpireTime()));
        int delay = Math.max(RING_DELAY, Gossiper.intervalInMillis * 2);
        logger.info("Announcing that I have left the ring for {}ms", delay);
        Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
    }

    private void unbootstrap(Runnable onFinish) throws ExecutionException, InterruptedException
    {
        Map<String, Multimap<Range<Token>, InetAddressAndPort>> rangesToStream = new HashMap<>();

        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
        {
            Multimap<Range<Token>, InetAddressAndPort> rangesMM = getChangedRangesForLeaving(keyspaceName, FBUtilities.getBroadcastAddressAndPort());

            if (logger.isDebugEnabled())
                logger.debug("Ranges needing transfer are [{}]", StringUtils.join(rangesMM.keySet(), ","));

            rangesToStream.put(keyspaceName, rangesMM);
        }

        setMode(Mode.LEAVING, "replaying batch log and streaming data to other nodes", true);

        // Start with BatchLog replay, which may create hints but no writes since this is no longer a valid endpoint.
        Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
        Future<StreamState> streamSuccess = streamRanges(rangesToStream);

        // Wait for batch log to complete before streaming hints.
        logger.debug("waiting for batch log processing.");
        batchlogReplay.get();

        setMode(Mode.LEAVING, "streaming hints to other nodes", true);

        Future hintsSuccess = streamHints();

        // wait for the transfer runnables to signal the latch.
        logger.debug("waiting for stream acks.");
        streamSuccess.get();
        hintsSuccess.get();
        logger.debug("stream acks all received.");
        leaveRing();
        onFinish.run();
    }

    private Future streamHints()
    {
        return HintsService.instance.transferHints(this::getPreferredHintsStreamTarget);
    }

    /**
     * Find the best target to stream hints to. Currently the closest peer according to the snitch
     */
    private UUID getPreferredHintsStreamTarget()
    {
        List<InetAddressAndPort> candidates = new ArrayList<>(StorageService.instance.getTokenMetadata().cloneAfterAllLeft().getAllEndpoints());
        candidates.remove(FBUtilities.getBroadcastAddressAndPort());
        for (Iterator<InetAddressAndPort> iter = candidates.iterator(); iter.hasNext(); )
        {
            InetAddressAndPort address = iter.next();
            if (!FailureDetector.instance.isAlive(address))
                iter.remove();
        }

        if (candidates.isEmpty())
        {
            logger.warn("Unable to stream hints since no live endpoints seen");
            throw new RuntimeException("Unable to stream hints since no live endpoints seen");
        }
        else
        {
            // stream to the closest peer as chosen by the snitch
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddressAndPort(), candidates);
            InetAddressAndPort hintsDestinationHost = candidates.get(0);
            return tokenMetadata.getHostId(hintsDestinationHost);
        }
    }

    public void move(String newToken) throws IOException
    {
        try
        {
            getTokenFactory().validate(newToken);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e.getMessage());
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
    private void move(Token newToken) throws IOException
    {
        if (newToken == null)
            throw new IOException("Can't move to the undefined (null) token.");

        if (tokenMetadata.sortedTokens().contains(newToken))
            throw new IOException("target token " + newToken + " is already owned by another node.");

        // address of the current node
        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();

        // This doesn't make any sense in a vnodes environment.
        if (getTokenMetadata().getTokens(localAddress).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }

        List<String> keyspacesToProcess = Schema.instance.getNonLocalStrategyKeyspaces();

        PendingRangeCalculatorService.instance.blockUntilFinished();
        // checking if data is moving to this node
        for (String keyspaceName : keyspacesToProcess)
        {
            if (tokenMetadata.getPendingRanges(keyspaceName, localAddress).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.moving(newToken));
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.moving(newToken));
        setMode(Mode.MOVING, String.format("Moving %s from %s to %s.", localAddress, getLocalTokens().iterator().next(), newToken), true);

        setMode(Mode.MOVING, String.format("Sleeping %s ms before start streaming/fetching ranges", RING_DELAY), true);
        Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS);

        RangeRelocator relocator = new RangeRelocator(Collections.singleton(newToken), keyspacesToProcess);

        if (relocator.streamsNeeded())
        {
            setMode(Mode.MOVING, "fetching new ranges and streaming old ranges", true);
            try
            {
                relocator.stream().get();
            }
            catch (ExecutionException | InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for stream/fetch ranges to finish: " + e.getMessage());
            }
        }
        else
        {
            setMode(Mode.MOVING, "No ranges to fetch/stream", true);
        }

        setTokens(Collections.singleton(newToken)); // setting new token as we have everything settled

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", getLocalTokens().iterator().next());
    }

    private class RangeRelocator
    {
        private final StreamPlan streamPlan = new StreamPlan(StreamOperation.RELOCATION);

        private RangeRelocator(Collection<Token> tokens, List<String> keyspaceNames)
        {
            calculateToFromStreams(tokens, keyspaceNames);
        }

        private void calculateToFromStreams(Collection<Token> newTokens, List<String> keyspaceNames)
        {
            InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            TokenMetadata tokenMetaCloneAllSettled = tokenMetadata.cloneAfterAllSettled();
            // clone to avoid concurrent modification in calculateNaturalEndpoints
            TokenMetadata tokenMetaClone = tokenMetadata.cloneOnlyTokenMap();

            for (String keyspace : keyspaceNames)
            {
                // replication strategy of the current keyspace
                AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
                Multimap<InetAddressAndPort, Range<Token>> endpointToRanges = strategy.getAddressRanges();

                logger.debug("Calculating ranges to stream and request for keyspace {}", keyspace);
                for (Token newToken : newTokens)
                {
                    // getting collection of the currently used ranges by this keyspace
                    Collection<Range<Token>> currentRanges = endpointToRanges.get(localAddress);
                    // collection of ranges which this node will serve after move to the new token
                    Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);

                    // ring ranges and endpoints associated with them
                    // this used to determine what nodes should we ping about range data
                    Multimap<Range<Token>, InetAddressAndPort> rangeAddresses = strategy.getRangeAddresses(tokenMetaClone);

                    // calculated parts of the ranges to request/stream from/to nodes in the ring
                    Pair<Set<Range<Token>>, Set<Range<Token>>> rangesPerKeyspace = calculateStreamAndFetchRanges(currentRanges, updatedRanges);

                    /**
                     * In this loop we are going through all ranges "to fetch" and determining
                     * nodes in the ring responsible for data we are interested in
                     */
                    Multimap<Range<Token>, InetAddressAndPort> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
                    for (Range<Token> toFetch : rangesPerKeyspace.right)
                    {
                        for (Range<Token> range : rangeAddresses.keySet())
                        {
                            if (range.contains(toFetch))
                            {
                                List<InetAddressAndPort> endpoints = null;

                                if (useStrictConsistency)
                                {
                                    Set<InetAddressAndPort> oldEndpoints = Sets.newHashSet(rangeAddresses.get(range));
                                    Set<InetAddressAndPort> newEndpoints = Sets.newHashSet(strategy.calculateNaturalEndpoints(toFetch.right, tokenMetaCloneAllSettled));

                                    //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                                    //So we need to be careful to only be strict when endpoints == RF
                                    if (oldEndpoints.size() == strategy.getReplicationFactor())
                                    {
                                        oldEndpoints.removeAll(newEndpoints);

                                        //No relocation required
                                        if (oldEndpoints.isEmpty())
                                            continue;

                                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                                    }

                                    endpoints = Lists.newArrayList(oldEndpoints.iterator().next());
                                }
                                else
                                {
                                    endpoints = snitch.getSortedListByProximity(localAddress, rangeAddresses.get(range));
                                }

                                // storing range and preferred endpoint set
                                rangesToFetchWithPreferredEndpoints.putAll(toFetch, endpoints);
                            }
                        }

                        Collection<InetAddressAndPort> addressList = rangesToFetchWithPreferredEndpoints.get(toFetch);
                        if (addressList == null || addressList.isEmpty())
                            continue;

                        if (useStrictConsistency)
                        {
                            if (addressList.size() > 1)
                                throw new IllegalStateException("Multiple strict sources found for " + toFetch);

                            InetAddressAndPort sourceIp = addressList.iterator().next();
                            if (Gossiper.instance.isEnabled() && !Gossiper.instance.getEndpointStateForEndpoint(sourceIp).isAlive())
                                throw new RuntimeException("A node required to move the data consistently is down ("+sourceIp+").  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
                        }
                    }

                    // calculating endpoints to stream current ranges to if needed
                    // in some situations node will handle current ranges as part of the new ranges
                    Multimap<InetAddressAndPort, Range<Token>> endpointRanges = HashMultimap.create();
                    for (Range<Token> toStream : rangesPerKeyspace.left)
                    {
                        Set<InetAddressAndPort> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaClone));
                        Set<InetAddressAndPort> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaCloneAllSettled));
                        logger.debug("Range: {} Current endpoints: {} New endpoints: {}", toStream, currentEndpoints, newEndpoints);
                        for (InetAddressAndPort address : Sets.difference(newEndpoints, currentEndpoints))
                        {
                            logger.debug("Range {} has new owner {}", toStream, address);
                            endpointRanges.put(address, toStream);
                        }
                    }

                    // stream ranges
                    for (InetAddressAndPort address : endpointRanges.keySet())
                    {
                        logger.debug("Will stream range {} of keyspace {} to endpoint {}", endpointRanges.get(address), keyspace, address);
                        InetAddressAndPort preferred = SystemKeyspace.getPreferredIP(address);
                        streamPlan.transferRanges(address, preferred, keyspace, endpointRanges.get(address));
                    }

                    // stream requests
                    Multimap<InetAddressAndPort, Range<Token>> workMap = RangeStreamer.getWorkMap(rangesToFetchWithPreferredEndpoints, keyspace, FailureDetector.instance, useStrictConsistency);
                    for (InetAddressAndPort address : workMap.keySet())
                    {
                        logger.debug("Will request range {} of keyspace {} from endpoint {}", workMap.get(address), keyspace, address);
                        InetAddressAndPort preferred = SystemKeyspace.getPreferredIP(address);
                        streamPlan.requestRanges(address, preferred, keyspace, workMap.get(address));
                    }

                    logger.debug("Keyspace {}: work map {}.", keyspace, workMap);
                }
            }
        }

        public Future<StreamState> stream()
        {
            return streamPlan.execute();
        }

        public boolean streamsNeeded()
        {
            return !streamPlan.isEmpty();
        }
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
        if (removingNode == null)
        {
            return "No token removals in process.";
        }

        Collection toFormat = replicatingNodes;
        if (!withPort)
        {
            toFormat = new ArrayList(replicatingNodes.size());
            for (InetAddressAndPort node : replicatingNodes)
            {
                toFormat.add(node.toString(false));
            }
        }

        return String.format("Removing token (%s). Waiting for replication confirmation from [%s].",
                             tokenMetadata.getToken(removingNode),
                             StringUtils.join(toFormat, ","));
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeNode() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
        if (!replicatingNodes.isEmpty()  || tokenMetadata.getSizeOfLeavingEndpoints() > 0)
        {
            logger.warn("Removal not confirmed for for {}", StringUtils.join(this.replicatingNodes, ","));
            for (InetAddressAndPort endpoint : tokenMetadata.getLeavingEndpoints())
            {
                UUID hostId = tokenMetadata.getHostId(endpoint);
                Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
                excise(tokenMetadata.getTokens(endpoint), endpoint);
            }
            replicatingNodes.clear();
            removingNode = null;
        }
        else
        {
            logger.warn("No nodes to force removal on, call 'removenode' first");
        }
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
        InetAddressAndPort myAddress = FBUtilities.getBroadcastAddressAndPort();
        UUID localHostId = tokenMetadata.getHostId(myAddress);
        UUID hostId = UUID.fromString(hostIdString);
        InetAddressAndPort endpoint = tokenMetadata.getEndpointForHostId(hostId);

        if (endpoint == null)
            throw new UnsupportedOperationException("Host ID not found.");

        if (!tokenMetadata.isMember(endpoint))
            throw new UnsupportedOperationException("Node to be removed is not a member of the token ring");

        if (endpoint.equals(myAddress))
             throw new UnsupportedOperationException("Cannot remove self");

        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");

        // A leaving endpoint that is dead is already being removed.
        if (tokenMetadata.isLeaving(endpoint))
            logger.warn("Node {} is already being removed, continuing removal anyway", endpoint);

        if (!replicatingNodes.isEmpty())
            throw new UnsupportedOperationException("This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");

        Collection<Token> tokens = tokenMetadata.getTokens(endpoint);

        // Find the endpoints that are going to become responsible for data
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
        {
            // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
            if (Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor() == 1)
                continue;

            // get all ranges that change ownership (that is, a node needs
            // to take responsibility for new range)
            Multimap<Range<Token>, InetAddressAndPort> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
            IFailureDetector failureDetector = FailureDetector.instance;
            for (InetAddressAndPort ep : changedRanges.values())
            {
                if (failureDetector.isAlive(ep))
                    replicatingNodes.add(ep);
                else
                    logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
            }
        }
        removingNode = endpoint;

        tokenMetadata.addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();

        // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
        // we add our own token so other nodes to let us know when they're done
        Gossiper.instance.advertiseRemoving(endpoint, hostId, localHostId);

        // kick off streaming commands
        restoreReplicaCount(endpoint, myAddress);

        // wait for ReplicationFinishedVerbHandler to signal we're done
        while (!replicatingNodes.isEmpty())
        {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }

        excise(tokens, endpoint);

        // gossiper will indicate the token has left
        Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);

        replicatingNodes.clear();
        removingNode = null;
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

    public String getOperationMode()
    {
        return operationMode.toString();
    }

    public boolean isStarting()
    {
        return operationMode == Mode.STARTING;
    }

    public boolean isMoving()
    {
        return operationMode == Mode.MOVING;
    }

    public boolean isJoining()
    {
        return operationMode == Mode.JOINING;
    }

    public boolean isDrained()
    {
        return operationMode == Mode.DRAINED;
    }

    public boolean isDraining()
    {
        return operationMode == Mode.DRAINING;
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
        ExecutorService counterMutationStage = StageManager.getStage(Stage.COUNTER_MUTATION);
        ExecutorService viewMutationStage = StageManager.getStage(Stage.VIEW_MUTATION);
        ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);

        if (mutationStage.isTerminated()
            && counterMutationStage.isTerminated()
            && viewMutationStage.isTerminated())
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
            setMode(Mode.DRAINING, "starting drain process", !isFinalShutdown);

            BatchlogManager.instance.shutdown();
            HintsService.instance.pauseDispatch();

            if (daemon != null)
                shutdownClientServers();
            ScheduledExecutors.optionalTasks.shutdown();
            Gossiper.instance.stop();

            if (!isFinalShutdown)
                setMode(Mode.DRAINING, "shutting down MessageService", false);

            // In-progress writes originating here could generate hints to be written, so shut down MessagingService
            // before mutation stage, so we can get all the hints saved before shutting down
            MessagingService.instance().shutdown();

            if (!isFinalShutdown)
                setMode(Mode.DRAINING, "clearing mutation stage", false);
            viewMutationStage.shutdown();
            counterMutationStage.shutdown();
            mutationStage.shutdown();
            viewMutationStage.awaitTermination(3600, TimeUnit.SECONDS);
            counterMutationStage.awaitTermination(3600, TimeUnit.SECONDS);
            mutationStage.awaitTermination(3600, TimeUnit.SECONDS);

            StorageProxy.instance.verifyNoHintsInProgress();

            if (!isFinalShutdown)
                setMode(Mode.DRAINING, "flushing column families", false);

            // disable autocompaction - we don't want to start any new compactions while we are draining
            for (Keyspace keyspace : Keyspace.all())
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    cfs.disableAutoCompaction();

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
                    flushes.add(cfs.forceFlush());
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
                    flushes.add(cfs.forceFlush());
            }
            FBUtilities.waitOnFutures(flushes);

            HintsService.instance.shutdownBlocking();

            // Interrupt ongoing compactions and shutdown CM to prevent further compactions.
            CompactionManager.instance.forceShutdown();

            // whilst we've flushed all the CFs, which will have recycled all completed segments, we want to ensure
            // there are no segments to replay, so we force the recycling of any remaining (should be at most one)
            CommitLog.instance.forceRecycleAllSegments();

            CommitLog.instance.shutdownBlocking();

            // wait for miscellaneous tasks like sstable and commitlog segment deletion
            ScheduledExecutors.nonPeriodicTasks.shutdown();
            if (!ScheduledExecutors.nonPeriodicTasks.awaitTermination(1, TimeUnit.MINUTES))
                logger.warn("Failed to wait for non periodic tasks to shutdown");

            ColumnFamilyStore.shutdownPostFlushExecutor();
            setMode(Mode.DRAINED, !isFinalShutdown);
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
    }

    // Never ever do this at home. Used by tests.
    @VisibleForTesting
    public IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = DatabaseDescriptor.setPartitionerUnsafe(newPartitioner);
        tokenMetadata = tokenMetadata.cloneWithNewPartitioner(newPartitioner);
        valueFactory = new VersionedValue.VersionedValueFactory(newPartitioner);
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd)
    {
        TokenMetadata old = tokenMetadata;
        tokenMetadata = tmd;
        return old;
    }

    public void truncate(String keyspace, String table) throws TimeoutException, IOException
    {
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
        List<Token> sortedTokens = tokenMetadata.sortedTokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<Token, Float>(tokenMetadata.partitioner.describeOwnership(sortedTokens));
        Map<InetAddress, Float> nodeMap = new LinkedHashMap<>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet())
        {
            InetAddressAndPort endpoint = tokenMetadata.getEndpoint(entry.getKey());
            Float tokenOwnership = entry.getValue();
            if (nodeMap.containsKey(endpoint.address))
                nodeMap.put(endpoint.address, nodeMap.get(endpoint.address) + tokenOwnership);
            else
                nodeMap.put(endpoint.address, tokenOwnership);
        }
        return nodeMap;
    }

    public Map<String, Float> getOwnershipWithPort()
    {
        List<Token> sortedTokens = tokenMetadata.sortedTokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<Token, Float>(tokenMetadata.partitioner.describeOwnership(sortedTokens));
        Map<String, Float> nodeMap = new LinkedHashMap<>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet())
        {
            InetAddressAndPort endpoint = tokenMetadata.getEndpoint(entry.getKey());
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
        AbstractReplicationStrategy strategy;
        if (keyspace != null)
        {
            Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
            if (keyspaceInstance == null)
                throw new IllegalArgumentException("The keyspace " + keyspace + ", does not exist");

            if (keyspaceInstance.getReplicationStrategy() instanceof LocalStrategy)
                throw new IllegalStateException("Ownership values for keyspaces with LocalStrategy are meaningless");
            strategy = keyspaceInstance.getReplicationStrategy();
        }
        else
        {
            List<String> userKeyspaces = Schema.instance.getUserKeyspaces();

            if (userKeyspaces.size() > 0)
            {
                keyspace = userKeyspaces.get(0);
                AbstractReplicationStrategy replicationStrategy = Schema.instance.getKeyspaceInstance(keyspace).getReplicationStrategy();
                for (String keyspaceName : userKeyspaces)
                {
                    if (!Schema.instance.getKeyspaceInstance(keyspaceName).getReplicationStrategy().hasSameSettings(replicationStrategy))
                        throw new IllegalStateException("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
                }
            }
            else
            {
                keyspace = "system_traces";
            }

            Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
            if (keyspaceInstance == null)
                throw new IllegalArgumentException("The node does not have " + keyspace + " yet, probably still bootstrapping");
            strategy = keyspaceInstance.getReplicationStrategy();
        }

        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();

        Collection<Collection<InetAddressAndPort>> endpointsGroupedByDc = new ArrayList<>();
        // mapping of dc's to nodes, use sorted map so that we get dcs sorted
        SortedMap<String, Collection<InetAddressAndPort>> sortedDcsToEndpoints = new TreeMap<>();
        sortedDcsToEndpoints.putAll(metadata.getTopology().getDatacenterEndpoints().asMap());
        for (Collection<InetAddressAndPort> endpoints : sortedDcsToEndpoints.values())
            endpointsGroupedByDc.add(endpoints);

        Map<Token, Float> tokenOwnership = tokenMetadata.partitioner.describeOwnership(tokenMetadata.sortedTokens());
        LinkedHashMap<InetAddressAndPort, Float> finalOwnership = Maps.newLinkedHashMap();

        Multimap<InetAddressAndPort, Range<Token>> endpointToRanges = strategy.getAddressRanges();
        // calculate ownership per dc
        for (Collection<InetAddressAndPort> endpoints : endpointsGroupedByDc)
        {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (InetAddressAndPort endpoint : endpoints)
            {
                float ownership = 0.0f;
                for (Range<Token> range : endpointToRanges.get(endpoint))
                {
                    if (tokenOwnership.containsKey(range.right))
                        ownership += tokenOwnership.get(range.right);
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
        result.entrySet().stream().forEachOrdered(entry -> asInets.put(entry.getKey().address, entry.getValue()));
        return asInets;
    }

    public LinkedHashMap<String, Float> effectiveOwnershipWithPort(String keyspace) throws IllegalStateException
    {
        LinkedHashMap<InetAddressAndPort, Float> result = getEffectiveOwnership(keyspace);
        LinkedHashMap<String, Float> asStrings = new LinkedHashMap<>();
        result.entrySet().stream().forEachOrdered(entry -> asStrings.put(entry.getKey().toString(), entry.getValue()));
        return asStrings;
    }

    public List<String> getKeyspaces()
    {
        List<String> keyspaceNamesList = new ArrayList<>(Schema.instance.getKeyspaces());
        return Collections.unmodifiableList(keyspaceNamesList);
    }

    public List<String> getNonSystemKeyspaces()
    {
        return Schema.instance.getNonSystemKeyspaces();
    }

    public List<String> getNonLocalStrategyKeyspaces()
    {
        return Collections.unmodifiableList(Schema.instance.getNonLocalStrategyKeyspaces());
    }

    public Map<String, String> getViewBuildStatuses(String keyspace, String view, boolean withPort)
    {
        Map<UUID, String> coreViewStatus = SystemDistributedKeyspace.viewStatus(keyspace, view);
        Map<InetAddressAndPort, UUID> hostIdToEndpoint = tokenMetadata.getEndpointToHostIdMapForReading();
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<InetAddressAndPort, UUID> entry : hostIdToEndpoint.entrySet())
        {
            UUID hostId = entry.getValue();
            InetAddressAndPort endpoint = entry.getKey();
            result.put(endpoint.toString(withPort),
                       coreViewStatus.containsKey(hostId)
                       ? coreViewStatus.get(hostId)
                       : "UNKNOWN");
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
            for (String ks : Schema.instance.getKeyspaces())
            {
                Keyspace.open(ks).getReplicationStrategy().snitch = newSnitch;
            }
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

        updateTopology();
    }

    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    private Future<StreamState> streamRanges(Map<String, Multimap<Range<Token>, InetAddressAndPort>> rangesToStreamByKeyspace)
    {
        // First, we build a list of ranges to stream to each host, per table
        Map<String, Map<InetAddressAndPort, List<Range<Token>>>> sessionsToStreamByKeyspace = new HashMap<>();

        for (Map.Entry<String, Multimap<Range<Token>, InetAddressAndPort>> entry : rangesToStreamByKeyspace.entrySet())
        {
            String keyspace = entry.getKey();
            Multimap<Range<Token>, InetAddressAndPort> rangesWithEndpoints = entry.getValue();

            if (rangesWithEndpoints.isEmpty())
                continue;

            Map<InetAddressAndPort, Set<Range<Token>>> transferredRangePerKeyspace = SystemKeyspace.getTransferredRanges("Unbootstrap",
                                                                                                                         keyspace,
                                                                                                                         StorageService.instance.getTokenMetadata().partitioner);
            Map<InetAddressAndPort, List<Range<Token>>> rangesPerEndpoint = new HashMap<>();
            for (Map.Entry<Range<Token>, InetAddressAndPort> endPointEntry : rangesWithEndpoints.entries())
            {
                Range<Token> range = endPointEntry.getKey();
                InetAddressAndPort endpoint = endPointEntry.getValue();

                Set<Range<Token>> transferredRanges = transferredRangePerKeyspace.get(endpoint);
                if (transferredRanges != null && transferredRanges.contains(range))
                {
                    logger.debug("Skipping transferred range {} of keyspace {}, endpoint {}", range, keyspace, endpoint);
                    continue;
                }

                List<Range<Token>> curRanges = rangesPerEndpoint.get(endpoint);
                if (curRanges == null)
                {
                    curRanges = new LinkedList<>();
                    rangesPerEndpoint.put(endpoint, curRanges);
                }
                curRanges.add(range);
            }

            sessionsToStreamByKeyspace.put(keyspace, rangesPerEndpoint);
        }

        StreamPlan streamPlan = new StreamPlan(StreamOperation.DECOMMISSION);

        // Vinculate StreamStateStore to current StreamPlan to update transferred ranges per StreamSession
        streamPlan.listeners(streamStateStore);

        for (Map.Entry<String, Map<InetAddressAndPort, List<Range<Token>>>> entry : sessionsToStreamByKeyspace.entrySet())
        {
            String keyspaceName = entry.getKey();
            Map<InetAddressAndPort, List<Range<Token>>> rangesPerEndpoint = entry.getValue();

            for (Map.Entry<InetAddressAndPort, List<Range<Token>>> rangesEntry : rangesPerEndpoint.entrySet())
            {
                List<Range<Token>> ranges = rangesEntry.getValue();
                InetAddressAndPort newEndpoint = rangesEntry.getKey();
                InetAddressAndPort preferred = SystemKeyspace.getPreferredIP(newEndpoint);

                // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                streamPlan.transferRanges(newEndpoint, preferred, keyspaceName, ranges);
            }
        }
        return streamPlan.execute();
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for keyspace and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    public Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(Collection<Range<Token>> current, Collection<Range<Token>> updated)
    {
        Set<Range<Token>> toStream = new HashSet<>();
        Set<Range<Token>> toFetch  = new HashSet<>();


        for (Range<Token> r1 : current)
        {
            boolean intersect = false;
            for (Range<Token> r2 : updated)
            {
                if (r1.intersects(r2))
                {
                    // adding difference ranges to fetch from a ring
                    toStream.addAll(r1.subtract(r2));
                    intersect = true;
                }
            }
            if (!intersect)
            {
                toStream.add(r1); // should seed whole old range
            }
        }

        for (Range<Token> r2 : updated)
        {
            boolean intersect = false;
            for (Range<Token> r1 : current)
            {
                if (r2.intersects(r1))
                {
                    // adding difference ranges to fetch from a ring
                    toFetch.addAll(r2.subtract(r1));
                    intersect = true;
                }
            }
            if (!intersect)
            {
                toFetch.add(r2); // should fetch whole old range
            }
        }

        return Pair.create(toStream, toFetch);
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
                    for (Map.Entry<Range<Token>, List<InetAddressAndPort>> entry : StorageService.instance.getRangeToAddressMap(keyspace).entrySet())
                    {
                        Range<Token> range = entry.getKey();
                        for (InetAddressAndPort endpoint : entry.getValue())
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
    public void loadNewSSTables(String ksName, String cfName)
    {
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
            for (Range<Token> range : getPrimaryRangesForEndpoint(keyspace.getName(), FBUtilities.getBroadcastAddressAndPort()))
                keys.addAll(keySamples(keyspace.getColumnFamilyStores(), range));
        }

        List<String> sampledKeys = new ArrayList<>(keys.size());
        for (DecoratedKey key : keys)
            sampledKeys.add(key.getToken().toString());
        return sampledKeys;
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
        MigrationManager.resetLocalSchema();
    }

    public void reloadLocalSchema()
    {
        Schema.instance.reloadSchemaAndAnnounceVersion();
    }

    public void setTraceProbability(double probability)
    {
        this.traceProbability = probability;
    }

    public double getTraceProbability()
    {
        return traceProbability;
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

    public int getTombstoneWarnThreshold()
    {
        return DatabaseDescriptor.getTombstoneWarnThreshold();
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        DatabaseDescriptor.setTombstoneWarnThreshold(threshold);
    }

    public int getTombstoneFailureThreshold()
    {
        return DatabaseDescriptor.getTombstoneFailureThreshold();
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(threshold);
    }

    public int getBatchSizeFailureThreshold()
    {
        return DatabaseDescriptor.getBatchSizeFailThresholdInKB();
    }

    public void setBatchSizeFailureThreshold(int threshold)
    {
        DatabaseDescriptor.setBatchSizeFailThresholdInKB(threshold);
        logger.info("Updated batch_size_fail_threshold_in_kb to {}", threshold);
    }

    public int getBatchSizeWarnThreshold()
    {
        return DatabaseDescriptor.getBatchSizeWarnThresholdInKB();
    }

    public void setBatchSizeWarnThreshold(int threshold)
    {
        DatabaseDescriptor.setBatchSizeWarnThresholdInKB(threshold);
        logger.info("Updated batch_size_warn_threshold_in_kb to {}", threshold);
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        DatabaseDescriptor.setHintedHandoffThrottleInKB(throttleInKB);
        logger.info("Updated hinted_handoff_throttle_in_kb to {}", throttleInKB);
    }
}
