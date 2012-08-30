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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.*;
import org.apache.log4j.Level;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.service.AntiEntropyService.RepairFuture;
import org.apache.cassandra.service.AntiEntropyService.TreeRequestVerbHandler;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService implements IEndpointStateChangeSubscriber, StorageServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    public static final int RING_DELAY = getRingDelay(); // delay after which we assume ring has stablized

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

    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
     public static final DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledTasks");

    /**
     * This pool is used by tasks that can have longer execution times, and usually are non periodic.
     */
    public static final DebuggableScheduledThreadPoolExecutor tasks = new DebuggableScheduledThreadPoolExecutor("NonPeriodicTasks");
/**
     * tasks that do not need to be waited for on shutdown/drain
     */
    public static final DebuggableScheduledThreadPoolExecutor optionalTasks = new DebuggableScheduledThreadPoolExecutor("OptionalTasks");
    static
    {
        tasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /* This abstraction maintains the token/endpoint metadata information */
    private TokenMetadata tokenMetadata = new TokenMetadata();

    public VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(getPartitioner());

    public static final StorageService instance = new StorageService();

    private static final StorageMetrics metrics = new StorageMetrics();

    public static IPartitioner getPartitioner()
    {
        return DatabaseDescriptor.getPartitioner();
    }

    public Collection<Range<Token>> getLocalRanges(String table)
    {
        return getRangesForEndpoint(table, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getLocalPrimaryRanges()
    {
        return getPrimaryRangesForEndpoint(FBUtilities.getBroadcastAddress());
    }

    @Deprecated
    public Range<Token> getLocalPrimaryRange()
    {
        return getPrimaryRangeForEndpoint(FBUtilities.getBroadcastAddress());
    }

    // For JMX's sake. Use getLocalPrimaryRange for internal uses
    public List<String> getPrimaryRange()
    {
        return getLocalPrimaryRange().asList();
    }

    private final Set<InetAddress> replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
    private CassandraDaemon daemon;

    private InetAddress removingNode;

    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;

    /* we bootstrap but do NOT join the ring unless told to do so */
    private boolean isSurveyMode= Boolean.parseBoolean(System.getProperty("cassandra.write_survey", "false"));

    /* when intialized as a client, we shouldn't write to the system table. */
    private boolean isClientMode;
    private boolean initialized;
    private volatile boolean joined = false;

    /* the probability for tracing any particular request, 0 disables tracing and 1 enables for all */
    private double tracingProbability = 0.0;

    private static enum Mode { NORMAL, CLIENT, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED }
    private Mode operationMode;

    private final MigrationManager migrationManager = new MigrationManager();

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();

    public void finishBootstrapping()
    {
        isBootstrapMode = false;
    }

    /** This method updates the local token on disk  */
    public void setTokens(Collection<Token> tokens)
    {
        if (logger.isDebugEnabled())
            logger.debug("Setting tokens to {}", tokens);
        SystemTable.updateTokens(tokens);
        tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS,
                                                   valueFactory.normal(getLocalTokens(), SystemTable.getLocalHostId()));
        setMode(Mode.NORMAL, false);
    }

    public StorageService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=StorageService"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        /* register the verb handlers */
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.MUTATION, new RowMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.READ_REPAIR, new ReadRepairVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.READ, new ReadVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.RANGE_SLICE, new RangeSliceVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.INDEX_SCAN, new IndexScanVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.COUNTER_MUTATION, new CounterMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.TRUNCATE, new TruncateVerbHandler());

        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.BOOTSTRAP_TOKEN, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.STREAM_REQUEST, new StreamRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.STREAM_REPLY, new StreamReplyVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.REPLICATION_FINISHED, new ReplicationFinishedVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.REQUEST_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.INTERNAL_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.TREE_REQUEST, new TreeRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.TREE_RESPONSE, new AntiEntropyService.TreeResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.STREAMING_REPAIR_REQUEST, new StreamingRepairTask.StreamingRepairRequest());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.STREAMING_REPAIR_RESPONSE, new StreamingRepairTask.StreamingRepairResponse());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_SHUTDOWN, new GossipShutdownVerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.DEFINITIONS_UPDATE, new DefinitionsUpdateVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.SCHEMA_CHECK, new SchemaCheckVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.MIGRATION_REQUEST, new MigrationRequestVerbHandler());

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.SNAPSHOT, new SnapshotVerbHandler());

        // spin up the streaming service so it is available for jmx tools.
        if (StreamingService.instance == null)
            throw new RuntimeException("Streaming service is unavailable.");
    }

    public void registerDaemon(CassandraDaemon daemon)
    {
        this.daemon = daemon;
    }

    // should only be called via JMX
    public void stopGossiping()
    {
        if (initialized)
        {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            initialized = false;
        }
    }

    // should only be called via JMX
    public void startGossiping()
    {
        if (!initialized)
        {
            logger.warn("Starting gossip by operator request");
            Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));
            initialized = true;
        }
    }

    // should only be called via JMX
    public void startRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        daemon.thriftServer.start();
    }

    public void stopRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        daemon.thriftServer.stop();
    }

    public boolean isRPCServerRunning()
    {
        if (daemon == null)
        {
            return false;
        }
        return daemon.thriftServer.isRunning();
    }

    public void startNativeTransport()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        daemon.nativeServer.start();
    }

    public void stopNativeTransport()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured  daemon");
        }
        daemon.nativeServer.stop();
    }

    public boolean isNativeTransportRunning()
    {
        if (daemon == null)
        {
            return false;
        }
        return daemon.nativeServer.isRunning();
    }

    public void stopClient()
    {
        Gossiper.instance.unregister(migrationManager);
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
        // give it a second so that task accepted before the MessagingService shutdown gets submitted to the stage (to avoid RejectedExecutionException)
        try { Thread.sleep(1000L); } catch (InterruptedException e) {}
        StageManager.shutdownNow();
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    public synchronized void initClient() throws IOException, ConfigurationException
    {
        initClient(RING_DELAY);
    }

    public synchronized void initClient(int delay) throws IOException, ConfigurationException
    {
        if (initialized)
        {
            if (!isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = true;
        logger.info("Starting up client gossip");
        setMode(Mode.CLIENT, false);
        Gossiper.instance.register(this);
        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        MessagingService.instance().listen(FBUtilities.getLocalAddress());

        // sleep a while to allow gossip to warm up (the other nodes need to know about this one before they can reply).
        try
        {
            Thread.sleep(delay);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        Schema.instance.updateVersionAndAnnounce();
    }

    public synchronized void initServer() throws IOException, ConfigurationException
    {
        initServer(RING_DELAY);
    }

    public synchronized void initServer(int delay) throws IOException, ConfigurationException
    {
        logger.info("Cassandra version: " + FBUtilities.getReleaseVersionString());
        logger.info("Thrift API version: " + Constants.VERSION);
        logger.info("CQL supported versions: " + StringUtils.join(ClientState.getCQLSupportedVersion(), ",") + " (default: " + ClientState.DEFAULT_CQL_VERSION + ")");

        if (initialized)
        {
            if (isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = false;

        // Ensure StorageProxy is initialized on start-up; see CASSANDRA-3797.
        try
        {
            Class.forName("org.apache.cassandra.service.StorageProxy");
        }
        catch (ClassNotFoundException e)
        {
            throw new AssertionError(e);
        }

        if (Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true")))
        {
            logger.info("Loading persisted ring state");
            Multimap<InetAddress, Token> loadedTokens = SystemTable.loadTokens();
            for (InetAddress ep : loadedTokens.keySet())
            {
                if (ep.equals(FBUtilities.getBroadcastAddress()))
                {
                    // entry has been mistakenly added, delete it
                    SystemTable.removeTokens(loadedTokens.get(ep));
                }
                else
                {
                    tokenMetadata.updateNormalTokens(loadedTokens.get(ep), ep);
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }

        if (Boolean.parseBoolean(System.getProperty("cassandra.renew_counter_id", "false")))
        {
            logger.info("Renewing local node id (as requested)");
            NodeId.renewLocalId();
        }

        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        Thread drainOnShutdown = new Thread(new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws ExecutionException, InterruptedException, IOException
            {
                ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);
                if (mutationStage.isShutdown())
                    return; // drained already

                stopRPCServer();
                optionalTasks.shutdown();
                Gossiper.instance.stop();

                // In-progress writes originating here could generate hints to be written, so shut down MessagingService
                // before mutation stage, so we can get all the hints saved before shutting down
                MessagingService.instance().shutdown();
                mutationStage.shutdown();
                mutationStage.awaitTermination(3600, TimeUnit.SECONDS);
                StorageProxy.instance.verifyNoHintsInProgress();

                List<Future<?>> flushes = new ArrayList<Future<?>>();
                for (Table table : Table.all())
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(table.name);
                    if (!ksm.durableWrites)
                    {
                        for (ColumnFamilyStore cfs : table.getColumnFamilyStores())
                        {
                            Future<?> future = cfs.forceFlush();
                            if (future != null)
                                flushes.add(future);
                        }
                    }
                }
                FBUtilities.waitOnFutures(flushes);

                CommitLog.instance.shutdownBlocking();

                // wait for miscellaneous tasks like sstable and commitlog segment deletion
                tasks.shutdown();
                if (!tasks.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);

        if (Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true")))
        {
            joinTokenRing(delay);
        }
        else
        {
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    }

    private void joinTokenRing(int delay) throws IOException, org.apache.cassandra.config.ConfigurationException
    {
        logger.info("Starting up server gossip");
        joined = true;

        // Seed the host ID-to-endpoint map with our own ID.
        getTokenMetadata().updateHostId(SystemTable.getLocalHostId(), FBUtilities.getBroadcastAddress());

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a nodeId to our state, below.)
        Gossiper.instance.register(this);
        Gossiper.instance.register(migrationManager);
        Gossiper.instance.start(SystemTable.incrementAndGetGeneration()); // needed for node-ring gathering.
        // gossip network proto version
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        // gossip schema version when gossiper is running
        Schema.instance.updateVersionAndAnnounce();
        // add rpc listening info
        Gossiper.instance.addLocalApplicationState(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(DatabaseDescriptor.getRpcAddress()));
        if (0 != DatabaseDescriptor.getReplaceTokens().size())
            Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.hibernate(true));

        MessagingService.instance().listen(FBUtilities.getLocalAddress());
        LoadBroadcaster.instance.startBroadcasting();
        MigrationManager.passiveAnnounce(Schema.instance.getVersion());
        Gossiper.instance.addLocalApplicationState(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

        HintedHandOffManager.instance.start();

        // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
        // If we are a seed, or if the user manually sets auto_bootstrap to false,
        // we'll skip streaming data from other nodes and jump directly into the ring.
        //
        // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
        // which is useful for both new users and testing.
        //
        // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
        // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
        Set<InetAddress> current = new HashSet<InetAddress>();
        Collection<Token> tokens;
        logger.debug("Bootstrap variables: {} {} {} {}",
                      new Object[]{ DatabaseDescriptor.isAutoBootstrap(),
                                    SystemTable.bootstrapInProgress(),
                                    SystemTable.bootstrapComplete(),
                                    DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())});
        if (DatabaseDescriptor.isAutoBootstrap()
            && !SystemTable.bootstrapComplete()
            && !DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()))
        {
            if (SystemTable.bootstrapInProgress())
                logger.warn("Detected previous bootstrap failure; retrying");
            else
                SystemTable.setBootstrapState(SystemTable.BootstrapState.IN_PROGRESS);
            setMode(Mode.JOINING, "waiting for ring information", true);
            // first sleep the delay to make sure we see all our peers
            for (int i = 0; i < delay; i += 1000)
            {
                // if we see schema, we can proceed to the next check directly
                if (!Schema.instance.getVersion().equals(Schema.emptyVersion))
                {
                    logger.debug("got schema: {}", Schema.instance.getVersion());
                    break;
                }
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
            // if our schema hasn't matched yet, keep sleeping until it does
            // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
            while (!MigrationManager.isReadyForBootstrap())
            {
                setMode(Mode.JOINING, "waiting for schema information to complete", true);
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
            setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);


            if (logger.isDebugEnabled())
                logger.debug("... got ring + schema info");

            if (DatabaseDescriptor.getReplaceTokens().size() == 0)
            {
                if (tokenMetadata.isMember(FBUtilities.getBroadcastAddress()))
                {
                    String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                setMode(Mode.JOINING, "getting bootstrap token", true);
                tokens = BootStrapper.getBootstrapTokens(tokenMetadata, LoadBroadcaster.instance.getLoadInfo());
            }
            else
            {
                try
                {
                    // Sleeping additionally to make sure that the server actually is not alive
                    // and giving it more time to gossip if alive.
                    Thread.sleep(LoadBroadcaster.BROADCAST_INTERVAL);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                tokens = new ArrayList<Token>();
                for (String token : DatabaseDescriptor.getReplaceTokens())
                    tokens.add(StorageService.getPartitioner().getTokenFactory().fromString(token));

                // check for operator errors...
                for (Token token : tokens)
                {
                    InetAddress existing = tokenMetadata.getEndpoint(token);
                    if (existing != null)
                    {
                        if (Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp() > (System.currentTimeMillis() - delay))
                            throw new UnsupportedOperationException("Cannnot replace a token for a Live node... ");
                        current.add(existing);
                    }
                }

                setMode(Mode.JOINING, "Replacing a node with token: " + tokens, true);
            }

            bootstrap(tokens);
            assert !isBootstrapMode; // bootstrap will block until finished
        }
        else
        {
            tokens = SystemTable.getSavedTokens();
            if (tokens.isEmpty())
            {
                Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
                if (initialTokens.size() < 1)
                {
                    tokens = BootStrapper.getRandomTokens(tokenMetadata, DatabaseDescriptor.getNumTokens());
                    if (DatabaseDescriptor.getNumTokens() == 1)
                        logger.warn("Generated random token " + tokens + ". Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations");
                    else
                        logger.info("Generated random tokens.");
                }
                else
                {
                    tokens = new ArrayList<Token>();
                    for (String token : initialTokens)
                        tokens.add(getPartitioner().getTokenFactory().fromString(token));
                    logger.info("Saved token not found. Using " + tokens + " from configuration");
                }
            }
            else
            {
                // if we were already bootstrapped with 1 token but num_tokens is set higher in the config,
                // then we need to migrate to multi-token
                if (tokens.size() == 1 && DatabaseDescriptor.getNumTokens() > 1)
                {
                    // wait for ring info
                    logger.info("Sleeping for ring delay (" + delay + "ms)");
                    try
                    {
                        Thread.sleep(delay);
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                    logger.info("Calculating new tokens");
                    // calculate num_tokens tokens evenly spaced in the range (left, right]
                    Token right = tokens.iterator().next();
                    TokenMetadata clone = tokenMetadata.cloneOnlyTokenMap();
                    clone.updateNormalToken(right, FBUtilities.getBroadcastAddress());
                    Token left = clone.getPredecessor(right);

                    // get (num_tokens - 1) tokens spaced evenly, and the last token will be our current token (right)
                    for (int tok = 1; tok < DatabaseDescriptor.getNumTokens(); ++tok)
                    {
                        Token l = left;
                        Token r = right;
                        // iteratively calculate the location of the token using midpoint
                        // num iterations is number of bits in IEE754 mantissa (including implicit leading 1)
                        // we stop early for terminating fractions
                        // TODO: alternatively we could add an interpolate() method to IPartitioner
                        double frac = (double)tok / (double)DatabaseDescriptor.getNumTokens();
                        Token midpoint = getPartitioner().midpoint(l, r);
                        for (int i = 0; i < 53; ++i)
                        {
                            frac *= 2;
                            if (frac == 1.0) /* not a bug */
                                break;
                            else if (frac > 1.0)
                            {
                                l = midpoint;
                                frac -= 1.0;
                            }
                            else
                                r = midpoint;
                            midpoint = getPartitioner().midpoint(l, r);
                        }
                        tokens.add(midpoint);
                    }
                    logger.info("Split previous range (" + left + ", " + right + "] into " + tokens);
                }
                else
                    logger.info("Using saved token " + tokens);
            }
        }

        if (!isSurveyMode)
        {
            // start participating in the ring.
            SystemTable.setBootstrapState(SystemTable.BootstrapState.COMPLETED);
            setTokens(tokens);
            // remove the existing info about the replaced node.
            if (!current.isEmpty())
                for (InetAddress existing : current)
                    Gossiper.instance.replacedEndpoint(existing);
            logger.info("Bootstrap/Replace/Move completed! Now serving reads.");
            assert tokenMetadata.sortedTokens().size() > 0;
        }
        else
        {
            logger.info("Bootstrap complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
        }
    }

    public synchronized void joinRing() throws IOException, ConfigurationException
    {
        if (!joined)
        {
            logger.info("Joining ring by operator request");
            joinTokenRing(0);
        }
        else if (isSurveyMode)
        {
            setTokens(SystemTable.getSavedTokens());
            SystemTable.setBootstrapState(SystemTable.BootstrapState.COMPLETED);
            isSurveyMode = false;
            logger.info("Leaving write survey mode and joining ring at operator request");
            assert tokenMetadata.sortedTokens().size() > 0;
        }
    }

    public boolean isJoined()
    {
        return joined;
    }

    public void rebuild(String sourceDc)
    {
        logger.info("rebuild from dc: {}", sourceDc == null ? "(any dc)" : sourceDc);

        RangeStreamer streamer = new RangeStreamer(tokenMetadata, FBUtilities.getBroadcastAddress(), OperationType.REBUILD);
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));
        if (sourceDc != null)
            streamer.addSourceFilter(new RangeStreamer.SingleDatacenterFilter(DatabaseDescriptor.getEndpointSnitch(), sourceDc));

        for (String table : Schema.instance.getNonSystemTables())
            streamer.addRanges(table, getLocalRanges(table));

        streamer.fetch();
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

    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
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

    private void bootstrap(Collection<Token> tokens) throws IOException
    {
        isBootstrapMode = true;
        SystemTable.updateTokens(tokens); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        if (0 == DatabaseDescriptor.getReplaceTokens().size())
        {
            // if not an existing token then bootstrap
            Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS,
                                                       valueFactory.bootstrapping(tokens, SystemTable.getLocalHostId()));
            setMode(Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
            try
            {
                Thread.sleep(RING_DELAY);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
        else
        {
            // Dont set any state for the node which is bootstrapping the existing token...
            tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
        }
        Tracing.instance();
        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, tokenMetadata).bootstrap(); // handles token update
    }

    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata;
    }

    /**
     * Gossip about the known severity of the events in this node
     */
    public synchronized boolean reportSeverity(double incr)
    {
        if (!Gossiper.instance.isEnabled())
            return false;
        double update = getSeverity(FBUtilities.getBroadcastAddress()) + incr;
        VersionedValue updated = StorageService.instance.valueFactory.severity(update);
        Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, updated);
        return true;
    }
    
    public double getSeverity(InetAddress endpoint)
    {
        VersionedValue event;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state != null && (event = state.getApplicationState(ApplicationState.SEVERITY)) != null)
            return Double.parseDouble(event.value);
        return 0.0;
    }

    /**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
        for (Map.Entry<Range<Token>,List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey().asList(), stringify(entry.getValue()));
        }
        return map;
    }

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return
     */
    public String getRpcaddress(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return DatabaseDescriptor.getRpcAddress().getHostAddress();
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS) == null)
            return endpoint.getHostAddress();
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value;
    }

    /**
     * for a keyspace, return the ranges and corresponding RPC addresses for a given keyspace.
     * @param keyspace
     * @return
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<String>(entry.getValue().size());
            for (InetAddress endpoint: entry.getValue())
            {
                rpcaddrs.add(getRpcaddress(endpoint));
            }
            map.put(entry.getKey().asList(), rpcaddrs);
        }
        return map;
    }

    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemTables().get(0);

        Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : tokenMetadata.getPendingRanges(keyspace).entrySet())
        {
            List<InetAddress> l = new ArrayList<InetAddress>(entry.getValue());
            map.put(entry.getKey().asList(), stringify(l));
        }
        return map;
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemTables().get(0);

        List<Range<Token>> ranges = getAllRanges(tokenMetadata.sortedTokens());
        return constructRangeToEndpointMap(keyspace, ranges);
    }

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List<String> describeRingJMX(String keyspace) throws InvalidRequestException
    {
        List<TokenRange> tokenRanges = describeRing(keyspace);
        List<String> result = new ArrayList<String>(tokenRanges.size());

        for (TokenRange tokenRange : tokenRanges)
            result.add(tokenRange.toString());

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
        if (keyspace == null || !Schema.instance.getNonSystemTables().contains(keyspace))
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);

        List<TokenRange> ranges = new ArrayList<TokenRange>();
        Token.TokenFactory tf = getPartitioner().getTokenFactory();

        for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            Range range = entry.getKey();
            List<InetAddress> addresses = entry.getValue();
            List<String> endpoints = new ArrayList<String>(addresses.size());
            List<String> rpc_endpoints = new ArrayList<String>(addresses.size());
            List<EndpointDetails> epDetails = new ArrayList<EndpointDetails>(addresses.size());

            for (InetAddress endpoint : addresses)
            {
                EndpointDetails details = new EndpointDetails();
                details.host = endpoint.getHostAddress();
                details.datacenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
                details.rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);

                endpoints.add(details.host);
                rpc_endpoints.add(getRpcaddress(endpoint));

                epDetails.add(details);
            }

            TokenRange tr = new TokenRange(tf.toString(range.left.getToken()), tf.toString(range.right.getToken()), endpoints)
                                    .setEndpoint_details(epDetails)
                                    .setRpc_endpoints(rpc_endpoints);

            ranges.add(tr);
        }

        return ranges;
    }

    public Map<String, String> getTokenToEndpointMap()
    {
        Map<Token, InetAddress> mapInetAddress = tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap();
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<String, String>(mapInetAddress.size());
        List<Token> tokens = new ArrayList<Token>(mapInetAddress.keySet());
        Collections.sort(tokens);
        for (Token token : tokens)
        {
            mapString.put(token.toString(), mapInetAddress.get(token).getHostAddress());
        }
        return mapString;
    }

    public String getLocalHostId()
    {
        return getTokenMetadata().getHostId(FBUtilities.getBroadcastAddress()).toString();
    }

    public Map<String, String> getHostIdMap()
    {
        Map<String, String> mapOut = new HashMap<String, String>();
        for (Map.Entry<InetAddress, UUID> entry : getTokenMetadata().getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(), entry.getValue().toString());
        return mapOut;
    }

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range<Token>, List<InetAddress>> constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges)
    {
        Map<Range<Token>, List<InetAddress>> rangeToEndpointMap = new HashMap<Range<Token>, List<InetAddress>>();
        for (Range<Token> range : ranges)
        {
            rangeToEndpointMap.put(range, Table.open(keyspace).getReplicationStrategy().getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
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
     * you should never bootstrap a new node during a removetoken, decommission or move.
     */
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        switch (state)
        {
            case STATUS:
                String apStateValue = value.value;
                String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
                assert (pieces.length > 0);

                String moveName = pieces[0];

                if (moveName.equals(VersionedValue.STATUS_BOOTSTRAPPING))
                    handleStateBootstrap(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_NORMAL))
                    handleStateNormal(endpoint, pieces);
                else if (moveName.equals(VersionedValue.REMOVING_TOKEN) || moveName.equals(VersionedValue.REMOVED_TOKEN))
                    handleStateRemoving(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_LEAVING))
                    handleStateLeaving(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_LEFT))
                    handleStateLeft(endpoint, pieces);
                else if (moveName.equals(VersionedValue.STATUS_MOVING))
                    handleStateMoving(endpoint, pieces);
        }
    }

    /**
     * Checks MS for the version, provided MS _really_ knows it (has directly communicated with the node) otherwise falls back to checking the gossipped version (learned about this node indirectly)
     * If both fail, the node is too old to use hostid-style status serialization
     * @param endpoint
     * @return boolean whether or not to use hostid
     */
    private boolean usesHostId(InetAddress endpoint)
    {
        if (MessagingService.instance().knowsVersion(endpoint) && MessagingService.instance().getVersion(endpoint) >= MessagingService.VERSION_12)
            return true;
        else  if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NET_VERSION) != null && Integer.valueOf(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NET_VERSION).value) >= MessagingService.VERSION_12)
                return true;
        return false;
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     * @param pieces STATE_BOOTSTRAPPING,bootstrap token as string
     */
    private void handleStateBootstrap(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;

        // Parse versioned values according to end-point version:
        //   versions  < 1.2 .....: STATUS,TOKEN
        //   versions >= 1.2 .....: STATUS,HOST_ID,TOKEN,TOKEN,...
        int tokenPos;
        if (usesHostId(endpoint))
        {
            assert pieces.length >= 3;
            tokenPos = 2;
        }
            else tokenPos = 1;

        Collection<Token> tokens = new ArrayList<Token>();
        for (int i = tokenPos; i < pieces.length; ++i)
            tokens.add(getPartitioner().getTokenFactory().fromString(pieces[i]));

        if (logger.isDebugEnabled())
            logger.debug("Node " + endpoint + " state bootstrapping, token " + tokens);

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
                logger.info("Node " + endpoint + " state jump to bootstrap");
            tokenMetadata.removeEndpoint(endpoint);
        }

        tokenMetadata.addBootstrapTokens(tokens, endpoint);
        calculatePendingRanges();

        if (usesHostId(endpoint))
            tokenMetadata.updateHostId(UUID.fromString(pieces[1]), endpoint);
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     * @param pieces STATE_NORMAL,token
     */
    private void handleStateNormal(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;

        // Parse versioned values according to end-point version:
        //   versions  < 1.2 .....: STATUS,TOKEN
        //   versions >= 1.2 .....: STATUS,HOST_ID,TOKEN,TOKEN,...
        int tokensPos;
        if (usesHostId(endpoint))
        {
            assert pieces.length >= 3;
            tokensPos = 2;
        }
        else
            tokensPos = 1;
        logger.debug("Using token position {} for {}", tokensPos, endpoint);

        Collection<Token> tokens = new ArrayList<Token>();
        for (int i = tokensPos; i < pieces.length; ++i)
            tokens.add(getPartitioner().getTokenFactory().fromString(pieces[i]));

        if (logger.isDebugEnabled())
            logger.debug("Node " + endpoint + " state normal, token " + tokens);

        if (tokenMetadata.isMember(endpoint))
            logger.info("Node " + endpoint + " state jump to normal");

        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
        if (usesHostId(endpoint))
            tokenMetadata.updateHostId(UUID.fromString(pieces[1]), endpoint);

        Set<Token> tokensToUpdateInMetadata = new HashSet<Token>();
        Set<Token> tokensToUpdateInSystemTable = new HashSet<Token>();
        Set<InetAddress> endpointsToRemove = new HashSet<InetAddress>();
        Multimap<InetAddress, Token> epToTokenCopy = getTokenMetadata().getEndpointToTokenMapForReading();

        for (Token token : tokens)
        {
            // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
            InetAddress currentOwner = tokenMetadata.getEndpoint(token);
            if (currentOwner == null)
            {
                logger.debug("New node " + endpoint + " at token " + token);
                tokensToUpdateInMetadata.add(token);
                if (!isClientMode)
                    tokensToUpdateInSystemTable.add(token);
            }
            else if (endpoint.equals(currentOwner))
            {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                // no need to persist, token/ip did not change
                tokensToUpdateInMetadata.add(token);
            }
            else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0)
            {
                tokensToUpdateInMetadata.add(token);
                if (!isClientMode)
                    tokensToUpdateInSystemTable.add(token);

                // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).size() < 1)
                    endpointsToRemove.add(currentOwner);

                logger.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner",
                                          endpoint,
                                          currentOwner,
                                          token,
                                          endpoint));
            }
            else
            {
                logger.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s",
                                           endpoint,
                                           currentOwner,
                                           token,
                                           endpoint));
            }
        }

        tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (InetAddress ep : endpointsToRemove)
            Gossiper.instance.removeEndpoint(ep);
        SystemTable.updateTokens(endpoint, tokensToUpdateInSystemTable);

        if (tokenMetadata.isMoving(endpoint)) // if endpoint was moving to a new token
            tokenMetadata.removeFromMoving(endpoint);

        calculatePendingRanges();
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     * @param pieces STATE_LEAVING,token
     */
    private void handleStateLeaving(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Collection<Token> tokens = new ArrayList<Token>();
        for (int i = 1; i < pieces.length; ++i)
            tokens.add(getPartitioner().getTokenFactory().fromString(pieces[i]));

        if (logger.isDebugEnabled())
            logger.debug("Node " + endpoint + " state leaving, tokens " + tokens);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata.isMember(endpoint))
        {
            logger.info("Node " + endpoint + " state jump to leaving");
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }
        else if (!tokenMetadata.getTokens(endpoint).containsAll(tokens))
        {
            logger.warn("Node " + endpoint + " 'leaving' token mismatch. Long network partition?");
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata.addLeavingEndpoint(endpoint);
        calculatePendingRanges();
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Collection<Token> tokens = null;
        Integer version = MessagingService.instance().getVersion(endpoint);
        if (version < MessagingService.VERSION_12)
            tokens = Arrays.asList(getPartitioner().getTokenFactory().fromString(pieces[1]));
        else
        {
            tokens = new ArrayList<Token>(pieces.length - 2);
            for (int i = 2; i < pieces.length; ++i)
                tokens.add(getPartitioner().getTokenFactory().fromString(pieces[i]));
        }

        if (logger.isDebugEnabled())
            logger.debug("Node " + endpoint + " state left, tokens " + tokens);

        excise(tokens, endpoint, extractExpireTime(pieces, version));
    }

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    private void handleStateMoving(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger.isDebugEnabled())
            logger.debug("Node " + endpoint + " state moving, new token " + token);

        tokenMetadata.addMovingEndpoint(token, endpoint);

        calculatePendingRanges();
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removetoken'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(InetAddress endpoint, String[] pieces)
    {
        assert (pieces.length > 0);

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            logger.info("Received removeToken gossip about myself. Is this node rejoining after an explicit removetoken?");
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
                excise(removeTokens, endpoint, extractExpireTime(pieces, MessagingService.instance().getVersion(endpoint)));
            }
            else if (VersionedValue.REMOVING_TOKEN.equals(state))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Tokens " + removeTokens + " removed manually (endpoint was " + endpoint + ")");

                // Note that the endpoint is being removed
                tokenMetadata.addLeavingEndpoint(endpoint);
                calculatePendingRanges();

                // find the endpoint coordinating this removal that we need to notify when we're done
                String[] coordinator = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR).value.split(VersionedValue.DELIMITER_STR, -1);
                UUID hostId = UUID.fromString(coordinator[1]);
                // grab any data we are now responsible for and notify responsible node
                restoreReplicaCount(endpoint, tokenMetadata.getEndpointForHostId(hostId));
            }
        } // not a member, nothing to do
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint)
    {
        HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
        Gossiper.instance.removeEndpoint(endpoint);
        tokenMetadata.removeEndpoint(endpoint);
        tokenMetadata.removeBootstrapTokens(tokens);
        calculatePendingRanges();
        if (!isClientMode)
        {
            logger.info("Removing tokens " + tokens + " for " + endpoint);
            SystemTable.removeTokens(tokens);
        }
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tokens, endpoint);
    }

    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    protected long extractExpireTime(String[] pieces, int version)
    {
        if (version < MessagingService.VERSION_12)
        {
            if (pieces.length >= 3)
                return Long.parseLong(pieces[2]);
            else
                return 0L;
        } else
        {
            if (VersionedValue.STATUS_LEFT.equals(pieces[0]))
                return Long.parseLong(pieces[1]);
            else
                return Long.parseLong(pieces[2]);
        }
    }

    /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    private void calculatePendingRanges()
    {
        for (String table : Schema.instance.getNonSystemTables())
            calculatePendingRanges(Table.open(table).getReplicationStrategy(), table);
    }

    // public & static for testing purposes
    public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String table)
    {
        TokenMetadata tm = StorageService.instance.getTokenMetadata();
        Multimap<Range<Token>, InetAddress> pendingRanges = HashMultimap.create();
        BiMultiValMap<Token, InetAddress> bootstrapTokens = tm.getBootstrapTokens();
        Set<InetAddress> leavingEndpoints = tm.getLeavingEndpoints();

        if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && tm.getMovingEndpoints().isEmpty())
        {
            if (logger.isDebugEnabled())
                logger.debug("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", table);
            tm.setPendingRanges(table, pendingRanges);
            return;
        }

        Multimap<InetAddress, Range<Token>> addressRanges = strategy.getAddressRanges();

        // Copy of metadata reflecting the situation after all leave operations are finished.
        TokenMetadata allLeftMetadata = tm.cloneAfterAllLeft();

        // get all ranges that will be affected by leaving nodes
        Set<Range<Token>> affectedRanges = new HashSet<Range<Token>>();
        for (InetAddress endpoint : leavingEndpoints)
            affectedRanges.addAll(addressRanges.get(endpoint));

        // for each of those ranges, find what new nodes will be responsible for the range when
        // all leaving nodes are gone.
        for (Range<Token> range : affectedRanges)
        {
            Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, tm.cloneOnlyTokenMap()));
            Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
            pendingRanges.putAll(range, Sets.difference(newEndpoints, currentEndpoints));
        }

        // At this stage pendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.

        // For each of the bootstrapping nodes, simply add and remove them one by one to
        // allLeftMetadata and check in between what their ranges would be.
        for (InetAddress endpoint : bootstrapTokens.inverse().keySet())
        {
            Collection<Token> tokens = bootstrapTokens.inverse().get(endpoint);
            
            allLeftMetadata.updateNormalTokens(tokens, endpoint);
            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                pendingRanges.put(range, endpoint);
            allLeftMetadata.removeEndpoint(endpoint);
        }

        // At this stage pendingRanges has been updated according to leaving and bootstrapping nodes.
        // We can now finish the calculation by checking moving nodes.

        // For each of the moving nodes, we do the same thing we did for bootstrapping:
        // simply add and remove them one by one to allLeftMetadata and check in between what their ranges would be.
        for (Pair<Token, InetAddress> moving : tm.getMovingEndpoints())
        {
            InetAddress endpoint = moving.right; // address of the moving node

            //  moving.left is a new token of the endpoint
            allLeftMetadata.updateNormalToken(moving.left, endpoint);

            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
            {
                pendingRanges.put(range, endpoint);
            }

            allLeftMetadata.removeEndpoint(endpoint);
        }

        tm.setPendingRanges(table, pendingRanges);

        if (logger.isDebugEnabled())
            logger.debug("Pending ranges:\n" + (pendingRanges.isEmpty() ? "<empty>" : tm.printPendingRanges()));
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param table the table ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    private Multimap<InetAddress, Range<Token>> getNewSourceRanges(String table, Set<Range<Token>> ranges)
    {
        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        Multimap<Range<Token>, InetAddress> rangeAddresses = Table.open(table).getReplicationStrategy().getRangeAddresses(tokenMetadata.cloneOnlyTokenMap());
        Multimap<InetAddress, Range<Token>> sourceRanges = HashMultimap.create();
        IFailureDetector failureDetector = FailureDetector.instance;

        // find alive sources for our new ranges
        for (Range<Token> range : ranges)
        {
            Collection<InetAddress> possibleRanges = rangeAddresses.get(range);
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            List<InetAddress> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);

            assert (!sources.contains(myAddress));

            for (InetAddress source : sources)
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
    private void sendReplicationNotification(InetAddress remote)
    {
        // notify the remote token
        MessageOut msg = new MessageOut(MessagingService.Verb.REPLICATION_FINISHED);
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger.isDebugEnabled())
            logger.debug("Notifying " + remote.toString() + " of replication completion\n");
        while (failureDetector.isAlive(remote))
        {
            IAsyncResult iar = MessagingService.instance().sendRR(msg, remote);
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
    private void restoreReplicaCount(InetAddress endpoint, final InetAddress notifyEndpoint)
    {
        final Multimap<InetAddress, String> fetchSources = HashMultimap.create();
        Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();

        final InetAddress myAddress = FBUtilities.getBroadcastAddress();

        for (String table : Schema.instance.getNonSystemTables())
        {
            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(table, endpoint);
            Set<Range<Token>> myNewRanges = new HashSet<Range<Token>>();
            for (Map.Entry<Range<Token>, InetAddress> entry : changedRanges.entries())
            {
                if (entry.getValue().equals(myAddress))
                    myNewRanges.add(entry.getKey());
            }
            Multimap<InetAddress, Range<Token>> sourceRanges = getNewSourceRanges(table, myNewRanges);
            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : sourceRanges.asMap().entrySet())
            {
                fetchSources.put(entry.getKey(), table);
                rangesToFetch.put(table, entry);
            }
        }

        for (final String table : rangesToFetch.keySet())
        {
            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : rangesToFetch.get(table))
            {
                final InetAddress source = entry.getKey();
                Collection<Range<Token>> ranges = entry.getValue();
                final IStreamCallback callback = new IStreamCallback()
                {
                    public void onSuccess()
                    {
                        synchronized (fetchSources)
                        {
                            fetchSources.remove(source, table);
                            if (fetchSources.isEmpty())
                                sendReplicationNotification(notifyEndpoint);
                        }
                    }

                    public void onFailure()
                    {
                        logger.warn("Streaming from " + source + " failed");
                        onSuccess(); // calling onSuccess to send notification
                    }
                };
                if (logger.isDebugEnabled())
                    logger.debug("Requesting from " + source + " ranges " + StringUtils.join(ranges, ", "));
                StreamIn.requestRanges(source, table, ranges, callback, OperationType.RESTORE_REPLICA_COUNT);
            }
        }
    }

    // needs to be modified to accept either a table or ARS.
    private Multimap<Range<Token>, InetAddress> getChangedRangesForLeaving(String table, InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range<Token>> ranges = getRangesForEndpoint(table, endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node " + endpoint + " ranges [" + StringUtils.join(ranges, ", ") + "]");

        Map<Range<Token>, List<InetAddress>> currentReplicaEndpoints = new HashMap<Range<Token>, List<InetAddress>>();

        // Find (for each range) all nodes that store replicas for these ranges as well
        for (Range<Token> range : ranges)
            currentReplicaEndpoints.put(range, Table.open(table).getReplicationStrategy().calculateNaturalEndpoints(range.right, tokenMetadata.cloneOnlyTokenMap()));

        TokenMetadata temp = tokenMetadata.cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removetoken
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range<Token>, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoints list, but
        // not in the currentReplicaEndpoints list, will be needing the
        // range.
        for (Range<Token> range : ranges)
        {
            Collection<InetAddress> newReplicaEndpoints = Table.open(table).getReplicationStrategy().calculateNaturalEndpoints(range.right, temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger.isDebugEnabled())
                if (newReplicaEndpoints.isEmpty())
                    logger.debug("Range " + range + " already in all replicas");
                else
                    logger.debug("Range " + range + " will be responsibility of " + StringUtils.join(newReplicaEndpoints, ", "));
            changedRanges.putAll(range, newReplicaEndpoints);
        }

        return changedRanges;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        if (!isClientMode && getTokenMetadata().isMember(endpoint))
            HintedHandOffManager.instance.scheduleHintDelivery(endpoint);
    }

    public void onRemove(InetAddress endpoint)
    {
        tokenMetadata.removeEndpoint(endpoint);
        calculatePendingRanges();
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        MessagingService.instance().convict(endpoint);
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(endpoint, state);
    }

    /** raw load value */
    public double getLoad()
    {
        double bytes = 0;
        for (String tableName : Schema.instance.getTables())
        {
            Table table = Table.open(tableName);
            for (ColumnFamilyStore cfs : table.getColumnFamilyStores())
                bytes += cfs.getLiveDiskSpaceUsed();
        }
        return bytes;
    }

    public String getLoadString()
    {
        return FileUtils.stringifyFileSize(getLoad());
    }

    public Map<String, String> getLoadMap()
    {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<InetAddress,Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet())
        {
            map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(FBUtilities.getBroadcastAddress().getHostAddress(), getLoadString());
        return map;
    }

    public final void deliverHints(String host) throws UnknownHostException
    {
        HintedHandOffManager.instance.scheduleHintDelivery(host);
    }

    public Collection<Token> getLocalTokens()
    {
        Collection<Token> tokens = SystemTable.getSavedTokens();
        assert tokens != null && !tokens.isEmpty(); // should not be called before initServer sets this
        return tokens;
    }

    /* These methods belong to the MBean interface */

    public List<String> getTokens()
    {
        return getTokens(FBUtilities.getBroadcastAddress());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException
    {
        return getTokens(InetAddress.getByName(endpoint));
    }

    private List<String> getTokens(InetAddress endpoint)
    {
        List<String> strTokens = new ArrayList<String>();
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

    public List<String> getLeavingNodes()
    {
        return stringify(tokenMetadata.getLeavingEndpoints());
    }

    public List<String> getMovingNodes()
    {
        List<String> endpoints = new ArrayList<String>();

        for (Pair<Token, InetAddress> node : tokenMetadata.getMovingEndpoints())
        {
            endpoints.add(node.right.getHostAddress());
        }

        return endpoints;
    }

    public List<String> getJoiningNodes()
    {
        return stringify(tokenMetadata.getBootstrapTokens().values());
    }

    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers());
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

    private List<String> stringify(Iterable<InetAddress> endpoints)
    {
        List<String> stringEndpoints = new ArrayList<String>();
        for (InetAddress ep : endpoints)
        {
            stringEndpoints.add(ep.getHostAddress());
        }
        return stringEndpoints;
    }

    public int getCurrentGenerationNumber()
    {
        return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddress());
    }

    public void forceTableCleanup(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (tableName.equals(Table.SYSTEM_KS))
            throw new RuntimeException("Cleanup of the system table is neither necessary nor wise");

        NodeId.OneShotRenewer nodeIdRenewer = new NodeId.OneShotRenewer();
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.forceCleanup(nodeIdRenewer);
        }
    }

    public void scrub(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
            cfStore.scrub();
    }

    public void upgradeSSTables(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
            cfStore.sstablesRewrite();
    }

    public void forceTableCompaction(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.forceMajorCompaction();
        }
    }

    /**
     * Takes the snapshot for the given tables. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param tableNames the name of the tables to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... tableNames) throws IOException
    {
        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Iterable<Table> tables;
        if (tableNames.length == 0)
        {
            tables = Table.all();
        }
        else
        {
            ArrayList<Table> t = new ArrayList<Table>(tableNames.length);
            for (String table : tableNames)
                t.add(getValidTable(table));
            tables = t;
        }

        // Do a check to see if this snapshot exists before we actually snapshot
        for (Table table : tables)
            if (table.snapshotExists(tag))
                throw new IOException("Snapshot " + tag + " already exists.");


        for (Table table : tables)
            table.snapshot(tag, null);
    }

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param tableName the keyspace which holds the specified column family
     * @param columnFamilyName the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    public void takeColumnFamilySnapshot(String tableName, String columnFamilyName, String tag) throws IOException
    {
        if (tableName == null)
            throw new IOException("You must supply a table name");

        if (columnFamilyName == null)
            throw new IOException("You mus supply a column family name");

        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Table table = getValidTable(tableName);
        if (table.snapshotExists(tag))
            throw new IOException("Snapshot " + tag + " already exists.");

        table.snapshot(tag, columnFamilyName);
    }

    private Table getValidTable(String tableName) throws IOException
    {
        if (!Schema.instance.getTables().contains(tableName))
        {
            throw new IOException("Table " + tableName + " does not exist");
        }
        return Table.open(tableName);
    }

    /**
     * Remove the snapshot with the given name from the given tables.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... tableNames) throws IOException
    {
        if(tag == null)
            tag = "";

        Iterable<Table> tables;
        if (tableNames.length == 0)
        {
            tables = Table.all();
        }
        else
        {
            ArrayList<Table> tempTables = new ArrayList<Table>(tableNames.length);
            for(String table : tableNames)
                tempTables.add(getValidTable(table));
            tables = tempTables;
        }

        for (Table table : tables)
            table.clearSnapshot(tag);

        if (logger.isDebugEnabled())
            logger.debug("Cleared out snapshot directories");
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(String tableName, String... cfNames) throws IOException
    {
        Table table = getValidTable(tableName);

        if (cfNames.length == 0)
            // all stores are interesting
            return table.getColumnFamilyStores();

        // filter out interesting stores
        Set<ColumnFamilyStore> valid = new HashSet<ColumnFamilyStore>();
        for (String cfName : cfNames)
        {
            ColumnFamilyStore cfStore = table.getColumnFamilyStore(cfName);
            if (cfStore == null)
            {
                // this means there was a cf passed in that is not recognized in the keyspace. report it and continue.
                logger.warn(String.format("Invalid column family specified: %s. Proceeding with others.", cfName));
                continue;
            }
            valid.add(cfStore);
        }
        return valid;
    }

    /**
     * Flush all memtables for a table and column families.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableFlush(final String tableName, final String... columnFamilies)
                throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            logger.debug("Forcing flush on keyspace " + tableName + ", CF " + cfStore.getColumnFamilyName());
            cfStore.forceBlockingFlush();
        }
    }

    /**
     * Trigger proactive repair for a table and column families.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableRepair(final String tableName, boolean isSequential, final String... columnFamilies) throws IOException
    {
        if (Table.SYSTEM_KS.equals(tableName))
            return;

        Collection<Range<Token>> ranges = getLocalRanges(tableName);
        int cmd = nextRepairCommand.incrementAndGet();
        logger.info("Starting repair command #{}, repairing {} ranges.", cmd, ranges.size());

        List<AntiEntropyService.RepairFuture> futures = new ArrayList<AntiEntropyService.RepairFuture>(ranges.size());
        for (Range<Token> range : ranges)
        {
            AntiEntropyService.RepairFuture future = forceTableRepair(range, tableName, isSequential, columnFamilies);
            if (future == null)
                continue;
            futures.add(future);
            // wait for a session to be done with its differencing before starting the next one
            try
            {
                future.session.differencingDone.await();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting for the differencing of repair session " + future.session + " to be done. Repair may be imprecise.", e);
            }
        }

        boolean failedSession = false;

        // block until all repair sessions have completed
        for (AntiEntropyService.RepairFuture future : futures)
        {
            try
            {
                future.get();
            }
            catch (Exception e)
            {
                logger.error("Repair session " + future.session.getName() + " failed.", e);
                failedSession = true;
            }
        }

        if (failedSession)
            throw new IOException("Repair command #" + cmd + ": some repair session(s) failed (see log for details).");
        else
            logger.info("Repair command #{} completed successfully", cmd);
    }

    public void forceTableRepairPrimaryRange(final String tableName, boolean isSequential, final String... columnFamilies) throws IOException
    {
        if (Table.SYSTEM_KS.equals(tableName))
            return;

        List<AntiEntropyService.RepairFuture> futures = new ArrayList<AntiEntropyService.RepairFuture>();
        for (Range<Token> range : getLocalPrimaryRanges())
        {
            RepairFuture future = forceTableRepair(range, tableName, isSequential, columnFamilies);
            if (future != null)
                futures.add(future);
        }
        if (futures.isEmpty())
            return;
        for (AntiEntropyService.RepairFuture future : futures)
            FBUtilities.waitOnFuture(future);
    }

    public void forceTableRepairRange(String beginToken, String endToken, final String tableName, boolean isSequential, final String... columnFamilies) throws IOException
    {
        if (Table.SYSTEM_KS.equals(tableName))
            return;

        Token parsedBeginToken = getPartitioner().getTokenFactory().fromString(beginToken);
        Token parsedEndToken = getPartitioner().getTokenFactory().fromString(endToken);

        logger.info("starting user-requested repair of range ({}, {}] for keyspace {} and column families {}",
                     new Object[] {parsedBeginToken, parsedEndToken, tableName, columnFamilies});
        AntiEntropyService.RepairFuture future = forceTableRepair(new Range<Token>(parsedBeginToken, parsedEndToken), tableName, isSequential, columnFamilies);
        if (future == null)
            return;
        try
        {
            future.get();
        }
        catch (Exception e)
        {
            logger.error("Repair session " + future.session.getName() + " failed.", e);
        }
    }

    public AntiEntropyService.RepairFuture forceTableRepair(final Range<Token> range, final String tableName, boolean isSequential, final String... columnFamilies) throws IOException
    {
        ArrayList<String> names = new ArrayList<String>();
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            names.add(cfStore.getColumnFamilyName());
        }

        if (names.isEmpty())
        {
            logger.info("No column family to repair for keyspace " + tableName);
            return null;
        }

        return AntiEntropyService.instance.submitRepairSession(range, tableName, isSequential, names.toArray(new String[names.size()]));
    }

    public void forceTerminateAllRepairSessions() {
        AntiEntropyService.instance.terminateSessions();
    }

    /* End of MBean interface methods */

    /**
     * This method returns the predecessor of the endpoint ep on the identifier
     * space.
     */
    InetAddress getPredecessor(Token token)
    {
        return tokenMetadata.getEndpoint(tokenMetadata.getPredecessor(token));
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    public InetAddress getSuccessor(Token token)
    {
        return tokenMetadata.getEndpoint(tokenMetadata.getSuccessor(token));
    }

    /**
     * Get the primary ranges for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return collection of ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangesForEndpoint(InetAddress ep)
    {
        return tokenMetadata.getPrimaryRangesFor(tokenMetadata.getTokens(ep));
    }

    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    @Deprecated
    public Range<Token> getPrimaryRangeForEndpoint(InetAddress ep)
    {
        return tokenMetadata.getPrimaryRangeFor(tokenMetadata.getToken(ep));
    }

    /**
     * Get all ranges an endpoint is responsible for (by table)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range<Token>> getRangesForEndpoint(String table, InetAddress ep)
    {
        return Table.open(table).getReplicationStrategy().getAddressRanges().get(ep);
    }

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range<Token>> getAllRanges(List<Token> sortedTokens)
    {
        if (logger.isDebugEnabled())
            logger.debug("computing ranges for " + StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        int size = sortedTokens.size();
        List<Range<Token>> ranges = new ArrayList<Range<Token>>(size + 1);
        for (int i = 1; i < size; ++i)
        {
            Range<Token> range = new Range<Token>(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range<Token> range = new Range<Token>(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param table keyspace name also known as table
     * @param cf Column family name
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String table, String cf, String key)
    {
        CFMetaData cfMetaData = Schema.instance.getTableDefinition(table).cfMetaData().get(cf);
        return getNaturalEndpoints(table, getPartitioner().getToken(cfMetaData.getKeyValidator().fromString(key)));
    }

    public List<InetAddress> getNaturalEndpoints(String table, ByteBuffer key)
    {
        return getNaturalEndpoints(table, getPartitioner().getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param table keyspace name also known as table
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(String table, RingPosition pos)
    {
        return Table.open(table).getReplicationStrategy().getNaturalEndpoints(pos);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param table keyspace name also known as table
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(String table, ByteBuffer key)
    {
        return getLiveNaturalEndpoints(table, getPartitioner().decorateKey(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(String table, RingPosition pos)
    {
        List<InetAddress> endpoints = Table.open(table).getReplicationStrategy().getNaturalEndpoints(pos);
        List<InetAddress> liveEps = new ArrayList<InetAddress>(endpoints.size());

        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    public void setLog4jLevel(String classQualifier, String rawLevel)
    {
        Level level = Level.toLevel(rawLevel);
        org.apache.log4j.Logger.getLogger(classQualifier).setLevel(level);
        logger.info("set log level to " + level + " for classes under '" + classQualifier + "' (if the level doesn't look like '" + rawLevel + "' then log4j couldn't parse '" + rawLevel + "')");
    }

    /**
     * @return list of Tokens (_not_ keys!) breaking up the data this node is responsible for into pieces of roughly keysPerSplit
     */
    public List<Token> getSplits(String table, String cfName, Range<Token> range, int keysPerSplit)
    {
        List<Token> tokens = new ArrayList<Token>();
        // we use the actual Range token for the first and last brackets of the splits to ensure correctness
        tokens.add(range.left);

        Table t = Table.open(table);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        List<DecoratedKey> keys = keySamples(Collections.singleton(cfs), range);
        int splits = keys.size() * DatabaseDescriptor.getIndexInterval() / keysPerSplit;

        if (keys.size() >= splits)
        {
            for (int i = 1; i < splits; i++)
            {
                int index = i * (keys.size() / splits);
                tokens.add(keys.get(index).token);
            }
        }

        tokens.add(range.right);
        return tokens;
    }

    private List<DecoratedKey> keySamples(Iterable<ColumnFamilyStore> cfses, Range<Token> range)
    {
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
        for (ColumnFamilyStore cfs : cfses)
            Iterables.addAll(keys, cfs.keySamples(range));
        FBUtilities.sortSampledKeys(keys, range);
        return keys;
    }

    /** return a token to which if a node bootstraps it will get about 1/2 of this node's range */
    public Token getBootstrapToken()
    {
        Range<Token> range = getLocalPrimaryRange();

        List<DecoratedKey> keys = keySamples(ColumnFamilyStore.allUserDefined(), range);

        Token token;
        if (keys.size() < 3)
        {
            token = getPartitioner().midpoint(range.left, range.right);
            logger.debug("Used midpoint to assign token " + token);
        }
        else
        {
            token = keys.get(keys.size() / 2).token;
            logger.debug("Used key sample of size " + keys.size() + " to assign token " + token);
        }
        if (tokenMetadata.getEndpoint(token) != null && tokenMetadata.isMember(tokenMetadata.getEndpoint(token)))
            throw new RuntimeException("Chose token " + token + " which is already in use by " + tokenMetadata.getEndpoint(token) + " -- specify one manually with initial_token");
        // Hack to prevent giving nodes tokens with DELIMITER_STR in them (which is fine in a row key/token)
        if (token instanceof StringToken)
        {
            token = new StringToken(((String)token.token).replaceAll(VersionedValue.DELIMITER_STR, ""));
            if (tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap().containsKey(token))
                throw new RuntimeException("Unable to compute unique token for new node -- specify one manually with initial_token");
        }
        return token;
    }

    /**
     * Broadcast leaving status and update local tokenMetadata accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.leaving(getLocalTokens()));
        tokenMetadata.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
        calculatePendingRanges();
    }

    public void decommission() throws InterruptedException
    {
        if (!tokenMetadata.isMember(FBUtilities.getBroadcastAddress()))
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
        if (tokenMetadata.cloneAfterAllLeft().sortedTokens().size() < 2)
            throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
        for (String table : Schema.instance.getNonSystemTables())
        {
            if (tokenMetadata.getPendingRanges(table, FBUtilities.getBroadcastAddress()).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        if (logger.isDebugEnabled())
            logger.debug("DECOMMISSIONING");
        startLeaving();
        setMode(Mode.LEAVING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
        Thread.sleep(RING_DELAY);

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
                stopRPCServer();
                Gossiper.instance.stop();
                MessagingService.instance().shutdown();
                StageManager.shutdownNow();
                setMode(Mode.DECOMMISSIONED, true);
                // let op be responsible for killing the process
            }
        };
        unbootstrap(finishLeaving);
    }

    private void leaveRing()
    {
        SystemTable.setBootstrapState(SystemTable.BootstrapState.NEEDS_BOOTSTRAP);
        tokenMetadata.removeEndpoint(FBUtilities.getBroadcastAddress());
        calculatePendingRanges();

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.left(getLocalTokens(),Gossiper.computeExpireTime()));
        int delay = Math.max(RING_DELAY, Gossiper.intervalInMillis * 2);
        logger.info("Announcing that I have left the ring for " + delay + "ms");
        try
        {
            Thread.sleep(delay);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    private void unbootstrap(final Runnable onFinish)
    {
        Map<String, Multimap<Range<Token>, InetAddress>> rangesToStream = new HashMap<String, Multimap<Range<Token>, InetAddress>>();

        for (final String table : Schema.instance.getNonSystemTables())
        {
            Multimap<Range<Token>, InetAddress> rangesMM = getChangedRangesForLeaving(table, FBUtilities.getBroadcastAddress());

            if (logger.isDebugEnabled())
                logger.debug("Ranges needing transfer are [" + StringUtils.join(rangesMM.keySet(), ",") + "]");

            rangesToStream.put(table, rangesMM);
        }

        setMode(Mode.LEAVING, "streaming data to other nodes", true);

        CountDownLatch latch = streamRanges(rangesToStream);

        // wait for the transfer runnables to signal the latch.
        logger.debug("waiting for stream aks.");
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("stream acks all received.");
        leaveRing();
        onFinish.run();
    }

    public void move(String newToken) throws IOException, InterruptedException, ConfigurationException
    {
        getPartitioner().getTokenFactory().validate(newToken);
        move(getPartitioner().getTokenFactory().fromString(newToken));
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
        InetAddress localAddress = FBUtilities.getBroadcastAddress();

        // This doesn't make any sense in a vnodes environment.
        if (getTokenMetadata().getTokens(localAddress).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }
        
        List<String> tablesToProcess = Schema.instance.getNonSystemTables();

        // checking if data is moving to this node
        for (String table : tablesToProcess)
        {
            if (tokenMetadata.getPendingRanges(table, localAddress).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.moving(newToken));
        setMode(Mode.MOVING, String.format("Moving %s from %s to %s.", localAddress, getLocalTokens().iterator().next(), newToken), true);

        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        Map<String, Multimap<InetAddress, Range<Token>>> rangesToFetch = new HashMap<String, Multimap<InetAddress, Range<Token>>>();
        Map<String, Multimap<Range<Token>, InetAddress>> rangesToStreamByTable = new HashMap<String, Multimap<Range<Token>, InetAddress>>();

        TokenMetadata tokenMetaCloneAllSettled = tokenMetadata.cloneAfterAllSettled();
        // clone to avoid concurrent modification in calculateNaturalEndpoints
        TokenMetadata tokenMetaClone = tokenMetadata.cloneOnlyTokenMap();

        // for each of the non system tables calculating new ranges
        // which current node will handle after move to the new token
        for (String table : tablesToProcess)
        {
            // replication strategy of the current keyspace (aka table)
            AbstractReplicationStrategy strategy = Table.open(table).getReplicationStrategy();

            // getting collection of the currently used ranges by this keyspace
            Collection<Range<Token>> currentRanges = getRangesForEndpoint(table, localAddress);
            // collection of ranges which this node will serve after move to the new token
            Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetadata, newToken, localAddress);

            // ring ranges and endpoints associated with them
            // this used to determine what nodes should we ping about range data
            Multimap<Range<Token>, InetAddress> rangeAddresses = strategy.getRangeAddresses(tokenMetaClone);

            // calculated parts of the ranges to request/stream from/to nodes in the ring
            Pair<Set<Range<Token>>, Set<Range<Token>>> rangesPerTable = calculateStreamAndFetchRanges(currentRanges, updatedRanges);

            /**
             * In this loop we are going through all ranges "to fetch" and determining
             * nodes in the ring responsible for data we are interested in
             */
            Multimap<Range<Token>, InetAddress> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
            for (Range<Token> toFetch : rangesPerTable.right)
            {
                for (Range<Token> range : rangeAddresses.keySet())
                {
                    if (range.contains(toFetch))
                    {
                        List<InetAddress> endpoints = snitch.getSortedListByProximity(localAddress, rangeAddresses.get(range));
                        // storing range and preferred endpoint set
                        rangesToFetchWithPreferredEndpoints.putAll(toFetch, endpoints);
                    }
                }
            }

            // calculating endpoints to stream current ranges to if needed
            // in some situations node will handle current ranges as part of the new ranges
            Multimap<Range<Token>, InetAddress> rangeWithEndpoints = HashMultimap.create();

            for (Range<Token> toStream : rangesPerTable.left)
            {
                Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaClone));
                Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaCloneAllSettled));
                rangeWithEndpoints.putAll(toStream, Sets.difference(newEndpoints, currentEndpoints));
            }

            // associating table with range-to-endpoints map
            rangesToStreamByTable.put(table, rangeWithEndpoints);

            Multimap<InetAddress, Range<Token>> workMap = RangeStreamer.getWorkMap(rangesToFetchWithPreferredEndpoints);
            rangesToFetch.put(table, workMap);

            if (logger.isDebugEnabled())
                logger.debug("Table {}: work map {}.", table, workMap);
        }

        if (!rangesToStreamByTable.isEmpty() || !rangesToFetch.isEmpty())
        {
            setMode(Mode.MOVING, String.format("Sleeping %s ms before start streaming/fetching ranges", RING_DELAY), true);
            try
            {
                Thread.sleep(RING_DELAY);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Sleep interrupted " + e.getMessage());
            }

            setMode(Mode.MOVING, "fetching new ranges and streaming old ranges", true);
            if (logger.isDebugEnabled())
                logger.debug("[Move->STREAMING] Work Map: " + rangesToStreamByTable);

            CountDownLatch streamLatch = streamRanges(rangesToStreamByTable);

            if (logger.isDebugEnabled())
                logger.debug("[Move->FETCHING] Work Map: " + rangesToFetch);

            CountDownLatch fetchLatch = requestRanges(rangesToFetch);

            try
            {
                streamLatch.await();
                fetchLatch.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted latch while waiting for stream/fetch ranges to finish: " + e.getMessage());
            }
        }

        setTokens(Collections.singleton(newToken)); // setting new token as we have everything settled

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", getLocalTokens().iterator().next());
    }

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus()
    {
        if (removingNode == null) {
            return "No token removals in process.";
        }
        return String.format("Removing token (%s). Waiting for replication confirmation from [%s].",
                             tokenMetadata.getToken(removingNode),
                             StringUtils.join(replicatingNodes, ","));
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
        if (!replicatingNodes.isEmpty()  || !tokenMetadata.getLeavingEndpoints().isEmpty())
        {
            logger.warn("Removal not confirmed for for " + StringUtils.join(this.replicatingNodes, ","));
            for (InetAddress endpoint : tokenMetadata.getLeavingEndpoints())
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
            throw new UnsupportedOperationException("No tokens to force removal on, call 'removetoken' first");
        }
    }

    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString token for the node
     */
    public void removeNode(String hostIdString)
    {
        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        UUID localHostId = tokenMetadata.getHostId(myAddress);
        UUID hostId = UUID.fromString(hostIdString);
        InetAddress endpoint = tokenMetadata.getEndpointForHostId(hostId);

        if (endpoint == null)
            throw new UnsupportedOperationException("Host ID not found.");

        Collection<Token> tokens = tokenMetadata.getTokens(endpoint);

        if (endpoint.equals(myAddress))
             throw new UnsupportedOperationException("Cannot remove self");

        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");

        // A leaving endpoint that is dead is already being removed.
        if (tokenMetadata.isLeaving(endpoint))
            logger.warn("Node " + endpoint + " is already being removed, continuing removal anyway");

        if (!replicatingNodes.isEmpty())
            throw new UnsupportedOperationException("This node is already processing a removal. Wait for it to complete, or use 'removetoken force' if this has failed.");

        // Find the endpoints that are going to become responsible for data
        for (String table : Schema.instance.getNonSystemTables())
        {
            // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
            if (Table.open(table).getReplicationStrategy().getReplicationFactor() == 1)
                continue;

            // get all ranges that change ownership (that is, a node needs
            // to take responsibility for new range)
            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(table, endpoint);
            IFailureDetector failureDetector = FailureDetector.instance;
            for (InetAddress ep : changedRanges.values())
            {
                if (failureDetector.isAlive(ep))
                    replicatingNodes.add(ep);
                else
                    logger.warn("Endpoint " + ep + " is down and will not receive data for re-replication of " + endpoint);
            }
        }
        removingNode = endpoint;

        tokenMetadata.addLeavingEndpoint(endpoint);
        calculatePendingRanges();
        // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
        // we add our own token so other nodes to let us know when they're done
        Gossiper.instance.advertiseRemoving(endpoint, hostId, localHostId);

        // kick off streaming commands
        restoreReplicaCount(endpoint, myAddress);

        // wait for ReplicationFinishedVerbHandler to signal we're done
        while (!replicatingNodes.isEmpty())
        {
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }

        excise(tokens, endpoint);

        // gossiper will indicate the token has left
        Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);

        replicatingNodes.clear();
        removingNode = null;
    }

    public void confirmReplication(InetAddress node)
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
            logger.info("Received unexpected REPLICATION_FINISHED message from " + node
                         + ". Was this node recently a removal coordinator?");
        }
    }

    public boolean isClientMode()
    {
        return isClientMode;
    }

    public synchronized void requestGC()
    {
        if (hasUnreclaimedSpace())
        {
            logger.info("requesting GC to free disk space");
            System.gc();
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private boolean hasUnreclaimedSpace()
    {
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfs.hasUnreclaimedSpace())
                return true;
        }
        return false;
    }

    public String getOperationMode()
    {
        return operationMode.toString();
    }

    public String getDrainProgress()
    {
        return String.format("Drained %s/%s ColumnFamilies", remainingCFs, totalCFs);
    }

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     * There are two differences between drain and the normal shutdown hook:
     * - Drain waits for in-progress streaming to complete
     * - Drain flushes *all* columnfamilies (shutdown hook only flushes non-durable CFs)
     */
    public synchronized void drain() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);
        if (mutationStage.isTerminated())
        {
            logger.warn("Cannot drain node (did it already happen?)");
            return;
        }
        setMode(Mode.DRAINING, "starting drain process", true);
        stopRPCServer();
        optionalTasks.shutdown();
        Gossiper.instance.stop();

        setMode(Mode.DRAINING, "shutting down MessageService", false);
        MessagingService.instance().shutdown();
        setMode(Mode.DRAINING, "waiting for streaming", false);
        MessagingService.instance().waitForStreaming();

        setMode(Mode.DRAINING, "clearing mutation stage", false);
        mutationStage.shutdown();
        mutationStage.awaitTermination(3600, TimeUnit.SECONDS);

        StorageProxy.instance.verifyNoHintsInProgress();

        setMode(Mode.DRAINING, "flushing column families", false);
        List<ColumnFamilyStore> cfses = new ArrayList<ColumnFamilyStore>();
        for (String tableName : Schema.instance.getNonSystemTables())
        {
            Table table = Table.open(tableName);
            cfses.addAll(table.getColumnFamilyStores());
        }
        totalCFs = remainingCFs = cfses.size();
        for (ColumnFamilyStore cfs : cfses)
        {
            cfs.forceBlockingFlush();
            remainingCFs--;
        }

        ColumnFamilyStore.postFlushExecutor.shutdown();
        ColumnFamilyStore.postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);

        CommitLog.instance.shutdownBlocking();

        // wait for miscellaneous tasks like sstable and commitlog segment deletion
        tasks.shutdown();
        if (!tasks.awaitTermination(1, TimeUnit.MINUTES))
            logger.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");

        setMode(Mode.DRAINED, true);
    }

    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = DatabaseDescriptor.getPartitioner();
        DatabaseDescriptor.setPartitioner(newPartitioner);
        valueFactory = new VersionedValue.VersionedValueFactory(getPartitioner());
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd)
    {
        TokenMetadata old = tokenMetadata;
        tokenMetadata = tmd;
        return old;
    }

    public void truncate(String keyspace, String columnFamily) throws UnavailableException, TimeoutException, IOException
    {
        StorageProxy.truncateBlocking(keyspace, columnFamily);
    }

    public boolean isDcAwareReplicationStrategy(String keyspace)
    {
        return SimpleStrategy.class != Table.open(keyspace).getReplicationStrategy().getClass();
    }

    public Map<InetAddress, Float> getOwnership()
    {
        List<Token> sortedTokens = tokenMetadata.sortedTokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<Token, Float>(getPartitioner().describeOwnership(sortedTokens));
        Map<InetAddress, Float> stringMap = new LinkedHashMap<InetAddress, Float>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet())
            stringMap.put(tokenMetadata.getEndpoint(entry.getKey()), entry.getValue());
        return stringMap;
    }

    /**
     * Calculates ownership. If there are multiple DC's and the replication strategy is DC aware then ownership will be
     * calculated per dc, i.e. each DC will have total ring ownership divided amongst its nodes. Without replication
     * total ownership will be a multiple of the number of DC's and this value will then go up within each DC depending
     * on the number of replicas within itself. For DC unaware replication strategies, ownership without replication
     * will be 100%.
     * 
     * @throws ConfigurationException
     */
    public LinkedHashMap<InetAddress, Float> effectiveOwnership(String keyspace) throws ConfigurationException
    {
        if (Schema.instance.getNonSystemTables().size() <= 0)
            throw new ConfigurationException("Couldn't find any Non System Keyspaces to infer replication topology");
        if (keyspace == null && !hasSameReplication(Schema.instance.getNonSystemTables()))
            throw new ConfigurationException("Non System keyspaces doesnt have the same topology");

        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemTables().get(0);

        Collection<Collection<InetAddress>> endpointsGroupedByDc = new ArrayList<Collection<InetAddress>>();
        // mapping of dc's to nodes, use sorted map so that we get dcs sorted
        SortedMap<String, Collection<InetAddress>> sortedDcsToEndpoints = new TreeMap<String, Collection<InetAddress>>();
        sortedDcsToEndpoints.putAll(metadata.getTopology().getDatacenterEndpoints().asMap());
        for (Collection<InetAddress> endpoints : sortedDcsToEndpoints.values())
            endpointsGroupedByDc.add(endpoints);

        Map<Token, Float> tokenOwnership = getPartitioner().describeOwnership(tokenMetadata.sortedTokens());
        LinkedHashMap<InetAddress, Float> finalOwnership = Maps.newLinkedHashMap();

        // calculate ownership per dc
        for (Collection<InetAddress> endpoints : endpointsGroupedByDc)
        {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (InetAddress endpoint : endpoints)
            {
                float ownership = 0.0f;
                for (Range<Token> range : getRangesForEndpoint(keyspace, endpoint))
                {
                    if (tokenOwnership.containsKey(range.right))
                        ownership += tokenOwnership.get(range.right);
                }
                finalOwnership.put(endpoint, ownership);
            }
        }
        return finalOwnership;
    }


    private boolean hasSameReplication(List<String> list)
    {
        if (list.isEmpty())
            return false;

        for (int i = 0; i < list.size() -1; i++)
        {
            KSMetaData ksm1 = Schema.instance.getKSMetaData(list.get(i));
            KSMetaData ksm2 = Schema.instance.getKSMetaData(list.get(i + 1));
            if (!ksm1.strategyClass.equals(ksm2.strategyClass) ||
                    !Iterators.elementsEqual(ksm1.strategyOptions.entrySet().iterator(),
                                             ksm2.strategyOptions.entrySet().iterator()))
                return false;
        }
        return true;
    }

    public List<String> getKeyspaces()
    {
        List<String> tableslist = new ArrayList<String>(Schema.instance.getTables());
        return Collections.unmodifiableList(tableslist);
    }

    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ConfigurationException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();

        // new snitch registers mbean during construction
        IEndpointSnitch newSnitch = FBUtilities.construct(epSnitchClassName, "snitch");
        if (dynamic)
        {
            DatabaseDescriptor.setDynamicUpdateInterval(dynamicUpdateInterval);
            DatabaseDescriptor.setDynamicResetInterval(dynamicResetInterval);
            DatabaseDescriptor.setDynamicBadnessThreshold(dynamicBadnessThreshold);
            newSnitch = new DynamicEndpointSnitch(newSnitch);
        }

        // point snitch references to the new instance
        DatabaseDescriptor.setEndpointSnitch(newSnitch);
        for (String ks : Schema.instance.getTables())
        {
            Table.open(ks).getReplicationStrategy().snitch = newSnitch;
        }

        if (oldSnitch instanceof DynamicEndpointSnitch)
            ((DynamicEndpointSnitch)oldSnitch).unregisterMBean();
    }

    /**
     * Flushes the two largest memtables by ops and by throughput
     */
    public void flushLargestMemtables()
    {
        ColumnFamilyStore largest = null;
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            long total = cfs.getTotalMemtableLiveSize();

            if (total > 0 && (largest == null || total > largest.getTotalMemtableLiveSize()))
            {
                logger.debug(total + " estimated memtable size for " + cfs);
                largest = cfs;
            }
        }
        if (largest == null)
        {
            logger.info("Unable to reduce heap usage since there are no dirty column families");
            return;
        }

        logger.warn("Flushing " + largest + " to relieve memory pressure");
        largest.forceFlush();
    }

    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByTable tables and data ranges with endpoints included for each
     * @return latch to count down
     */
    private CountDownLatch streamRanges(final Map<String, Multimap<Range<Token>, InetAddress>> rangesToStreamByTable)
    {
        // First, we build a list of ranges to stream to each host, per table
        final Map<String, Map<InetAddress, List<Range<Token>>>> sessionsToStreamByTable = new HashMap<String, Map<InetAddress, List<Range<Token>>>>();
        // The number of stream out sessions we need to start, to be built up as we build sessionsToStreamByTable
        int sessionCount = 0;

        for (Map.Entry<String, Multimap<Range<Token>, InetAddress>> entry : rangesToStreamByTable.entrySet())
        {
            Multimap<Range<Token>, InetAddress> rangesWithEndpoints = entry.getValue();

            if (rangesWithEndpoints.isEmpty())
                continue;

            final String table = entry.getKey();

            Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = new HashMap<InetAddress, List<Range<Token>>>();

            for (final Map.Entry<Range<Token>, InetAddress> endPointEntry : rangesWithEndpoints.entries())
            {
                final Range<Token> range = endPointEntry.getKey();
                final InetAddress endpoint = endPointEntry.getValue();

                List<Range<Token>> curRanges = rangesPerEndpoint.get(endpoint);
                if (curRanges == null)
                {
                    curRanges = new LinkedList<Range<Token>>();
                    rangesPerEndpoint.put(endpoint, curRanges);
                }
                curRanges.add(range);
            }

            sessionCount += rangesPerEndpoint.size();
            sessionsToStreamByTable.put(table, rangesPerEndpoint);
        }

        final CountDownLatch latch = new CountDownLatch(sessionCount);

        for (Map.Entry<String, Map<InetAddress, List<Range<Token>>>> entry : sessionsToStreamByTable.entrySet())
        {
            final String table = entry.getKey();
            final Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = entry.getValue();

            for (final Map.Entry<InetAddress, List<Range<Token>>> rangesEntry : rangesPerEndpoint.entrySet())
            {
                final List<Range<Token>> ranges = rangesEntry.getValue();
                final InetAddress newEndpoint = rangesEntry.getKey();

                final IStreamCallback callback = new IStreamCallback()
                {
                    public void onSuccess()
                    {
                        latch.countDown();
                    }

                    public void onFailure()
                    {
                        logger.warn("Streaming to " + newEndpoint + " failed");
                        onSuccess(); // calling onSuccess for latch countdown
                    }
                };

                StageManager.getStage(Stage.STREAM).execute(new Runnable()
                {
                    public void run()
                    {
                        // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                        StreamOut.transferRanges(newEndpoint, Table.open(table), ranges, callback,
                                OperationType.UNBOOTSTRAP);
                    }
                });
            }
        }
        return latch;
    }

    /**
     * Used to request ranges from endpoints in the ring (will block until all data is fetched and ready)
     * @param ranges ranges to fetch as map of the preferred address and range collection
     * @return latch to count down
     */
    private CountDownLatch requestRanges(final Map<String, Multimap<InetAddress, Range<Token>>> ranges)
    {
        final CountDownLatch latch = new CountDownLatch(ranges.keySet().size());
        for (Map.Entry<String, Multimap<InetAddress, Range<Token>>> entry : ranges.entrySet())
        {
            Multimap<InetAddress, Range<Token>> endpointWithRanges = entry.getValue();

            if (endpointWithRanges.isEmpty())
            {
                latch.countDown();
                continue;
            }

            final String table = entry.getKey();
            final Set<InetAddress> pending = new HashSet<InetAddress>(endpointWithRanges.keySet());

            // Send messages to respective folks to stream data over to me
            for (final InetAddress source: endpointWithRanges.keySet())
            {
                Collection<Range<Token>> toFetch = endpointWithRanges.get(source);

                final IStreamCallback callback = new IStreamCallback()
                {
                    public void onSuccess()
                    {
                        pending.remove(source);

                        if (pending.isEmpty())
                            latch.countDown();
                    }

                    public void onFailure()
                    {
                        logger.warn("Streaming from " + source + " failed");
                        onSuccess(); // calling onSuccess for latch countdown
                    }
                };

                if (logger.isDebugEnabled())
                    logger.debug("Requesting from " + source + " ranges " + StringUtils.join(toFetch, ", "));

                // sending actual request
                StreamIn.requestRanges(source, table, toFetch, callback, OperationType.BOOTSTRAP);
            }
        }
        return latch;
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for table and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    public Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(Collection<Range<Token>> current, Collection<Range<Token>> updated)
    {
        Set<Range<Token>> toStream = new HashSet<Range<Token>>();
        Set<Range<Token>> toFetch  = new HashSet<Range<Token>>();


        for (Range r1 : current)
        {
            boolean intersect = false;
            for (Range r2 : updated)
            {
                if (r1.intersects(r2))
                {
                    // adding difference ranges to fetch from a ring
                    toStream.addAll(r1.subtract(r2));
                    intersect = true;
                    break;
                }
            }
            if (!intersect)
            {
                toStream.add(r1); // should seed whole old range
            }
        }

        for (Range r2 : updated)
        {
            boolean intersect = false;
            for (Range r1 : current)
            {
                if (r2.intersects(r1))
                {
                    // adding difference ranges to fetch from a ring
                    toFetch.addAll(r2.subtract(r1));
                    intersect = true;
                    break;
                }
            }
            if (!intersect)
            {
                toFetch.add(r2); // should fetch whole old range
            }
        }

        return new Pair<Set<Range<Token>>, Set<Range<Token>>>(toStream, toFetch);
    }

    public void bulkLoad(String directory)
    {
        File dir = new File(directory);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Invalid directory " + directory);

        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            @Override
            public void init(String keyspace)
            {
                try
                {
                    setPartitioner(DatabaseDescriptor.getPartitioner());
                    for (Map.Entry<Range<Token>, List<InetAddress>> entry : StorageService.instance.getRangeToAddressMap(keyspace).entrySet())
                    {
                        Range<Token> range = entry.getKey();
                        for (InetAddress endpoint : entry.getValue())
                            addRangeForEndpoint(range, endpoint);
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean validateColumnFamily(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName) != null;
            }
        };

        SSTableLoader loader = new SSTableLoader(dir, client, new OutputHandler.LogOutput());
        try
        {
            loader.stream().get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public int getExceptionCount()
    {
        return CassandraDaemon.exceptions.get();
    }

    public void rescheduleFailedDeletions()
    {
        SSTableDeletingTask.rescheduleFailedTasks();
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
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
        for (Range<Token> range : getLocalPrimaryRanges())
            keys.addAll(keySamples(ColumnFamilyStore.allUserDefined(), range));

        List<String> sampledKeys = new ArrayList<String>(keys.size());
        for (DecoratedKey key : keys)
            sampledKeys.add(key.getToken().toString());
        return sampledKeys;
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore.rebuildSecondaryIndex(ksName, cfName, idxNames);
    }

    public void resetLocalSchema() throws IOException
    {
        MigrationManager.resetLocalSchema();
    }

    public void setTraceProbability(double probability)
    {
        this.tracingProbability = probability;
    }

    public double getTracingProbability()
    {
        return tracingProbability;
    }
}
