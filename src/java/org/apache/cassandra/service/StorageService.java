/**
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
import java.io.IOError;
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
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.service.AntiEntropyService.TreeRequestVerbHandler;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.thrift.Constants;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService implements IEndpointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = LoggerFactory.getLogger(StorageService.class);

    public static final int RING_DELAY = 30 * 1000; // delay after which we assume ring has stablized

    /* All verb handler identifiers */
    public enum Verb
    {
        MUTATION,
        BINARY, // Deprecated
        READ_REPAIR,
        READ,
        REQUEST_RESPONSE, // client-initiated reads and writes
        STREAM_INITIATE, // Deprecated
        STREAM_INITIATE_DONE, // Deprecated
        STREAM_REPLY,
        STREAM_REQUEST,
        RANGE_SLICE,
        BOOTSTRAP_TOKEN,
        TREE_REQUEST,
        TREE_RESPONSE,
        JOIN, // Deprecated
        GOSSIP_DIGEST_SYN,
        GOSSIP_DIGEST_ACK,
        GOSSIP_DIGEST_ACK2,
        DEFINITIONS_ANNOUNCE, // Deprecated
        DEFINITIONS_UPDATE,
        TRUNCATE,
        SCHEMA_CHECK,
        INDEX_SCAN,
        REPLICATION_FINISHED,
        INTERNAL_RESPONSE, // responses to internal calls
        COUNTER_MUTATION,
        STREAMING_REPAIR_REQUEST,
        STREAMING_REPAIR_RESPONSE,
        // use as padding for backwards compatability where a previous version needs to validate a verb from the future.
        UNUSED_1,
        UNUSED_2,
        UNUSED_3,
        ;
        // remember to add new verbs at the end, since we serialize by ordinal
    }
    public static final Verb[] VERBS = Verb.values();

    public static final EnumMap<StorageService.Verb, Stage> verbStages = new EnumMap<StorageService.Verb, Stage>(StorageService.Verb.class)
    {{
        put(Verb.MUTATION, Stage.MUTATION);
        put(Verb.BINARY, Stage.MUTATION);
        put(Verb.READ_REPAIR, Stage.MUTATION);
        put(Verb.READ, Stage.READ);
        put(Verb.REQUEST_RESPONSE, Stage.REQUEST_RESPONSE);
        put(Verb.STREAM_REPLY, Stage.MISC); // TODO does this really belong on misc? I've just copied old behavior here
        put(Verb.STREAM_REQUEST, Stage.STREAM);
        put(Verb.RANGE_SLICE, Stage.READ);
        put(Verb.BOOTSTRAP_TOKEN, Stage.MISC);
        put(Verb.TREE_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.TREE_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
        put(Verb.DEFINITIONS_UPDATE, Stage.READ);
        put(Verb.TRUNCATE, Stage.MUTATION);
        put(Verb.SCHEMA_CHECK, Stage.MIGRATION);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);
        put(Verb.COUNTER_MUTATION, Stage.MUTATION);
        put(Verb.UNUSED_1, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_2, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_3, Stage.INTERNAL_RESPONSE);
    }};


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
    private TokenMetadata tokenMetadata_ = new TokenMetadata();

    private IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    public VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

    public static final StorageService instance = new StorageService();

    public static IPartitioner getPartitioner()
    {
        return instance.partitioner;
    }

    public Collection<Range> getLocalRanges(String table)
    {
        return getRangesForEndpoint(table, FBUtilities.getBroadcastAddress());
    }

    public Range getLocalPrimaryRange()
    {
        return getPrimaryRangeForEndpoint(FBUtilities.getBroadcastAddress());
    }

    private Set<InetAddress> replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
    private CassandraDaemon daemon;

    private InetAddress removingNode;

    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    /* when intialized as a client, we shouldn't write to the system table. */
    private boolean isClientMode;
    private boolean initialized;
    private volatile boolean joined = false;

    private static enum Mode { NORMAL, CLIENT, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED }
    private Mode operationMode;

    private MigrationManager migrationManager = new MigrationManager();

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();

    public void finishBootstrapping()
    {
        isBootstrapMode = false;
    }

    /** This method updates the local token on disk  */
    public void setToken(Token token)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Setting token to {}", token);
        SystemTable.updateToken(token);
        tokenMetadata_.updateNormalToken(token, FBUtilities.getBroadcastAddress());
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.normal(getLocalToken()));
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
        MessagingService.instance().registerVerbHandlers(Verb.MUTATION, new RowMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.READ_REPAIR, new ReadRepairVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.READ, new ReadVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.RANGE_SLICE, new RangeSliceVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.INDEX_SCAN, new IndexScanVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.COUNTER_MUTATION, new CounterMutationVerbHandler());
        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(Verb.BOOTSTRAP_TOKEN, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.STREAM_REQUEST, new StreamRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.STREAM_REPLY, new StreamReplyVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.REPLICATION_FINISHED, new ReplicationFinishedVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.REQUEST_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.INTERNAL_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.TREE_REQUEST, new TreeRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.TREE_RESPONSE, new AntiEntropyService.TreeResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.STREAMING_REPAIR_REQUEST, new StreamingRepairTask.StreamingRepairRequest());
        MessagingService.instance().registerVerbHandlers(Verb.STREAMING_REPAIR_RESPONSE, new StreamingRepairTask.StreamingRepairResponse());

        MessagingService.instance().registerVerbHandlers(Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler());

        MessagingService.instance().registerVerbHandlers(Verb.DEFINITIONS_UPDATE, new DefinitionsUpdateVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.TRUNCATE, new TruncateVerbHandler());
        MessagingService.instance().registerVerbHandlers(Verb.SCHEMA_CHECK, new SchemaCheckVerbHandler());

        // spin up the streaming serivice so it is available for jmx tools.
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
            logger_.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            initialized = false;
        }
    }

    // should only be called via JMX
    public void startGossiping()
    {
        if (!initialized)
        {
            logger_.warn("Starting gossip by operator request");
            Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));
            initialized = true;
        }
    }

    // should only be called via JMX
    public void startRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured RPC daemon");
        }
        daemon.startRPCServer();
    }

    // should only be called via JMX
    public void stopRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured RPC daemon");
        }
        daemon.stopRPCServer();
    }

    public boolean isRPCServerRunning()
    {
        if (daemon == null)
        {
            return false;
        }
        return daemon.isRPCServerRunning();
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
        logger_.info("Starting up client gossip");
        setMode(Mode.CLIENT, false);
        Gossiper.instance.register(this);
        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        MessagingService.instance().listen(FBUtilities.getLocalAddress());

        // sleep a while to allow gossip to warm up (the other nodes need to know about this one before they can reply).
        try
        {
            Thread.sleep(delay);
        }
        catch (Exception ex)
        {
            throw new IOError(ex);
        }
        MigrationManager.passiveAnnounce(Schema.instance.getVersion());
    }

    public synchronized void initServer() throws IOException, ConfigurationException
    {
        initServer(RING_DELAY);
    }

    public synchronized void initServer(int delay) throws IOException, ConfigurationException
    {
        logger_.info("Cassandra version: " + FBUtilities.getReleaseVersionString());
        logger_.info("Thrift API version: " + Constants.VERSION);

        if (initialized)
        {
            if (isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = false;

        if (Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true")))
        {
            logger_.info("Loading persisted ring state");
            for (Map.Entry<Token, InetAddress> entry : SystemTable.loadTokens().entrySet())
            {
                if (entry.getValue() == FBUtilities.getLocalAddress())
                {
                    // entry has been mistakenly added, delete it
                    SystemTable.removeToken(entry.getKey());
                }
                else
                {
                    tokenMetadata_.updateNormalToken(entry.getKey(), entry.getValue());
                    Gossiper.instance.addSavedEndpoint(entry.getValue());
                }
            }
        }

        if (Boolean.parseBoolean(System.getProperty("cassandra.renew_counter_id", "false")))
        {
            logger_.info("Renewing local node id (as requested)");
            NodeId.renewLocalId();
        }

        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        Thread drainOnShutdown = new Thread(new WrappedRunnable()
        {
            public void runMayThrow() throws ExecutionException, InterruptedException, IOException
            {
                ThreadPoolExecutor mutationStage = StageManager.getStage(Stage.MUTATION);
                if (mutationStage.isShutdown())
                    return; // drained already

                optionalTasks.shutdown();
                Gossiper.instance.stop();
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
                    logger_.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);

        if (Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true")))
        {
            joinTokenRing(delay);
        }
        else
        {
            logger_.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    }

    private void joinTokenRing(int delay) throws IOException, org.apache.cassandra.config.ConfigurationException
    {
        logger_.info("Starting up server gossip");
        joined = true;

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a nodeId to our state, below.)
        Gossiper.instance.register(this);
        Gossiper.instance.register(migrationManager);
        Gossiper.instance.start(SystemTable.incrementAndGetGeneration()); // needed for node-ring gathering.
        // add rpc listening info
        Gossiper.instance.addLocalApplicationState(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(DatabaseDescriptor.getRpcAddress()));
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.hibernate(null != DatabaseDescriptor.getReplaceToken()));

        MessagingService.instance().listen(FBUtilities.getLocalAddress());
        LoadBroadcaster.instance.startBroadcasting();
        MigrationManager.passiveAnnounce(Schema.instance.getVersion());
        Gossiper.instance.addLocalApplicationState(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

        HintedHandOffManager.instance.registerMBean();

        if (DatabaseDescriptor.isAutoBootstrap()
                && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())
                && !SystemTable.isBootstrapped())
            logger_.info("This node will not auto bootstrap because it is configured to be a seed node.");

        InetAddress current = null;
        // first startup is only chance to bootstrap
        Token<?> token;
        if (DatabaseDescriptor.isAutoBootstrap()
            && !(SystemTable.isBootstrapped()
                 || DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())
                 || !Schema.instance.getNonSystemTables().isEmpty()))
        {
            setMode(Mode.JOINING, "waiting for ring and schema information", true);
            try
            {
                Thread.sleep(delay);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            if (logger_.isDebugEnabled())
                logger_.debug("... got ring + schema info");

            if (DatabaseDescriptor.getReplaceToken() == null)
            {
                if (tokenMetadata_.isMember(FBUtilities.getBroadcastAddress()))
                {
                    String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                setMode(Mode.JOINING, "getting bootstrap token", true);
                token = BootStrapper.getBootstrapToken(tokenMetadata_, LoadBroadcaster.instance.getLoadInfo());
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
                token = StorageService.getPartitioner().getTokenFactory().fromString(DatabaseDescriptor.getReplaceToken());
                // check for operator errors...
                current = tokenMetadata_.getEndpoint(token);
                if (null != current && Gossiper.instance.getEndpointStateForEndpoint(current).getUpdateTimestamp() > (System.currentTimeMillis() - delay))
                    throw new UnsupportedOperationException("Cannnot replace a token for a Live node... ");
                setMode(Mode.JOINING, "Replacing a node with token: " + token, true);
            }

            bootstrap(token);
            assert !isBootstrapMode; // bootstrap will block until finished
        }
        else
        {
            token = SystemTable.getSavedToken();
            if (token == null)
            {
                String initialToken = DatabaseDescriptor.getInitialToken();
                if (initialToken == null)
                {
                    token = partitioner.getRandomToken();
                    logger_.warn("Generated random token " + token + ". Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations");
                }
                else
                {
                    token = partitioner.getTokenFactory().fromString(initialToken);
                    logger_.info("Saved token not found. Using " + token + " from configuration");
                }
            }
            else
            {
                logger_.info("Using saved token " + token);
            }
        }

        // start participating in the ring.
        SystemTable.setBootstrapped(true);
        setToken(token);
        // remove the existing info about the replaced node.
        if (current != null)
            Gossiper.instance.replacedEndpoint(current);
        logger_.info("Bootstrap/Replace/Move completed! Now serving reads.");
        assert tokenMetadata_.sortedTokens().size() > 0;
    }

    public synchronized void joinRing() throws IOException, org.apache.cassandra.config.ConfigurationException
    {
        if (!joined)
        {
            logger_.info("Joining ring by operator request");
            joinTokenRing(0);
        }
    }

    public boolean isJoined()
    {
        return joined;
    }

    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
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
            logger_.info(logMsg);
        else
            logger_.debug(logMsg);
    }

    private void bootstrap(Token token) throws IOException
    {
        isBootstrapMode = true;
        SystemTable.updateToken(token); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        if (null == DatabaseDescriptor.getReplaceToken())
        {
            // if not an existing token then bootstrap
            Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.bootstrapping(token));
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
            tokenMetadata_.updateNormalToken(token, FBUtilities.getBroadcastAddress());
        }
        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        new BootStrapper(FBUtilities.getBroadcastAddress(), token, tokenMetadata_).bootstrap(); // handles token update
    }

    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata_;
    }

    /**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return
     */
    public Map<Range, List<String>> getRangeToEndpointMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        for (Map.Entry<Range,List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey(), stringify(entry.getValue()));
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
    public Map<Range, List<String>> getRangeToRpcaddressMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        for (Map.Entry<Range,List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<String>();
            for (InetAddress endpoint: entry.getValue())
            {
                rpcaddrs.add(getRpcaddress(endpoint));
            }
            map.put(entry.getKey(), rpcaddrs);
        }
        return map;
    }

    public Map<Range, List<String>> getPendingRangeToEndpointMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemTables().get(0);

        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        for (Map.Entry<Range, Collection<InetAddress>> entry : tokenMetadata_.getPendingRanges(keyspace).entrySet())
        {
            List<InetAddress> l = new ArrayList<InetAddress>(entry.getValue());
            map.put(entry.getKey(), stringify(l));
        }
        return map;
    }

    public Map<Range, List<InetAddress>> getRangeToAddressMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemTables().get(0);

        List<Range> ranges = getAllRanges(tokenMetadata_.sortedTokens());
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
        List<String> result = new ArrayList<String>();

        for (TokenRange tokenRange : describeRing(keyspace))
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

        for (Map.Entry<Range, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            Range range = entry.getKey();
            List<String> endpoints = new ArrayList<String>();
            List<String> rpc_endpoints = new ArrayList<String>();
            List<EndpointDetails> epDetails = new ArrayList<EndpointDetails>();

            for (InetAddress endpoint : entry.getValue())
            {
                EndpointDetails details = new EndpointDetails();
                details.host = endpoint.getHostAddress();
                details.datacenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
                details.rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);

                endpoints.add(details.host);
                rpc_endpoints.add(getRpcaddress(endpoint));

                epDetails.add(details);
            }

            TokenRange tr = new TokenRange(tf.toString(range.left), tf.toString(range.right), endpoints)
                                    .setEndpoint_details(epDetails)
                                    .setRpc_endpoints(rpc_endpoints);

            ranges.add(tr);
        }

        return ranges;
    }

    public Map<Token, String> getTokenToEndpointMap()
    {
        Map<Token, InetAddress> mapInetAddress = tokenMetadata_.getTokenToEndpointMap();
        Map<Token, String> mapString = new HashMap<Token, String>(mapInetAddress.size());
        for (Map.Entry<Token, InetAddress> entry : mapInetAddress.entrySet())
        {
            mapString.put(entry.getKey(), entry.getValue().getHostAddress());
        }
        return mapString;
    }

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range, List<InetAddress>> constructRangeToEndpointMap(String keyspace, List<Range> ranges)
    {
        Map<Range, List<InetAddress>> rangeToEndpointMap = new HashMap<Range, List<InetAddress>>();
        for (Range range : ranges)
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
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     * @param pieces STATE_BOOTSTRAPPING,bootstrap token as string
     */
    private void handleStateBootstrap(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " state bootstrapping, token " + token);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata_.isMember(endpoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetadata_.isLeaving(endpoint))
                logger_.info("Node " + endpoint + " state jump to bootstrap");
            tokenMetadata_.removeEndpoint(endpoint);
        }

        tokenMetadata_.addBootstrapToken(token, endpoint);
        calculatePendingRanges();
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
        Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " state normal, token " + token);

        if (tokenMetadata_.isMember(endpoint))
            logger_.info("Node " + endpoint + " state jump to normal");

        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        InetAddress currentOwner = tokenMetadata_.getEndpoint(token);
        if (currentOwner == null)
        {
            logger_.debug("New node " + endpoint + " at token " + token);
            tokenMetadata_.updateNormalToken(token, endpoint);
            if (!isClientMode)
                SystemTable.updateToken(endpoint, token);
        }
        else if (endpoint.equals(currentOwner))
        {
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            // no need to persist, token/ip did not change
            tokenMetadata_.updateNormalToken(token, endpoint);
        }
        else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0)
        {
            logger_.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner",
                                       endpoint, currentOwner, token, endpoint));
            tokenMetadata_.updateNormalToken(token, endpoint);
            Gossiper.instance.removeEndpoint(currentOwner);
            if (!isClientMode)
                SystemTable.updateToken(endpoint, token);
        }
        else
        {
            logger_.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s",
                                       endpoint, currentOwner, token, endpoint));
        }

        if (tokenMetadata_.isMoving(endpoint)) // if endpoint was moving to a new token
            tokenMetadata_.removeFromMoving(endpoint);

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
        String moveValue = pieces[1];
        Token token = getPartitioner().getTokenFactory().fromString(moveValue);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " state leaving, token " + token);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata_.isMember(endpoint))
        {
            logger_.info("Node " + endpoint + " state jump to leaving");
            tokenMetadata_.updateNormalToken(token, endpoint);
        }
        else if (!tokenMetadata_.getToken(endpoint).equals(token))
        {
            logger_.warn("Node " + endpoint + " 'leaving' token mismatch. Long network partition?");
            tokenMetadata_.updateNormalToken(token, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata_.addLeavingEndpoint(endpoint);
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
        Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " state left, token " + token);

        excise(token, endpoint, extractExpireTime(pieces));
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

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " state moving, new token " + token);

        tokenMetadata_.addMovingEndpoint(token, endpoint);

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
            logger_.info("Received removeToken gossip about myself. Is this node rejoining after an explicit removetoken?");
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
        if (tokenMetadata_.isMember(endpoint))
        {
            String state = pieces[0];
            Token removeToken = tokenMetadata_.getToken(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state))
            {
                excise(removeToken, endpoint, extractExpireTime(pieces));
            }
            else if (VersionedValue.REMOVING_TOKEN.equals(state))
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Token " + removeToken + " removed manually (endpoint was " + endpoint + ")");

                // Note that the endpoint is being removed
                tokenMetadata_.addLeavingEndpoint(endpoint);
                calculatePendingRanges();

                // find the endpoint coordinating this removal that we need to notify when we're done
                String[] coordinator = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR).value.split(VersionedValue.DELIMITER_STR, -1);
                Token coordtoken = getPartitioner().getTokenFactory().fromString(coordinator[1]);
                // grab any data we are now responsible for and notify responsible node
                restoreReplicaCount(endpoint, tokenMetadata_.getEndpoint(coordtoken));
            }
        } // not a member, nothing to do
    }

    private void excise(Token token, InetAddress endpoint)
    {
        HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
        Gossiper.instance.removeEndpoint(endpoint);
        tokenMetadata_.removeEndpoint(endpoint);
        tokenMetadata_.removeBootstrapToken(token);
        calculatePendingRanges();
        if (!isClientMode)
        {
            logger_.info("Removing token " + token + " for " + endpoint);
            SystemTable.removeToken(token);
        }
    }
    
    private void excise(Token token, InetAddress endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(token, endpoint);
    }
    
    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    protected long extractExpireTime(String[] pieces)
    {
        long expireTime = 0L;
        if (pieces.length >= 3)
        {
            expireTime = Long.parseLong(pieces[2]);
        }
        return expireTime;
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
        Multimap<Range, InetAddress> pendingRanges = HashMultimap.create();
        Map<Token, InetAddress> bootstrapTokens = tm.getBootstrapTokens();
        Set<InetAddress> leavingEndpoints = tm.getLeavingEndpoints();

        if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && tm.getMovingEndpoints().isEmpty())
        {
            if (logger_.isDebugEnabled())
                logger_.debug("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", table);
            tm.setPendingRanges(table, pendingRanges);
            return;
        }

        Multimap<InetAddress, Range> addressRanges = strategy.getAddressRanges();

        // Copy of metadata reflecting the situation after all leave operations are finished.
        TokenMetadata allLeftMetadata = tm.cloneAfterAllLeft();

        // get all ranges that will be affected by leaving nodes
        Set<Range> affectedRanges = new HashSet<Range>();
        for (InetAddress endpoint : leavingEndpoints)
            affectedRanges.addAll(addressRanges.get(endpoint));

        // for each of those ranges, find what new nodes will be responsible for the range when
        // all leaving nodes are gone.
        for (Range range : affectedRanges)
        {
            Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, tm));
            Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
            pendingRanges.putAll(range, Sets.difference(newEndpoints, currentEndpoints));
        }

        // At this stage pendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.

        // For each of the bootstrapping nodes, simply add and remove them one by one to
        // allLeftMetadata and check in between what their ranges would be.
        for (Map.Entry<Token, InetAddress> entry : bootstrapTokens.entrySet())
        {
            InetAddress endpoint = entry.getValue();

            allLeftMetadata.updateNormalToken(entry.getKey(), endpoint);
            for (Range range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
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

            for (Range range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
            {
                pendingRanges.put(range, endpoint);
            }

            allLeftMetadata.removeEndpoint(endpoint);
        }

        tm.setPendingRanges(table, pendingRanges);

        if (logger_.isDebugEnabled())
            logger_.debug("Pending ranges:\n" + (pendingRanges.isEmpty() ? "<empty>" : tm.printPendingRanges()));
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param table the table ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    private Multimap<InetAddress, Range> getNewSourceRanges(String table, Set<Range> ranges)
    {
        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        Multimap<Range, InetAddress> rangeAddresses = Table.open(table).getReplicationStrategy().getRangeAddresses(tokenMetadata_);
        Multimap<InetAddress, Range> sourceRanges = HashMultimap.create();
        IFailureDetector failureDetector = FailureDetector.instance;

        // find alive sources for our new ranges
        for (Range range : ranges)
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
     * @param local the local address
     * @param remote node to send notification to
     */
    private void sendReplicationNotification(InetAddress local, InetAddress remote)
    {
        // notify the remote token
        Message msg = new Message(local, StorageService.Verb.REPLICATION_FINISHED, new byte[0], Gossiper.instance.getVersion(remote));
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger_.isDebugEnabled())
            logger_.debug("Notifying " + remote.toString() + " of replication completion\n");
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
        Multimap<String, Map.Entry<InetAddress, Collection<Range>>> rangesToFetch = HashMultimap.create();

        final InetAddress myAddress = FBUtilities.getBroadcastAddress();

        for (String table : Schema.instance.getNonSystemTables())
        {
            Multimap<Range, InetAddress> changedRanges = getChangedRangesForLeaving(table, endpoint);
            Set<Range> myNewRanges = new HashSet<Range>();
            for (Map.Entry<Range, InetAddress> entry : changedRanges.entries())
            {
                if (entry.getValue().equals(myAddress))
                    myNewRanges.add(entry.getKey());
            }
            Multimap<InetAddress, Range> sourceRanges = getNewSourceRanges(table, myNewRanges);
            for (Map.Entry<InetAddress, Collection<Range>> entry : sourceRanges.asMap().entrySet())
            {
                fetchSources.put(entry.getKey(), table);
                rangesToFetch.put(table, entry);
            }
        }

        for (final String table : rangesToFetch.keySet())
        {
            for (Map.Entry<InetAddress, Collection<Range>> entry : rangesToFetch.get(table))
            {
                final InetAddress source = entry.getKey();
                Collection<Range> ranges = entry.getValue();
                final Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        synchronized (fetchSources)
                        {
                            fetchSources.remove(source, table);
                            if (fetchSources.isEmpty())
                                sendReplicationNotification(myAddress, notifyEndpoint);
                        }
                    }
                };
                if (logger_.isDebugEnabled())
                    logger_.debug("Requesting from " + source + " ranges " + StringUtils.join(ranges, ", "));
                StreamIn.requestRanges(source, table, ranges, callback, OperationType.RESTORE_REPLICA_COUNT);
            }
        }
    }

    // needs to be modified to accept either a table or ARS.
    private Multimap<Range, InetAddress> getChangedRangesForLeaving(String table, InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range> ranges = getRangesForEndpoint(table, endpoint);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " ranges [" + StringUtils.join(ranges, ", ") + "]");

        Map<Range, List<InetAddress>> currentReplicaEndpoints = new HashMap<Range, List<InetAddress>>();

        // Find (for each range) all nodes that store replicas for these ranges as well
        for (Range range : ranges)
            currentReplicaEndpoints.put(range, Table.open(table).getReplicationStrategy().calculateNaturalEndpoints(range.right, tokenMetadata_));

        TokenMetadata temp = tokenMetadata_.cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removetoken
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoints list, but
        // not in the currentReplicaEndpoints list, will be needing the
        // range.
        for (Range range : ranges)
        {
            Collection<InetAddress> newReplicaEndpoints = Table.open(table).getReplicationStrategy().calculateNaturalEndpoints(range.right, temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger_.isDebugEnabled())
                if (newReplicaEndpoints.isEmpty())
                    logger_.debug("Range " + range + " already in all replicas");
                else
                    logger_.debug("Range " + range + " will be responsibility of " + StringUtils.join(newReplicaEndpoints, ", "));
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
            deliverHints(endpoint);
    }

    public void onRemove(InetAddress endpoint)
    {
        tokenMetadata_.removeEndpoint(endpoint);
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

    /**
     * Deliver hints to the specified node when it has crashed
     * and come back up/ marked as alive after a network partition
    */
    public final void deliverHints(InetAddress endpoint)
    {
        HintedHandOffManager.instance.deliverHints(endpoint);
    }

    public final void deliverHints(String host) throws UnknownHostException
    {
        HintedHandOffManager.instance.deliverHints(host);
    }

    public Token getLocalToken()
    {
        Token token = SystemTable.getSavedToken();
        assert token != null; // should not be called before initServer sets this
        return token;
    }

    /* These methods belong to the MBean interface */

    public String getToken()
    {
        return getLocalToken().toString();
    }

    public String getReleaseVersion()
    {
        return FBUtilities.getReleaseVersionString();
    }

    public List<String> getLeavingNodes()
    {
        return stringify(tokenMetadata_.getLeavingEndpoints());
    }

    public List<String> getMovingNodes()
    {
        List<String> endpoints = new ArrayList<String>();

        for (Pair<Token, InetAddress> node : tokenMetadata_.getMovingEndpoints())
        {
            endpoints.add(node.right.getHostAddress());
        }

        return endpoints;
    }

    public List<String> getJoiningNodes()
    {
        return stringify(tokenMetadata_.getBootstrapTokens().values());
    }

    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    private static String getCanonicalPath(String filename)
    {
        try
        {
            return new File(filename).getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public String[] getAllDataFileLocations()
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        for (int i = 0; i < locations.length; i++)
            locations[i] = getCanonicalPath(locations[i]);
        return locations;
    }

    public String[] getAllDataFileLocationsForTable(String table)
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocationsForTable(table);
        for (int i = 0; i < locations.length; i++)
            locations[i] = getCanonicalPath(locations[i]);
        return locations;
    }

    public String getCommitLogLocation()
    {
        return getCanonicalPath(DatabaseDescriptor.getCommitLogLocation());
    }

    public String getSavedCachesLocation()
    {
        return getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation());
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
        if (tableName.equals(Table.SYSTEM_TABLE))
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

    public void invalidateKeyCaches(String tableName, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.invalidateKeyCache();
        }
    }

    public void invalidateRowCaches(String tableName, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            cfStore.invalidateRowCache();
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
            ArrayList<Table> t = new ArrayList<Table>();
            for (String table : tableNames)
                t.add(getValidTable(table));
            tables = t;
        }

        // Do a check to see if this snapshot exists before we actually snapshot
        for (Table table : tables)
            if (table.snapshotExists(tag))
                throw new IOException("Snapshot " + tag + " already exists.");


        for (Table table : tables)
            table.snapshot(tag);
    }

    private Table getValidTable(String tableName) throws IOException
    {
        if (!Schema.instance.getTables().contains(tableName))
        {
            throw new IOException("Table " + tableName + "does not exist");
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
            ArrayList<Table> tempTables = new ArrayList<Table>();
            for(String table : tableNames)
                tempTables.add(getValidTable(table));
            tables = tempTables;
        }

        for (Table table : tables)
            table.clearSnapshot(tag);

        if (logger_.isDebugEnabled())
            logger_.debug("Cleared out snapshot directories");
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
                logger_.warn(String.format("Invalid column family specified: %s. Proceeding with others.", cfName));
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
            logger_.debug("Forcing flush on keyspace " + tableName + ", CF " + cfStore.getColumnFamilyName());
            cfStore.forceBlockingFlush();
        }
    }

    /**
     * Trigger proactive repair for a table and column families.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableRepair(final String tableName, final String... columnFamilies) throws IOException
    {
        if (Table.SYSTEM_TABLE.equals(tableName))
            return;


        Collection<Range> ranges = getLocalRanges(tableName);
        int cmd = nextRepairCommand.incrementAndGet();
        logger_.info("Starting repair command #{}, repairing {} ranges.", cmd, ranges.size());

        List<AntiEntropyService.RepairFuture> futures = new ArrayList<AntiEntropyService.RepairFuture>(ranges.size());
        for (Range range : ranges)
        {
            AntiEntropyService.RepairFuture future = forceTableRepair(range, tableName, columnFamilies);
            futures.add(future);
            // wait for a session to be done with its differencing before starting the next one
            try
            {
                future.session.differencingDone.await();
            }
            catch (InterruptedException e)
            {
                logger_.error("Interrupted while waiting for the differencing of repair session " + future.session + " to be done. Repair may be imprecise.", e);
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
                logger_.error("Repair session " + future.session.getName() + " failed.", e);
                failedSession = true;
            }
        }

        if (failedSession)
            throw new IOException("Repair command #" + cmd + ": some repair session(s) failed (see log for details).");
        else
            logger_.info("Repair command #{} completed successfully", cmd);
    }

    public void forceTableRepairPrimaryRange(final String tableName, final String... columnFamilies) throws IOException
    {
        if (Table.SYSTEM_TABLE.equals(tableName))
            return;

        AntiEntropyService.RepairFuture future = forceTableRepair(getLocalPrimaryRange(), tableName, columnFamilies);
        try
        {
            future.get();
        }
        catch (Exception e)
        {
            logger_.error("Repair session " + future.session.getName() + " failed.", e);
            throw new IOException("Some repair session(s) failed (see log for details).");
        }
    }

    public AntiEntropyService.RepairFuture forceTableRepair(final Range range, final String tableName, final String... columnFamilies) throws IOException
    {
        ArrayList<String> names = new ArrayList<String>();
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            names.add(cfStore.getColumnFamilyName());
        }

        return AntiEntropyService.instance.submitRepairSession(range, tableName, names.toArray(new String[names.size()]));
    }

    public void forceTerminateAllRepairSessions() {
        AntiEntropyService.instance.terminateSessions();
    }

    /* End of MBean interface methods */

    /**
     * This method returns the predecessor of the endpoint ep on the identifier
     * space.
     */
    InetAddress getPredecessor(InetAddress ep)
    {
        Token token = tokenMetadata_.getToken(ep);
        return tokenMetadata_.getEndpoint(tokenMetadata_.getPredecessor(token));
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    public InetAddress getSuccessor(InetAddress ep)
    {
        Token token = tokenMetadata_.getToken(ep);
        return tokenMetadata_.getEndpoint(tokenMetadata_.getSuccessor(token));
    }

    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    public Range getPrimaryRangeForEndpoint(InetAddress ep)
    {
        return tokenMetadata_.getPrimaryRangeFor(tokenMetadata_.getToken(ep));
    }

    /**
     * Get all ranges an endpoint is responsible for (by table)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range> getRangesForEndpoint(String table, InetAddress ep)
    {
        return Table.open(table).getReplicationStrategy().getAddressRanges().get(ep);
    }

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range> getAllRanges(List<Token> sortedTokens)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("computing ranges for " + StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        List<Range> ranges = new ArrayList<Range>();
        int size = sortedTokens.size();
        for (int i = 1; i < size; ++i)
        {
            Range range = new Range(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range range = new Range(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param table keyspace name also known as table
     * @param cf Column family name
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String table, String cf, String key)
    {
        CFMetaData cfMetaData = Schema.instance.getTableDefinition(table).cfMetaData().get(cf);
        return getNaturalEndpoints(table, partitioner.getToken(cfMetaData.getKeyValidator().fromString(key)));
    }

    public List<InetAddress> getNaturalEndpoints(String table, ByteBuffer key)
    {
        return getNaturalEndpoints(table, partitioner.getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param token - token for which we need to find the endpoint return value -
     * the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(String table, Token token)
    {
        return Table.open(table).getReplicationStrategy().getNaturalEndpoints(token);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(String table, ByteBuffer key)
    {
        return getLiveNaturalEndpoints(table, partitioner.getToken(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(String table, Token token)
    {
        List<InetAddress> liveEps = new ArrayList<InetAddress>();
        List<InetAddress> endpoints = Table.open(table).getReplicationStrategy().getNaturalEndpoints(token);

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
        logger_.info("set log level to " + level + " for classes under '" + classQualifier + "' (if the level doesn't look like '" + rawLevel + "' then log4j couldn't parse '" + rawLevel + "')");
    }

    /**
     * @return list of Tokens (_not_ keys!) breaking up the data this node is responsible for into pieces of roughly keysPerSplit
     */
    public List<Token> getSplits(String table, String cfName, Range range, int keysPerSplit)
    {
        List<Token> tokens = new ArrayList<Token>();
        // we use the actual Range token for the first and last brackets of the splits to ensure correctness
        tokens.add(range.left);

        List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
        Table t = Table.open(table);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        for (DecoratedKey sample : cfs.allKeySamples())
        {
            if (range.contains(sample.token))
                keys.add(sample);
        }
        FBUtilities.sortSampledKeys(keys, range);
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

    /** return a token to which if a node bootstraps it will get about 1/2 of this node's range */
    public Token getBootstrapToken()
    {
        Range range = getLocalPrimaryRange();
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfs.table.name.equals(Table.SYSTEM_TABLE))
                continue;
            for (DecoratedKey key : cfs.allKeySamples())
            {
                if (range.contains(key.token))
                    keys.add(key);
            }
        }
        FBUtilities.sortSampledKeys(keys, range);

        Token token;
        if (keys.size() < 3)
        {
            token = partitioner.midpoint(range.left, range.right);
            logger_.debug("Used midpoint to assign token " + token);
        }
        else
        {
            token = keys.get(keys.size() / 2).token;
            logger_.debug("Used key sample of size " + keys.size() + " to assign token " + token);
        }
        if (tokenMetadata_.getEndpoint(token) != null && tokenMetadata_.isMember(tokenMetadata_.getEndpoint(token)))
            throw new RuntimeException("Chose token " + token + " which is already in use by " + tokenMetadata_.getEndpoint(token) + " -- specify one manually with initial_token");
        // Hack to prevent giving nodes tokens with DELIMITER_STR in them (which is fine in a row key/token)
        if (token instanceof StringToken)
        {
            token = new StringToken(((String)token.token).replaceAll(VersionedValue.DELIMITER_STR, ""));
            if (tokenMetadata_.getTokenToEndpointMap().containsKey(token))
                throw new RuntimeException("Unable to compute unique token for new node -- specify one manually with initial_token");
        }
        return token;
    }

    /**
     * Broadcast leaving status and update local tokenMetadata_ accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.leaving(getLocalToken()));
        tokenMetadata_.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
        calculatePendingRanges();
    }

    public void decommission() throws InterruptedException
    {
        if (!tokenMetadata_.isMember(FBUtilities.getBroadcastAddress()))
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
        if (tokenMetadata_.cloneAfterAllLeft().sortedTokens().size() < 2)
            throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
        for (String table : Schema.instance.getNonSystemTables())
        {
            if (tokenMetadata_.getPendingRanges(table, FBUtilities.getBroadcastAddress()).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        if (logger_.isDebugEnabled())
            logger_.debug("DECOMMISSIONING");
        startLeaving();
        setMode(Mode.LEAVING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
        Thread.sleep(RING_DELAY);

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
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
        SystemTable.setBootstrapped(false);
        tokenMetadata_.removeEndpoint(FBUtilities.getBroadcastAddress());
        calculatePendingRanges();

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.left(getLocalToken(),Gossiper.computeExpireTime()));
        logger_.info("Announcing that I have left the ring for " + RING_DELAY + "ms");
        try
        {
            Thread.sleep(RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    private void unbootstrap(final Runnable onFinish)
    {
        Map<String, Multimap<Range, InetAddress>> rangesToStream = new HashMap<String, Multimap<Range, InetAddress>>();

        for (final String table : Schema.instance.getNonSystemTables())
        {
            Multimap<Range, InetAddress> rangesMM = getChangedRangesForLeaving(table, FBUtilities.getBroadcastAddress());

            if (logger_.isDebugEnabled())
                logger_.debug("Ranges needing transfer are [" + StringUtils.join(rangesMM.keySet(), ",") + "]");

            rangesToStream.put(table, rangesMM);
        }

        setMode(Mode.LEAVING, "streaming data to other nodes", true);

        CountDownLatch latch = streamRanges(rangesToStream);

        // wait for the transfer runnables to signal the latch.
        logger_.debug("waiting for stream aks.");
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        logger_.debug("stream acks all received.");
        leaveRing();
        onFinish.run();
    }

    public void move(String newToken) throws IOException, InterruptedException, ConfigurationException
    {
        partitioner.getTokenFactory().validate(newToken);
        move(partitioner.getTokenFactory().fromString(newToken));
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

        if (tokenMetadata_.sortedTokens().contains(newToken))
            throw new IOException("target token " + newToken + " is already owned by another node.");

        // address of the current node
        InetAddress localAddress = FBUtilities.getBroadcastAddress();
        List<String> tablesToProcess = Schema.instance.getNonSystemTables();

        // checking if data is moving to this node
        for (String table : tablesToProcess)
        {
            if (tokenMetadata_.getPendingRanges(table, localAddress).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        // setting 'moving' application state
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.moving(newToken));

        logger_.info(String.format("Moving %s from %s to %s.", localAddress, getLocalToken(), newToken));

        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        Map<String, Multimap<InetAddress, Range>> rangesToFetch = new HashMap<String, Multimap<InetAddress, Range>>();
        Map<String, Multimap<Range, InetAddress>> rangesToStreamByTable = new HashMap<String, Multimap<Range, InetAddress>>();

        TokenMetadata tokenMetaClone = tokenMetadata_.cloneAfterAllSettled();

        // for each of the non system tables calculating new ranges
        // which current node will handle after move to the new token
        for (String table : tablesToProcess)
        {
            // replication strategy of the current keyspace (aka table)
            AbstractReplicationStrategy strategy = Table.open(table).getReplicationStrategy();

            // getting collection of the currently used ranges by this keyspace
            Collection<Range> currentRanges = getRangesForEndpoint(table, localAddress);
            // collection of ranges which this node will serve after move to the new token
            Collection<Range> updatedRanges = strategy.getPendingAddressRanges(tokenMetadata_, newToken, localAddress);

            // ring ranges and endpoints associated with them
            // this used to determine what nodes should we ping about range data
            Multimap<Range, InetAddress> rangeAddresses = strategy.getRangeAddresses(tokenMetadata_);

            // calculated parts of the ranges to request/stream from/to nodes in the ring
            Pair<Set<Range>, Set<Range>> rangesPerTable = calculateStreamAndFetchRanges(currentRanges, updatedRanges);

            /**
             * In this loop we are going through all ranges "to fetch" and determining
             * nodes in the ring responsible for data we are interested in
             */
            Multimap<Range, InetAddress> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
            for (Range toFetch : rangesPerTable.right)
            {
                for (Range range : rangeAddresses.keySet())
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
            Multimap<Range, InetAddress> rangeWithEndpoints = HashMultimap.create();

            for (Range toStream : rangesPerTable.left)
            {
                Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetadata_));
                Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaClone));
                rangeWithEndpoints.putAll(toStream, Sets.difference(newEndpoints, currentEndpoints));
            }

            // associating table with range-to-endpoints map
            rangesToStreamByTable.put(table, rangeWithEndpoints);

            Multimap<InetAddress, Range> workMap = BootStrapper.getWorkMap(rangesToFetchWithPreferredEndpoints);
            rangesToFetch.put(table, workMap);

            if (logger_.isDebugEnabled())
                logger_.debug("Table {}: work map {}.", table, workMap);
        }

        if (!rangesToStreamByTable.isEmpty() || !rangesToFetch.isEmpty())
        {
            logger_.info("Sleeping {} ms before start streaming/fetching ranges.", RING_DELAY);

            try
            {
                Thread.sleep(RING_DELAY);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Sleep interrupted " + e.getMessage());
            }

            setMode(Mode.MOVING, "fetching new ranges and streaming old ranges", true);

            if (logger_.isDebugEnabled())
                logger_.debug("[Move->STREAMING] Work Map: " + rangesToStreamByTable);

            CountDownLatch streamLatch = streamRanges(rangesToStreamByTable);

            if (logger_.isDebugEnabled())
                logger_.debug("[Move->FETCHING] Work Map: " + rangesToFetch);

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

        setToken(newToken); // setting new token as we have everything settled

        if (logger_.isDebugEnabled())
            logger_.debug("Successfully moved to new token {}", getLocalToken());
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
                             tokenMetadata_.getToken(removingNode),
                             StringUtils.join(replicatingNodes, ","));
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
        if (!replicatingNodes.isEmpty()  || !tokenMetadata_.getLeavingEndpoints().isEmpty())
        {
            logger_.warn("Removal not confirmed for for " + StringUtils.join(this.replicatingNodes, ","));
            for (InetAddress endpoint : tokenMetadata_.getLeavingEndpoints())
            {
                Gossiper.instance.advertiseTokenRemoved(endpoint, tokenMetadata_.getToken(endpoint));
                tokenMetadata_.removeEndpoint(endpoint);
            }
            replicatingNodes.clear();
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
     * @param tokenString token for the node
     */
    public void removeToken(String tokenString)
    {
        removeToken(tokenString, RING_DELAY);
    }

    public void removeToken(String tokenString, int delay)
    {
        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        Token localToken = tokenMetadata_.getToken(myAddress);
        Token token = partitioner.getTokenFactory().fromString(tokenString);
        InetAddress endpoint = tokenMetadata_.getEndpoint(token);

        if (endpoint == null)
            throw new UnsupportedOperationException("Token not found.");

        if (endpoint.equals(myAddress))
             throw new UnsupportedOperationException("Cannot remove node's own token");

        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this token. Use decommission command to remove it from the ring");

        // A leaving endpoint that is dead is already being removed.
        if (tokenMetadata_.isLeaving(endpoint))
            logger_.warn("Node " + endpoint + " is already being removed, continuing removal anyway");

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
            Multimap<Range, InetAddress> changedRanges = getChangedRangesForLeaving(table, endpoint);
            IFailureDetector failureDetector = FailureDetector.instance;
            for (InetAddress ep : changedRanges.values())
            {
                if (failureDetector.isAlive(ep))
                    replicatingNodes.add(ep);
                else
                    logger_.warn("Endpoint " + ep + " is down and will not receive data for re-replication of " + endpoint);
            }
        }
        removingNode = endpoint;

        tokenMetadata_.addLeavingEndpoint(endpoint);
        calculatePendingRanges();
        // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
        // we add our own token so other nodes to let us know when they're done
        Gossiper.instance.advertiseRemoving(endpoint, token, localToken, delay);

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

        excise(token, endpoint);

        // gossiper will indicate the token has left
        Gossiper.instance.advertiseTokenRemoved(endpoint, token);

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
            logger_.info("Received unexpected REPLICATION_FINISHED message from " + node
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
            logger_.info("requesting GC to free disk space");
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

    /** shuts node off to writes, empties memtables and the commit log. */
    public synchronized void drain() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);
        if (mutationStage.isTerminated())
        {
            logger_.warn("Cannot drain node (did it already happen?)");
            return;
        }
        setMode(Mode.DRAINING, "starting drain process", true);
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
            logger_.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");

        setMode(Mode.DRAINED, true);
    }

    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = partitioner;
        partitioner = newPartitioner;
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd)
    {
        TokenMetadata old = tokenMetadata_;
        tokenMetadata_ = tmd;
        return old;
    }

    public void truncate(String keyspace, String columnFamily) throws UnavailableException, TimeoutException, IOException
    {
        StorageProxy.truncateBlocking(keyspace, columnFamily);
    }

    public void saveCaches() throws ExecutionException, InterruptedException
    {
        List<Future<?>> futures = new ArrayList<Future<?>>();
        logger_.debug("submitting cache saves");
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            futures.add(cfs.keyCache.submitWrite(-1));
            futures.add(cfs.rowCache.submitWrite(cfs.getRowCacheKeysToSave()));
        }
        FBUtilities.waitOnFutures(futures);
        logger_.debug("cache saves completed");
    }

    public Map<Token, Float> getOwnership()
    {
        List<Token> sortedTokens = new ArrayList<Token>(getTokenToEndpointMap().keySet());
        Collections.sort(sortedTokens);
        return partitioner.describeOwnership(sortedTokens);
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
                logger_.debug(total + " estimated memtable size for " + cfs);
                largest = cfs;
            }
        }
        if (largest == null)
        {
            logger_.info("Unable to reduce heap usage since there are no dirty column families");
            return;
        }

        logger_.warn("Flushing " + largest + " to relieve memory pressure");
        largest.forceFlush();
    }

    public void reduceCacheSizes()
    {
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            cfs.reduceCacheSizes();
    }

    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByTable tables and data ranges with endpoints included for each
     * @return latch to count down
     */
    private CountDownLatch streamRanges(final Map<String, Multimap<Range, InetAddress>> rangesToStreamByTable)
    {
        final CountDownLatch latch = new CountDownLatch(rangesToStreamByTable.keySet().size());
        for (final String table : rangesToStreamByTable.keySet())
        {
            Multimap<Range, InetAddress> rangesWithEndpoints = rangesToStreamByTable.get(table);

            if (rangesWithEndpoints.isEmpty())
            {
                latch.countDown();
                continue;
            }

            final Set<Map.Entry<Range, InetAddress>> pending = new HashSet<Map.Entry<Range, InetAddress>>(rangesWithEndpoints.entries());

            for (final Map.Entry<Range, InetAddress> entry : rangesWithEndpoints.entries())
            {
                final Range range = entry.getKey();
                final InetAddress newEndpoint = entry.getValue();

                final Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        synchronized (pending)
                        {
                            pending.remove(entry);

                            if (pending.isEmpty())
                                latch.countDown();
                        }
                    }
                };

                StageManager.getStage(Stage.STREAM).execute(new Runnable()
                {
                    public void run()
                    {
                        // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                        StreamOut.transferRanges(newEndpoint, Table.open(table), Arrays.asList(range), callback, OperationType.UNBOOTSTRAP);
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
    private CountDownLatch requestRanges(final Map<String, Multimap<InetAddress, Range>> ranges)
    {
        final CountDownLatch latch = new CountDownLatch(ranges.keySet().size());
        for (final String table : ranges.keySet())
        {
            Multimap<InetAddress, Range> endpointWithRanges = ranges.get(table);

            if (endpointWithRanges.isEmpty())
            {
                latch.countDown();
                continue;
            }

            final Set<InetAddress> pending = new HashSet<InetAddress>(endpointWithRanges.keySet());

            // Send messages to respective folks to stream data over to me
            for (final InetAddress source: endpointWithRanges.keySet())
            {
                Collection<Range> toFetch = endpointWithRanges.get(source);

                final Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        pending.remove(source);

                        if (pending.isEmpty())
                            latch.countDown();
                    }
                };

                if (logger_.isDebugEnabled())
                    logger_.debug("Requesting from " + source + " ranges " + StringUtils.join(toFetch, ", "));

                // sending actual request
                StreamIn.requestRanges(source, table, toFetch, callback, OperationType.BOOTSTRAP);
            }
        }
        return latch;
    }

    // see calculateStreamAndFetchRanges(Iterator, Iterator) for description
    private Pair<Set<Range>, Set<Range>> calculateStreamAndFetchRanges(Collection<Range> current, Collection<Range> updated)
    {
        return calculateStreamAndFetchRanges(current.iterator(), updated.iterator());
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for table and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    private Pair<Set<Range>, Set<Range>> calculateStreamAndFetchRanges(Iterator<Range> current, Iterator<Range> updated)
    {
        Set<Range> toStream = new HashSet<Range>();
        Set<Range> toFetch  = new HashSet<Range>();

        while (current.hasNext() && updated.hasNext())
        {
            Range r1 = current.next();
            Range r2 = updated.next();

            // if ranges intersect we need to fetch only missing part
            if (r1.intersects(r2))
            {
                // adding difference ranges to fetch from a ring
                toFetch.addAll(r1.differenceToFetch(r2));

                // if current range is a sub-range of a new range we don't need to seed
                // otherwise we need to seed parts of the current range
                if (!r2.contains(r1))
                {
                    // (A, B] & (C, D]
                    if (Range.compare(r1.left, r2.left) < 0) // if A < C
                    {
                        toStream.add(new Range(r1.left, r2.left)); // seed (A, C]
                    }

                    if (Range.compare(r1.right, r2.right) > 0) // if B > D
                    {
                        toStream.add(new Range(r2.right, r1.right)); // seed (D, B]
                    }
                }
            }
            else // otherwise we need to fetch whole new range
            {
                toStream.add(r1); // should seed whole old range
                toFetch.add(r2);
            }
        }

        return new Pair<Set<Range>, Set<Range>>(toStream, toFetch);
    }

    public void bulkLoad(String directory)
    {
        File dir = new File(directory);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Invalid directory " + directory);

        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Map.Entry<Range, List<InetAddress>> entry : StorageService.instance.getRangeToAddressMap(keyspace).entrySet())
                {
                    Range range = entry.getKey();
                    for (InetAddress endpoint : entry.getValue())
                        addRangeForEndpoint(range, endpoint);
                }
            }

            public boolean validateColumnFamily(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName) != null;
            }
        };

        SSTableLoader.OutputHandler oh = new SSTableLoader.OutputHandler()
        {
            public void output(String msg) { logger_.info(msg); }
            public void debug(String msg) { logger_.debug(msg); }
        };

        SSTableLoader loader = new SSTableLoader(dir, client, oh);
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
        return AbstractCassandraDaemon.exceptions.get();
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
}
