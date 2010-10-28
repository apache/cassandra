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

import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.RetryingScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.RawColumnDefinition;
import org.apache.cassandra.config.RawColumnFamily;
import org.apache.cassandra.config.RawKeyspace;
import org.apache.cassandra.db.BinaryVerbHandler;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DefinitionsAnnounceVerbHandler;
import org.apache.cassandra.db.DefinitionsUpdateResponseVerbHandler;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.ReadVerbHandler;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutationVerbHandler;
import org.apache.cassandra.db.SchemaCheckVerbHandler;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TruncateVerbHandler;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.GossipDigestAck2VerbHandler;
import org.apache.cassandra.gms.GossipDigestAckVerbHandler;
import org.apache.cassandra.gms.GossipDigestSynVerbHandler;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.service.AntiEntropyService.TreeRequestVerbHandler;
import org.apache.cassandra.streaming.ReplicationFinishedVerbHandler;
import org.apache.cassandra.streaming.StreamIn;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.streaming.StreamReplyVerbHandler;
import org.apache.cassandra.streaming.StreamRequestVerbHandler;
import org.apache.cassandra.streaming.StreamingService;
import org.apache.cassandra.thrift.Constants;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SkipNullRepresenter;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Dumper;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
        BINARY,
        READ_REPAIR,
        READ,
        READ_RESPONSE,
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
        DEFINITIONS_ANNOUNCE,
        DEFINITIONS_UPDATE_RESPONSE,
        TRUNCATE,
        SCHEMA_CHECK,
        INDEX_SCAN,
        REPLICATION_FINISHED,
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
        put(Verb.READ_RESPONSE, Stage.RESPONSE);
        put(Verb.STREAM_REPLY, Stage.MISC); // TODO does this really belong on misc? I've just copied old behavior here
        put(Verb.STREAM_REQUEST, Stage.STREAM);
        put(Verb.RANGE_SLICE, Stage.READ);
        put(Verb.BOOTSTRAP_TOKEN, Stage.MISC);
        put(Verb.TREE_REQUEST, Stage.ANTIENTROPY);
        put(Verb.TREE_RESPONSE, Stage.ANTIENTROPY);
        put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
        put(Verb.DEFINITIONS_ANNOUNCE, Stage.READ);
        put(Verb.DEFINITIONS_UPDATE_RESPONSE, Stage.READ);
        put(Verb.TRUNCATE, Stage.MUTATION);
        put(Verb.SCHEMA_CHECK, Stage.MIGRATION);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
    }};


    private static IPartitioner partitioner_ = DatabaseDescriptor.getPartitioner();
    public static VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner_);

    public static RetryingScheduledThreadPoolExecutor scheduledTasks = new RetryingScheduledThreadPoolExecutor("ScheduledTasks");

    public static final StorageService instance = new StorageService();

    public static IPartitioner getPartitioner() {
        return partitioner_;
    }

    public Collection<Range> getLocalRanges(String table)
    {
        return getRangesForEndpoint(table, FBUtilities.getLocalAddress());
    }

    public Range getLocalPrimaryRange()
    {
        return getPrimaryRangeForEndpoint(FBUtilities.getLocalAddress());
    }

    /* This abstraction maintains the token/endpoint metadata information */
    private TokenMetadata tokenMetadata_ = new TokenMetadata();

    /* This thread pool does consistency checks when the client doesn't care about consistency */
    private ExecutorService consistencyManager_ = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getConsistencyThreads(),
                                                                                   DatabaseDescriptor.getConsistencyThreads(),
                                                                                   StageManager.KEEPALIVE,
                                                                                   TimeUnit.SECONDS,
                                                                                   new LinkedBlockingQueue<Runnable>(),
                                                                                   new NamedThreadFactory("ReadRepair"),
                                                                                   "request");

    private Set<InetAddress> replicatingNodes;
    private InetAddress removingNode;

    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    /* when intialized as a client, we shouldn't write to the system table. */
    private boolean isClientMode;
    private boolean initialized;
    private String operationMode;
    private MigrationManager migrationManager = new MigrationManager();

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    public void finishBootstrapping()
    {
        isBootstrapMode = false;
        SystemTable.setBootstrapped(true);
        setToken(getLocalToken());
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.normal(getLocalToken()));
        logger_.info("Bootstrap/move completed! Now serving reads.");
        setMode("Normal", false);
    }

    /** This method updates the local token on disk  */
    public void setToken(Token token)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Setting token to {}", token);
        SystemTable.updateToken(token);
        tokenMetadata_.updateNormalToken(token, FBUtilities.getLocalAddress());
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
        MessagingService.instance.registerVerbHandlers(Verb.BINARY, new BinaryVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.MUTATION, new RowMutationVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.READ_REPAIR, new ReadRepairVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.READ, new ReadVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.RANGE_SLICE, new RangeSliceVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.INDEX_SCAN, new IndexScanVerbHandler());
        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance.registerVerbHandlers(Verb.BOOTSTRAP_TOKEN, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.STREAM_REQUEST, new StreamRequestVerbHandler() );
        MessagingService.instance.registerVerbHandlers(Verb.STREAM_REPLY, new StreamReplyVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.REPLICATION_FINISHED, new ReplicationFinishedVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.READ_RESPONSE, new ResponseVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.TREE_REQUEST, new TreeRequestVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.TREE_RESPONSE, new AntiEntropyService.TreeResponseVerbHandler());

        MessagingService.instance.registerVerbHandlers(Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler());
        
        MessagingService.instance.registerVerbHandlers(Verb.DEFINITIONS_ANNOUNCE, new DefinitionsAnnounceVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.DEFINITIONS_UPDATE_RESPONSE, new DefinitionsUpdateResponseVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.TRUNCATE, new TruncateVerbHandler());
        MessagingService.instance.registerVerbHandlers(Verb.SCHEMA_CHECK, new SchemaCheckVerbHandler());

        // spin up the streaming serivice so it is available for jmx tools.
        if (StreamingService.instance == null)
            throw new RuntimeException("Streaming service is unavailable.");
    }

    public void stopClient()
    {
        Gossiper.instance.unregister(migrationManager);
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.shutdown();
        StageManager.shutdownNow();
    }

    public synchronized void initClient() throws IOException
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
        setMode("Client", false);
        Gossiper.instance.register(this);
        Gossiper.instance.start(FBUtilities.getLocalAddress(), (int)(System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        MessagingService.instance.listen(FBUtilities.getLocalAddress());
        
        // sleep a while to allow gossip to warm up (the other nodes need to know about this one before they can reply).
        try
        {
            Thread.sleep(5000L);
        }
        catch (Exception ex)
        {
            throw new IOError(ex);
        }
        MigrationManager.announce(DatabaseDescriptor.getDefsVersion(), DatabaseDescriptor.getSeeds());
    }

    public synchronized void initServer() throws IOException, org.apache.cassandra.config.ConfigurationException
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

        try
        {
            GCInspector.instance.start();
        }
        catch (Throwable t)
        {
            logger_.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
        }

        if (Boolean.valueOf(System.getProperty("cassandra.load_ring_state", "true")))
        {
            logger_.info("Loading persisted ring state");
            for (Map.Entry<Token, InetAddress> entry : SystemTable.loadTokens().entrySet())
            {
                tokenMetadata_.updateNormalToken(entry.getKey(), entry.getValue());
                Gossiper.instance.addSavedEndpoint(entry.getValue());
            }
        }

        logger_.info("Starting up server gossip");

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a nodeId to our state, below.)
        Gossiper.instance.register(this);
        Gossiper.instance.register(migrationManager);
        Gossiper.instance.start(FBUtilities.getLocalAddress(), SystemTable.incrementAndGetGeneration()); // needed for node-ring gathering.

        MessagingService.instance.listen(FBUtilities.getLocalAddress());
        StorageLoadBalancer.instance.startBroadcasting();
        MigrationManager.announce(DatabaseDescriptor.getDefsVersion(), DatabaseDescriptor.getSeeds());

        if (DatabaseDescriptor.isAutoBootstrap()
                && DatabaseDescriptor.getSeeds().contains(FBUtilities.getLocalAddress())
                && !SystemTable.isBootstrapped())
            logger_.info("This node will not auto bootstrap because it is configured to be a seed node.");

        if (DatabaseDescriptor.isAutoBootstrap()
            && !(DatabaseDescriptor.getSeeds().contains(FBUtilities.getLocalAddress()) || SystemTable.isBootstrapped()))
        {
            setMode("Joining: getting load information", true);
            StorageLoadBalancer.instance.waitForLoadInfo();
            if (logger_.isDebugEnabled())
                logger_.debug("... got load info");
            if (tokenMetadata_.isMember(FBUtilities.getLocalAddress()))
            {
                String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                throw new UnsupportedOperationException(s);
            }
            setMode("Joining: getting bootstrap token", true);
            Token token = BootStrapper.getBootstrapToken(tokenMetadata_, StorageLoadBalancer.instance.getLoadInfo());
            // don't bootstrap if there are no tables defined.
            if (DatabaseDescriptor.getNonSystemTables().size() > 0)
            {
                bootstrap(token);
                assert !isBootstrapMode; // bootstrap will block until finished
            }
            else
            {
                isBootstrapMode = false;
                SystemTable.setBootstrapped(true);
                tokenMetadata_.updateNormalToken(token, FBUtilities.getLocalAddress());
                Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.normal(token));
                setMode("Normal", false);
            }
        }
        else
        {
            Token token = SystemTable.getSavedToken();
            if (token == null)
            {
                String initialToken = DatabaseDescriptor.getInitialToken();
                if (initialToken == null)
                {
                    token = partitioner_.getRandomToken();
                    logger_.warn("Generated random token " + token + ". Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations");
                }
                else
                {
                    token = partitioner_.getTokenFactory().fromString(initialToken);
                    logger_.info("Saved token not found. Using " + token + " from configuration");
                }
                SystemTable.updateToken(token);
            }
            else
            {
                logger_.info("Using saved token " + token);
            }
            SystemTable.setBootstrapped(true);
            tokenMetadata_.updateNormalToken(token, FBUtilities.getLocalAddress());
            Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.normal(token));
            setMode("Normal", false);
        } 
        
        assert tokenMetadata_.sortedTokens().size() > 0;
    }

    private void setMode(String m, boolean log)
    {
        operationMode = m;
        if (log)
            logger_.info(m);
    }

    private void bootstrap(Token token) throws IOException
    {
        isBootstrapMode = true;
        SystemTable.updateToken(token); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.bootstrapping(token));
        setMode("Joining: sleeping " + RING_DELAY + " ms for pending range setup", true);
        try
        {
            Thread.sleep(RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        setMode("Bootstrapping", true);
        new BootStrapper(FBUtilities.getLocalAddress(), token, tokenMetadata_).bootstrap(); // handles token update
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
     * This method performs the requisite operations to make
     * sure that the N replicas are in sync. We do this in the
     * background when we do not care much about consistency.
     */
    public void doConsistencyCheck(Row row, List<InetAddress> endpoints, ReadCommand command)
    {
        consistencyManager_.submit(new ConsistencyChecker(command.table, row, endpoints, command));
    }

    /**
     * for a keyspace, return the ranges and corresponding hosts for a given keyspace.
     * @param keyspace
     * @return
     */
    public Map<Range, List<String>> getRangeToEndpointMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
            keyspace = DatabaseDescriptor.getNonSystemTables().get(0);

        /* All the ranges for the tokens */
        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        for (Map.Entry<Range,List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey(), stringify(entry.getValue()));
        }
        return map;
    }

    public Map<Range, List<String>> getPendingRangeToEndpointMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system table.
        if (keyspace == null)
            keyspace = DatabaseDescriptor.getNonSystemTables().get(0);

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
        List<Range> ranges = getAllRanges(tokenMetadata_.sortedTokens());
        return constructRangeToEndpointMap(keyspace, ranges);
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
            rangeToEndpointMap.put(range, Table.open(keyspace).replicationStrategy.getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
    }

    /*
     * onChange only ever sees one ApplicationState piece change at a time, so we perform a kind of state machine here.
     * We are concerned with two events: knowing the token associated with an endpoint, and knowing its operation mode.
     * Nodes can start in either bootstrap or normal mode, and from bootstrap mode can change mode to normal.
     * A node in bootstrap mode needs to have pendingranges set in TokenMetadata; a node in normal mode
     * should instead be part of the token ring.
     * 
     * Normal MOVE_STATE progression of a node should be like this:
     * STATE_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATE_NORMAL,token 
     *   ready to serve reads and writes.
     * STATE_NORMAL,token,REMOVE_TOKEN,token
     *   specialized normal state in which this node acts as a proxy to tell the cluster about a dead node whose 
     *   token is being removed. this value becomes the permanent state of this node (unless it coordinates another
     *   removetoken in the future).
     * STATE_LEAVING,token 
     *   get ready to leave the cluster as part of a decommission or move
     * STATE_LEFT,token 
     *   set after decommission or move is completed.
     * 
     * Note: Any time a node state changes from STATE_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removetoken, decommission or move.
     */
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.STATUS)
            return;

        String apStateValue = value.value;
        String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);

        String moveName = pieces[0];

        if (moveName.equals(VersionedValue.STATUS_BOOTSTRAPPING))
            handleStateBootstrap(endpoint, pieces);
        else if (moveName.equals(VersionedValue.STATUS_NORMAL))
            handleStateNormal(endpoint, pieces);
        else if (moveName.equals(VersionedValue.STATUS_LEAVING))
            handleStateLeaving(endpoint, pieces);
        else if (moveName.equals(VersionedValue.STATUS_LEFT))
            handleStateLeft(endpoint, pieces);
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     * @param pieces STATE_BOOTSTRAPPING,bootstrap token as string
     */
    private void handleStateBootstrap(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length == 2;
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
     * @param pieces STATE_NORMAL,token[,other_state,token]
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
        InetAddress currentNode = tokenMetadata_.getEndpoint(token);
        if (currentNode == null)
        {
            logger_.debug("New node " + endpoint + " at token " + token);
            tokenMetadata_.updateNormalToken(token, endpoint);
            if (!isClientMode)
                SystemTable.updateToken(endpoint, token);
        }
        else if (endpoint.equals(currentNode))
        {
            // nothing to do
        }
        else if (Gossiper.instance.compareEndpointStartup(endpoint, currentNode) > 0)
        {
            logger_.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner",
                                       endpoint, currentNode, token, endpoint));
            tokenMetadata_.updateNormalToken(token, endpoint);
            if (!isClientMode)
                SystemTable.updateToken(endpoint, token);
        }
        else
        {
            logger_.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s",
                                       endpoint, currentNode, token, endpoint));
        }

        if (pieces.length > 2)
        {
            assert pieces.length == 4;
            handleStateRemoving(endpoint, getPartitioner().getTokenFactory().fromString(pieces[3]), pieces[2]);
        }

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
        assert pieces.length == 2;
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
     * Handle node leaving the ring. This can be either because of decommission or loadbalance
     *
     * @param endpoint If reason for leaving is decommission or loadbalance
     * endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length == 2;
        Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " state left, token " + token);

        excise(token, endpoint);
    }

    /**
     * Handle node being actively removed from the ring.
     *
     * @param endpoint node
     */
    private void handleStateRemoving(InetAddress endpoint, Token removeToken, String state)
    {
        InetAddress removeEndpoint = tokenMetadata_.getEndpoint(removeToken);
        
        if (removeEndpoint == null)
            return;
        
        if (removeEndpoint.equals(FBUtilities.getLocalAddress()))
        {
            logger_.info("Received removeToken gossip about myself. Is this node a replacement for a removed one?");
            return;
        }

        if (VersionedValue.REMOVED_TOKEN.equals(state))
        {
            excise(removeToken, removeEndpoint);
        }
        else if (VersionedValue.REMOVING_TOKEN.equals(state))
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Token " + removeToken + " removed manually (endpoint was " + removeEndpoint + ")");

            // Note that the endpoint is being removed
            tokenMetadata_.addLeavingEndpoint(removeEndpoint);
            calculatePendingRanges();

            // grab any data we are now responsible for and notify responsible node
            restoreReplicaCount(removeEndpoint, endpoint);
        }
    }

    private void excise(Token token, InetAddress endpoint)
    {
        Gossiper.instance.removeEndpoint(endpoint);
        tokenMetadata_.removeEndpoint(endpoint);
        HintedHandOffManager.deleteHintsForEndPoint(endpoint);
        tokenMetadata_.removeBootstrapToken(token);
        calculatePendingRanges();
        if (!isClientMode)
        {
            logger_.info("Removing token " + token + " for " + endpoint);
            SystemTable.removeToken(token);
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
        for (String table : DatabaseDescriptor.getNonSystemTables())
            calculatePendingRanges(Table.open(table).replicationStrategy, table);
    }

    // public & static for testing purposes
    public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String table)
    {
        TokenMetadata tm = StorageService.instance.getTokenMetadata();
        Multimap<Range, InetAddress> pendingRanges = HashMultimap.create();
        Map<Token, InetAddress> bootstrapTokens = tm.getBootstrapTokens();
        Set<InetAddress> leavingEndpoints = tm.getLeavingEndpoints();

        if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty())
        {
            if (logger_.isDebugEnabled())
                logger_.debug("No bootstrapping or leaving nodes -> empty pending ranges for {}", table);
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
            Collection<InetAddress> currentEndpoints = strategy.calculateNaturalEndpoints(range.right, tm);
            Collection<InetAddress> newEndpoints = strategy.calculateNaturalEndpoints(range.right, allLeftMetadata);
            newEndpoints.removeAll(currentEndpoints);
            pendingRanges.putAll(range, newEndpoints);
        }

        // At this stage pendingRanges has been updated according to leave operations. We can
        // now finish the calculation by checking bootstrapping nodes.

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
        InetAddress myAddress = FBUtilities.getLocalAddress();
        Multimap<Range, InetAddress> rangeAddresses = Table.open(table).replicationStrategy.getRangeAddresses(tokenMetadata_);
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
        Message msg = new Message(local, StorageService.Verb.REPLICATION_FINISHED, new byte[0]);
        IFailureDetector failureDetector = FailureDetector.instance;
        while (failureDetector.isAlive(remote))
        {
            IAsyncResult iar = MessagingService.instance.sendRR(msg, remote);
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

        final InetAddress myAddress = FBUtilities.getLocalAddress();

        for (String table : DatabaseDescriptor.getNonSystemTables())
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
                StreamIn.requestRanges(source, table, ranges, callback);
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
            currentReplicaEndpoints.put(range, Table.open(table).replicationStrategy.calculateNaturalEndpoints(range.right, tokenMetadata_));

        TokenMetadata temp = tokenMetadata_.cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removetoken
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoins list, but
        // not in the currentReplicaEndpoins list, will be needing the
        // range.
        for (Range range : ranges)
        {
            Collection<InetAddress> newReplicaEndpoints = Table.open(table).replicationStrategy.calculateNaturalEndpoints(range.right, temp);
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
        if (!isClientMode)
            deliverHints(endpoint);
    }

    public void onRemove(InetAddress endpoint)
    {
        tokenMetadata_.removeEndpoint(endpoint);
        calculatePendingRanges();
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        MessagingService.instance.convict(endpoint);
    }

    /** raw load value */
    public double getLoad()
    {
        double bytes = 0;
        for (String tableName : DatabaseDescriptor.getTables())
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
        for (Map.Entry<InetAddress,Double> entry : StorageLoadBalancer.instance.getLoadInfo().entrySet())
        {
            map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(FBUtilities.getLocalAddress().getHostAddress(), getLoadString());
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
        return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getLocalAddress());
    }

    public void forceTableCleanup() throws IOException, ExecutionException, InterruptedException
    {
        List<String> tables = DatabaseDescriptor.getNonSystemTables();
        for (String tName : tables)
        {
            Table table = Table.open(tName);
            table.forceCleanup();
        }
    }

    public void forceTableCleanup(String tableName) throws IOException, ExecutionException, InterruptedException
    {
        Table table = getValidTable(tableName);
        table.forceCleanup();
    }

    public void forceTableCompaction() throws IOException, ExecutionException, InterruptedException
    {
        for (Table table : Table.all())
            table.forceCompaction();
    }

    public void forceTableCompaction(String tableName) throws IOException, ExecutionException, InterruptedException
    {
        Table table = getValidTable(tableName);
        table.forceCompaction();
    }

    /**
     * Takes the snapshot for a given table.
     *
     * @param tableName the name of the table.
     * @param tag   the tag given to the snapshot (null is permissible)
     */
    public void takeSnapshot(String tableName, String tag) throws IOException
    {
        Table tableInstance = getValidTable(tableName);
        tableInstance.snapshot(tag);
    }

    private Table getValidTable(String tableName) throws IOException
    {
        if (!DatabaseDescriptor.getTables().contains(tableName))
        {
            throw new IOException("Table " + tableName + "does not exist");
        }
        return Table.open(tableName);
    }

    /**
     * Takes a snapshot for every table.
     *
     * @param tag the tag given to the snapshot (null is permissible)
     */
    public void takeAllSnapshot(String tag) throws IOException
    {
        for (Table table : Table.all())
            table.snapshot(tag);
    }

    /**
     * Remove all the existing snapshots.
     */
    public void clearSnapshot() throws IOException
    {
        for (Table table : Table.all())
            table.clearSnapshot();

        if (logger_.isDebugEnabled())
            logger_.debug("Cleared out all snapshot directories");
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
            logger_.debug("Forcing binary flush on keyspace " + tableName + ", CF " + cfStore.getColumnFamilyName());
            cfStore.forceFlushBinary();
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
        String[] families;
        if (columnFamilies.length == 0)
        {
            ArrayList<String> names = new ArrayList<String>();
            for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName)) {
                names.add(cfStore.getColumnFamilyName());
            }
            families = names.toArray(new String[] {});
        }
        else
        {
            families = columnFamilies;
        }
        AntiEntropyService.RepairSession sess = AntiEntropyService.instance.getRepairSession(tableName, families);
        
        try
        {
            sess.start();
            // block until the repair has completed
            sess.join();
        }
        catch (InterruptedException e)
        {
            throw new IOException("Repair session " + sess + " failed.", e);
        }
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
     * Get all ranges an endpoint is responsible for.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range> getRangesForEndpoint(String table, InetAddress ep)
    {
        return Table.open(table).replicationStrategy.getAddressRanges().get(ep);
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
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String table, ByteBuffer key)
    {
        return getNaturalEndpoints(table, partitioner_.getToken(key));
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
        return Table.open(table).replicationStrategy.getNaturalEndpoints(token);
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
        return getLiveNaturalEndpoints(table, partitioner_.getToken(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(String table, Token token)
    {
        List<InetAddress> liveEps = new ArrayList<InetAddress>();
        List<InetAddress> endpoints = Table.open(table).replicationStrategy.getNaturalEndpoints(token);

        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    /**
     * This function finds the closest live endpoint that contains a given key.
     */
    public InetAddress findSuitableEndpoint(String table, ByteBuffer key) throws IOException, UnavailableException
    {
        List<InetAddress> endpoints = getNaturalEndpoints(table, key);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);
        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                return endpoint;
        }
        throw new UnavailableException(); // no nodes that could contain key are alive
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
            for (DecoratedKey key : cfs.allKeySamples())
            {
                if (range.contains(key.token))
                    keys.add(key);
            }
        }
        FBUtilities.sortSampledKeys(keys, range);

        if (keys.size() < 3)
            return partitioner_.midpoint(range.left, range.right);
        else
            return keys.get(keys.size() / 2).token;
    }

    /**
     * Broadcast leaving status and update local tokenMetadata_ accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.leaving(getLocalToken()));
        tokenMetadata_.addLeavingEndpoint(FBUtilities.getLocalAddress());
        calculatePendingRanges();
    }

    public void decommission() throws InterruptedException
    {
        if (!tokenMetadata_.isMember(FBUtilities.getLocalAddress()))
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
        if (tokenMetadata_.cloneAfterAllLeft().sortedTokens().size() < 2)
            throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            if (tokenMetadata_.getPendingRanges(table, FBUtilities.getLocalAddress()).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        if (logger_.isDebugEnabled())
            logger_.debug("DECOMMISSIONING");
        startLeaving();
        setMode("Leaving: sleeping " + RING_DELAY + " ms for pending range setup", true);
        Thread.sleep(RING_DELAY);

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
                Gossiper.instance.stop();
                MessagingService.shutdown();
                StageManager.shutdownNow();
                setMode("Decommissioned", true);
                // let op be responsible for killing the process
            }
        };
        unbootstrap(finishLeaving);
    }

    private void leaveRing()
    {
        SystemTable.setBootstrapped(false);
        tokenMetadata_.removeEndpoint(FBUtilities.getLocalAddress());
        calculatePendingRanges();

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.left(getLocalToken()));
        try
        {
            Thread.sleep(2 * Gossiper.intervalInMillis_);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    private void unbootstrap(final Runnable onFinish)
    {
        final CountDownLatch latch = new CountDownLatch(DatabaseDescriptor.getNonSystemTables().size());
        for (final String table : DatabaseDescriptor.getNonSystemTables())
        {
            Multimap<Range, InetAddress> rangesMM = getChangedRangesForLeaving(table, FBUtilities.getLocalAddress());
            if (logger_.isDebugEnabled())
                logger_.debug("Ranges needing transfer are [" + StringUtils.join(rangesMM.keySet(), ",") + "]");
            if (rangesMM.isEmpty())
            {
                latch.countDown();
                continue;
            }

            setMode("Leaving: streaming data to other nodes", true);
            final Set<Map.Entry<Range, InetAddress>> pending = new HashSet<Map.Entry<Range, InetAddress>>(rangesMM.entries());
            for (final Map.Entry<Range, InetAddress> entry : rangesMM.entries())
            {
                final Range range = entry.getKey();
                final InetAddress newEndpoint = entry.getValue();
                final Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        synchronized(pending)
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
                        StreamOut.transferRanges(newEndpoint, table, Arrays.asList(range), callback);
                    }
                });
            }
        }

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

    public void move(String newToken) throws IOException, InterruptedException
    {
        move(partitioner_.getTokenFactory().fromString(newToken));
    }

    public void loadBalance() throws IOException, InterruptedException
    {
        move((Token)null);
    }

    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param token new token to boot to, or if null, find balanced token to boot to
     */
    private void move(final Token token) throws IOException, InterruptedException
    {
        for (String table : DatabaseDescriptor.getTables())
        {
            if (tokenMetadata_.getPendingRanges(table, FBUtilities.getLocalAddress()).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }
        if (token != null && tokenMetadata_.sortedTokens().contains(token))
            throw new IOException("target token " + token + " is already owned by another node");

        if (logger_.isDebugEnabled())
            logger_.debug("Leaving: old token was " + getLocalToken());
        startLeaving();
        setMode("Leaving: sleeping " + RING_DELAY + " ms for pending range setup", true);
        Thread.sleep(RING_DELAY);

        Runnable finishMoving = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                Token bootstrapToken = token;
                if (bootstrapToken == null)
                {
                    StorageLoadBalancer.instance.waitForLoadInfo();
                    bootstrapToken = BootStrapper.getBalancedToken(tokenMetadata_, StorageLoadBalancer.instance.getLoadInfo());
                }
                logger_.info("re-bootstrapping to new token {}", bootstrapToken);
                bootstrap(bootstrapToken);
            }
        };
        unbootstrap(finishMoving);
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
     * blocks forever due to node/stream failure.
     */
    public void forceRemoveCompletion()
    {
        if (!replicatingNodes.isEmpty())
            logger_.warn("Removal not confirmed for for " + StringUtils.join(this.replicatingNodes, ","));
        replicatingNodes.clear();
    }

    /**
     * Remove a node that has died.
     *
     * @param tokenString token for the node
     */
    public void removeToken(String tokenString)
    {
        InetAddress myAddress = FBUtilities.getLocalAddress();
        Token localToken = tokenMetadata_.getToken(myAddress);
        Token token = partitioner_.getTokenFactory().fromString(tokenString);
        InetAddress endpoint = tokenMetadata_.getEndpoint(token);

        if (endpoint == null)
            throw new UnsupportedOperationException("Token not found.");

        if (endpoint.equals(myAddress))
             throw new UnsupportedOperationException("Cannot remove node's own token");

        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this token. Use decommission command to remove it from the ring");

        // A leaving endpoint that is dead is already being removed.
        if (tokenMetadata_.isLeaving(endpoint)) 
            throw new UnsupportedOperationException("Node " + endpoint + " is already being removed.");

        if (replicatingNodes != null)
            throw new UnsupportedOperationException("This node is already processing a removal. Wait for it to complete.");

        // Find the endpoints that are going to become responsible for data
        replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
            if (DatabaseDescriptor.getReplicationFactor(table) == 1)
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
        // bundle two states together. include this nodes state to keep the status quo, 
        // but indicate the leaving token so that it can be dealt with.
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.removingNonlocal(localToken, token));

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

        // indicate the token has left
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.removedNonlocal(localToken, token));

        replicatingNodes = null;
        removingNode = null;
    }

    public void confirmReplication(InetAddress node)
    {
        assert replicatingNodes != null;
        replicatingNodes.remove(node);
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
        return operationMode;
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
        setMode("Starting drain process", true);
        Gossiper.instance.stop();
        setMode("Draining: shutting down MessageService", false);
        MessagingService.shutdown();
        setMode("Draining: emptying MessageService pools", false);
        MessagingService.waitFor();

        setMode("Draining: clearing mutation stage", false);
        mutationStage.shutdown();
        mutationStage.awaitTermination(3600, TimeUnit.SECONDS);

        // lets flush.
        setMode("Draining: flushing column families", false);
        List<ColumnFamilyStore> cfses = new ArrayList<ColumnFamilyStore>();
        for (String tableName : DatabaseDescriptor.getNonSystemTables())
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

        // want to make sure that any segments deleted as a result of flushing are gone.
        DeletionService.waitFor();

        setMode("Node is drained", true);
    }

    /**
     * load schema from yaml. This can only be done on a fresh system.
     * @throws ConfigurationException
     * @throws IOException
     */
    public void loadSchemaFromYAML() throws ConfigurationException, IOException
    { 
        // validate
        final Collection<KSMetaData> tables = DatabaseDescriptor.readTablesFromYaml();
        for (KSMetaData table : tables)
        {
            if (!table.name.matches(Migration.NAME_VALIDATOR_REGEX))
                throw new ConfigurationException("Invalid table name: " + table.name);
            for (CFMetaData cfm : table.cfMetaData().values())
                if (!Migration.isLegalName(cfm.cfName))
                    throw new ConfigurationException("Invalid column family name: " + cfm.cfName);
        }
        
        Callable<Migration> call = new Callable<Migration>()
        {
            public Migration call() throws Exception
            {
                // blow up if there is a schema saved.
                if (DatabaseDescriptor.getDefsVersion().timestamp() > 0 || Migration.getLastMigrationId() != null)
                    throw new ConfigurationException("Cannot load from XML on top of pre-existing schemas.");
             
                Migration migration = null;
                for (KSMetaData table : tables)
                {
                    migration = new AddKeyspace(table); 
                    migration.apply();
                }
                return migration;
            }
        };
        Migration migration;
        try
        {
            migration = StageManager.getStage(Stage.MIGRATION).submit(call).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException)e.getCause();
            else if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            else if (e.getCause() instanceof Exception)
                throw new ConfigurationException(e.getCause().getMessage(), (Exception)e.getCause());
            else
                throw new RuntimeException(e);
        }
        
        assert DatabaseDescriptor.getDefsVersion().timestamp() > 0;
        DefsTable.dumpToStorage(DatabaseDescriptor.getDefsVersion());
        // flush system and definition tables.
        Collection<Future> flushers = new ArrayList<Future>();
        flushers.addAll(Table.open(Table.SYSTEM_TABLE).flush());
        for (Future f : flushers)
        {
            try
            {
                f.get();
            }
            catch (Exception e)
            {
                ConfigurationException ce = new ConfigurationException(e.getMessage());
                ce.initCause(e);
                throw ce;
            }
        }
        
        // we don't want to announce after every Migration.apply(). keep track of the last one and then announce the
        // current version.
        if (migration != null)
            migration.announce();
        
    }

    public String exportSchema() throws IOException
    {
        List<RawKeyspace> keyspaces = new ArrayList<RawKeyspace>();
        for (String ksname : DatabaseDescriptor.getNonSystemTables())
        {
            KSMetaData ksm = DatabaseDescriptor.getTableDefinition(ksname);
            RawKeyspace rks = new RawKeyspace();
            rks.name = ksm.name;
            rks.replica_placement_strategy = ksm.strategyClass.getName();
            rks.replication_factor = ksm.replicationFactor;
            rks.column_families = new RawColumnFamily[ksm.cfMetaData().size()];
            int i = 0;
            for (CFMetaData cfm : ksm.cfMetaData().values())
            {
                RawColumnFamily rcf = new RawColumnFamily();
                rcf.name = cfm.cfName;
                rcf.compare_with = cfm.comparator.getClass().getName();
                rcf.compare_subcolumns_with = cfm.subcolumnComparator == null ? null : cfm.subcolumnComparator.getClass().getName();
                rcf.column_type = cfm.cfType;
                rcf.comment = cfm.comment;
                rcf.keys_cached = cfm.keyCacheSize;
                rcf.read_repair_chance = cfm.readRepairChance;
                rcf.gc_grace_seconds = cfm.gcGraceSeconds;
                rcf.rows_cached = cfm.rowCacheSize;
                rcf.column_metadata = new RawColumnDefinition[cfm.column_metadata.size()];
                int j = 0;
                for (ColumnDefinition cd : cfm.column_metadata.values())
                {
                    RawColumnDefinition rcd = new RawColumnDefinition();
                    rcd.index_name = cd.index_name;
                    rcd.index_type = cd.index_type;
                    rcd.name = ByteBufferUtil.string(cd.name, Charsets.UTF_8);
                    rcd.validator_class = cd.validator.getClass().getName();
                    rcf.column_metadata[j++] = rcd;
                }
                if (j == 0)
                    rcf.column_metadata = null;
                rks.column_families[i++] = rcf;
            }
            // whew.
            keyspaces.add(rks);
        }
        
        DumperOptions options = new DumperOptions();
        /* Use a block YAML arrangement */
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        SkipNullRepresenter representer = new SkipNullRepresenter();
        /* Use Tag.MAP to avoid the class name being included as global tag */
        representer.addClassTag(RawColumnFamily.class, Tag.MAP);
        representer.addClassTag(Keyspaces.class, Tag.MAP);
        representer.addClassTag(ColumnDefinition.class, Tag.MAP);
        Dumper dumper = new Dumper(representer, options);
        Yaml yaml = new Yaml(dumper);
        Keyspaces ks = new Keyspaces();
        ks.keyspaces = keyspaces;
        return yaml.dump(ks);
    }
    
    public class Keyspaces
    {
        public List<RawKeyspace> keyspaces;
    }
    
    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = partitioner_;
        partitioner_ = newPartitioner;
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner_);
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
            futures.add(cfs.submitKeyCacheWrite());
            futures.add(cfs.submitRowCacheWrite());
        }
        FBUtilities.waitOnFutures(futures);
        logger_.debug("cache saves completed");
    }

}
