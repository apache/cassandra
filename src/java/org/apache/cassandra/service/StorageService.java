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

import java.io.IOException;
import java.io.IOError;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import javax.management.*;

import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.Streaming;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public final class StorageService implements IEndPointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = Logger.getLogger(StorageService.class);     

    // these aren't in an enum since other gossip users can create states ad-hoc too (e.g. load broadcasting)
    public final static String STATE_NORMAL = "NORMAL";
    public final static String STATE_BOOTSTRAPPING = "BOOTSTRAPPING";
    public final static String STATE_LEAVING = "LEAVING";
    public final static String STATE_LEFT = "LEFT";

    /* All verb handler identifiers */
    public final static String mutationVerbHandler_ = "ROW-MUTATION-VERB-HANDLER";
    public final static String binaryVerbHandler_ = "BINARY-VERB-HANDLER";
    public final static String readRepairVerbHandler_ = "READ-REPAIR-VERB-HANDLER";
    public final static String readVerbHandler_ = "ROW-READ-VERB-HANDLER";
    public final static String streamInitiateVerbHandler_ = "BOOTSTRAP-INITIATE-VERB-HANDLER";
    public final static String streamInitiateDoneVerbHandler_ = "BOOTSTRAP-INITIATE-DONE-VERB-HANDLER";
    public final static String streamFinishedVerbHandler_ = "BOOTSTRAP-TERMINATE-VERB-HANDLER";
    public final static String dataFileVerbHandler_ = "DATA-FILE-VERB-HANDLER";
    public final static String bootstrapMetadataVerbHandler_ = "BS-METADATA-VERB-HANDLER";
    public final static String rangeVerbHandler_ = "RANGE-VERB-HANDLER";
    public final static String rangeSliceVerbHandler_ = "RANGE-SLICE-VERB-HANDLER";
    public final static String bootstrapTokenVerbHandler_ = "SPLITS-VERB-HANDLER";

    private static IPartitioner partitioner_ = DatabaseDescriptor.getPartitioner();

    private static volatile StorageService instance_;

    public static IPartitioner<?> getPartitioner() {
        return partitioner_;
    }

    public Collection<Range> getLocalRanges()
    {
        return getRangesForEndPoint(FBUtilities.getLocalAddress());
    }

    public Range getLocalPrimaryRange()
    {
        return getPrimaryRangeForEndPoint(FBUtilities.getLocalAddress());
    }

    /*
     * Factory method that gets an instance of the StorageService
     * class.
    */
    public static StorageService instance()
    {
        if (instance_ == null)
        {
            synchronized (StorageService.class)
            {
                if (instance_ == null)
                {
                    try
                    {
                        instance_ = new StorageService();
                    }
                    catch (Throwable th)
                    {
                        logger_.error(LogUtil.throwableToString(th));
                        System.exit(1);
                    }
                }
            }
        }
        return instance_;
    }

    /*
     * This is the endpoint snitch which depends on the network architecture. We
     * need to keep this information for each endpoint so that we make decisions
     * while doing things like replication etc.
     *
     */
    private IEndPointSnitch endPointSnitch_;

    /* This abstraction maintains the token/endpoint metadata information */
    private TokenMetadata tokenMetadata_ = new TokenMetadata();
    private SystemTable.StorageMetadata storageMetadata_;

    /* This thread pool does consistency checks when the client doesn't care about consistency */
    private ExecutorService consistencyManager_ = new DebuggableThreadPoolExecutor(DatabaseDescriptor.getConsistencyThreads(),
                                                                                   DatabaseDescriptor.getConsistencyThreads(),
                                                                                   Integer.MAX_VALUE,
                                                                                   TimeUnit.SECONDS,
                                                                                   new LinkedBlockingQueue<Runnable>(),
                                                                                   new NamedThreadFactory("CONSISTENCY-MANAGER"));

    /* We use this interface to determine where replicas need to be placed */
    private AbstractReplicationStrategy replicationStrategy_;
    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    private Set<InetAddress> bootstrapSet;
  
    public synchronized void addBootstrapSource(InetAddress s)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Added " + s + " as a bootstrap source");
        bootstrapSet.add(s);
    }
    
    public synchronized void removeBootstrapSource(InetAddress s)
    {
        bootstrapSet.remove(s);
        if (logger_.isDebugEnabled())
            logger_.debug("Removed " + s + " as a bootstrap source; remaining is [" + StringUtils.join(bootstrapSet, ", ") + "]");

        if (bootstrapSet.isEmpty())
        {
            finishBootstrapping();
        }
    }

    private void finishBootstrapping()
    {
        isBootstrapMode = false;
        SystemTable.setBootstrapped(true);
        setToken(getLocalToken());
        Gossiper.instance().addApplicationState(StorageService.STATE_NORMAL, new ApplicationState(partitioner_.getTokenFactory().toString(getLocalToken())));
        logger_.info("Bootstrap completed! Now serving reads.");
    }

    private void updateForeignToken(Token token, InetAddress endpoint)
    {
        tokenMetadata_.update(token, endpoint);
        SystemTable.updateToken(endpoint, token);
    }

    /**
     * for bulk loading clients to be able to use tokenmetadata/messagingservice
     * without fully starting storageservice / systemtable.
     */
    public void updateForeignTokenUnsafe(Token token, InetAddress endpoint)
    {
        tokenMetadata_.update(token, endpoint);
    }

    /** This method updates the local token on disk  */
    public void setToken(Token token)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Setting token to " + token);
        SystemTable.updateToken(token);
        tokenMetadata_.update(token, FBUtilities.getLocalAddress());
    }

    public StorageService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.service:type=StorageService"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        bootstrapSet = new HashSet<InetAddress>();
        endPointSnitch_ = DatabaseDescriptor.getEndPointSnitch();

        /* register the verb handlers */
        MessagingService.instance().registerVerbHandlers(binaryVerbHandler_, new BinaryVerbHandler());
        MessagingService.instance().registerVerbHandlers(mutationVerbHandler_, new RowMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(readRepairVerbHandler_, new ReadRepairVerbHandler());
        MessagingService.instance().registerVerbHandlers(readVerbHandler_, new ReadVerbHandler());
        MessagingService.instance().registerVerbHandlers(dataFileVerbHandler_, new DataFileVerbHandler() );
        MessagingService.instance().registerVerbHandlers(rangeVerbHandler_, new RangeVerbHandler());
        MessagingService.instance().registerVerbHandlers(rangeSliceVerbHandler_, new RangeSliceVerbHandler());
        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(bootstrapTokenVerbHandler_, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(bootstrapMetadataVerbHandler_, new BootstrapMetadataVerbHandler() );
        MessagingService.instance().registerVerbHandlers(streamInitiateVerbHandler_, new Streaming.StreamInitiateVerbHandler());
        MessagingService.instance().registerVerbHandlers(streamInitiateDoneVerbHandler_, new Streaming.StreamInitiateDoneVerbHandler());
        MessagingService.instance().registerVerbHandlers(streamFinishedVerbHandler_, new Streaming.StreamFinishedVerbHandler());

        replicationStrategy_ = getReplicationStrategy(tokenMetadata_, partitioner_);
    }

    public static AbstractReplicationStrategy getReplicationStrategy(TokenMetadata tokenMetadata, IPartitioner partitioner)
    {
        AbstractReplicationStrategy replicationStrategy = null;
        Class<AbstractReplicationStrategy> cls = DatabaseDescriptor.getReplicaPlacementStrategyClass();
        Class [] parameterTypes = new Class[] { TokenMetadata.class, IPartitioner.class, int.class};
        try
        {
            Constructor<AbstractReplicationStrategy> constructor = cls.getConstructor(parameterTypes);
            replicationStrategy = constructor.newInstance(tokenMetadata, partitioner, DatabaseDescriptor.getReplicationFactor());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return replicationStrategy;
    }
    
    public void start() throws IOException
    {
        storageMetadata_ = SystemTable.initMetadata();

        /* Listen for application messages */
        MessagingService.instance().listen(FBUtilities.getLocalAddress());
        /* Listen for control messages */
        MessagingService.instance().listenUDP(FBUtilities.getLocalAddress());

        SelectorManager.getSelectorManager().start();
        SelectorManager.getUdpSelectorManager().start();

        StorageLoadBalancer.instance().startBroadcasting();

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a nodeId to our state, below.)
        Gossiper.instance().register(this);
        Gossiper.instance().start(FBUtilities.getLocalAddress(), storageMetadata_.getGeneration());

        if (DatabaseDescriptor.isAutoBootstrap()
            && !(DatabaseDescriptor.getSeeds().contains(FBUtilities.getLocalAddress()) || SystemTable.isBootstrapped()))
        {
            logger_.info("Starting in bootstrap mode (first, sleeping to get load information)");
            StorageLoadBalancer.instance().waitForLoadInfo();
            logger_.info("... got load info");
            Token token = BootStrapper.getBootstrapToken(tokenMetadata_, StorageLoadBalancer.instance().getLoadInfo());
            startBootstrap(token);
            // don't finish startup (enabling thrift) until after bootstrap is done
            while (isBootstrapMode)
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
        }
        else
        {
            SystemTable.setBootstrapped(true);
            Token token = storageMetadata_.getToken();
            tokenMetadata_.update(token, FBUtilities.getLocalAddress());
            Gossiper.instance().addApplicationState(StorageService.STATE_NORMAL, new ApplicationState(partitioner_.getTokenFactory().toString(token)));
        }

        assert tokenMetadata_.sortedTokens().size() > 0;
    }

    private void startBootstrap(Token token) throws IOException
    {
        isBootstrapMode = true;
        SystemTable.updateToken(token); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        Gossiper.instance().addApplicationState(StorageService.STATE_BOOTSTRAPPING, new ApplicationState(partitioner_.getTokenFactory().toString(token)));
        logger_.info("bootstrap sleeping " + Streaming.RING_DELAY);
        try
        {
            Thread.sleep(Streaming.RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        new BootStrapper(replicationStrategy_, FBUtilities.getLocalAddress(), token, tokenMetadata_).startBootstrap(); // handles token update
    }

    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata_;
    }

    public IEndPointSnitch getEndPointSnitch()
    {
        return endPointSnitch_;
    }
    
    /*
     * Given an InetAddress this method will report if the
     * endpoint is in the same data center as the local
     * storage endpoint.
    */
    public boolean isInSameDataCenter(InetAddress endpoint) throws IOException
    {
        return endPointSnitch_.isInSameDataCenter(FBUtilities.getLocalAddress(), endpoint);
    }
    
    /**
     * This method performs the requisite operations to make
     * sure that the N replicas are in sync. We do this in the
     * background when we do not care much about consistency.
     */
    public void doConsistencyCheck(Row row, List<InetAddress> endpoints, ReadCommand command)
    {
        consistencyManager_.submit(new ConsistencyManager(command.table, row, endpoints, command));
    }

    public Map<Range, List<String>> getRangeToEndPointMap()
    {
        /* All the ranges for the tokens */
        List<Range> ranges = getAllRanges(tokenMetadata_.sortedTokens());
        Map<Range, List<String>> map = new HashMap<Range, List<String>>();
        for (Map.Entry<Range,List<InetAddress>> entry : constructRangeToEndPointMap(ranges).entrySet())
        {
            map.put(entry.getKey(), stringify(entry.getValue()));
        }
        return map;
    }

    /**
     * Construct the range to endpoint mapping based on the true view 
     * of the world. 
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    public Map<Range, List<InetAddress>> constructRangeToEndPointMap(List<Range> ranges)
    {
        Map<Range, List<InetAddress>> rangeToEndPointMap = new HashMap<Range, List<InetAddress>>();
        for (Range range : ranges)
        {
            rangeToEndPointMap.put(range, replicationStrategy_.getNaturalEndpoints(range.right()));
        }
        return rangeToEndPointMap;
    }

    /*
     * onChange only ever sees one ApplicationState piece change at a time, so we perform a kind of state machine here.
     * We are concerned with two events: knowing the token associated with an enpoint, and knowing its operation mode.
     * Nodes can start in either bootstrap or normal mode, and from bootstrap mode can change mode to normal.
     * A node in bootstrap mode needs to have pendingranges set in TokenMetadata; a node in normal mode
     * should instead be part of the token ring.
     */
    public void onChange(InetAddress endpoint, String stateName, ApplicationState state)
    {
        if (STATE_BOOTSTRAPPING.equals(stateName))
        {
            Token token = getPartitioner().getTokenFactory().fromString(state.getValue());
            if (logger_.isDebugEnabled())
                logger_.debug(endpoint + " state bootstrapping, token " + token);
            updateBootstrapRanges(token, endpoint);
        }
        else if (STATE_NORMAL.equals(stateName))
        {
            Token token = getPartitioner().getTokenFactory().fromString(state.getValue());
            if (logger_.isDebugEnabled())
                logger_.debug(endpoint + " state normal, token " + token);
            updateForeignToken(token, endpoint);
            replicationStrategy_.removeObsoletePendingRanges();
        }
        else if (STATE_LEAVING.equals(stateName))
        {
            Token token = getPartitioner().getTokenFactory().fromString(state.getValue());
            assert tokenMetadata_.getToken(endpoint).equals(token);
            updateLeavingRanges(endpoint);
        }
        else if (STATE_LEFT.equals(stateName))
        {
            Token token = getPartitioner().getTokenFactory().fromString(state.getValue());
            assert tokenMetadata_.getToken(endpoint).equals(token);
            tokenMetadata_.removeEndpoint(endpoint);
            replicationStrategy_.removeObsoletePendingRanges();
        }
    }

    private Multimap<Range, InetAddress> getChangedRangesForLeaving(InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range> ranges = getRangesForEndPoint(endpoint);

        if (logger_.isDebugEnabled())
            logger_.debug("leaving node ranges are [" + StringUtils.join(ranges, ", ") + "]");

        Map<Range, ArrayList<InetAddress>> currentReplicaEndpoints = new HashMap<Range, ArrayList<InetAddress>>();

        // Find (for each range) all nodes that store replicas for these ranges as well
        for (Range range : ranges)
            currentReplicaEndpoints.put(range, replicationStrategy_.getNaturalEndpoints(range.right(), tokenMetadata_));

        TokenMetadata temp = tokenMetadata_.cloneWithoutPending();
        temp.removeEndpoint(endpoint);

        Multimap<Range, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoins list, but
        // not in the currentReplicaEndpoins list, will be needing the
        // range.
        for (Range range : ranges)
        {
            ArrayList<InetAddress> newReplicaEndpoints = replicationStrategy_.getNaturalEndpoints(range.right(), temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger_.isDebugEnabled())
                logger_.debug("adding pending range " + range + " to endpoints " + StringUtils.join(newReplicaEndpoints, ", "));
            changedRanges.putAll(range, newReplicaEndpoints);
        }

        return changedRanges;
    }

    private void updateLeavingRanges(final InetAddress endpoint)
    {
        if (logger_.isDebugEnabled())
            logger_.debug(endpoint + " is leaving; calculating pendingranges");
        Multimap<Range, InetAddress> ranges = getChangedRangesForLeaving(endpoint);
        for (Range range : ranges.keySet())
        {
            for (InetAddress newEndpoint : ranges.get(range))
            {
                tokenMetadata_.addPendingRange(range, newEndpoint);
            }
        }
    }

    private void updateBootstrapRanges(Token token, InetAddress endpoint)
    {
        for (Range range : replicationStrategy_.getPendingAddressRanges(tokenMetadata_, token, endpoint))
        {
            tokenMetadata_.addPendingRange(range, endpoint);
        }
    }

    public static void updateBootstrapRanges(AbstractReplicationStrategy strategy, TokenMetadata metadata, Token token, InetAddress endpoint)
    {
        for (Range range : strategy.getPendingAddressRanges(metadata, token, endpoint))
        {
            metadata.addPendingRange(range, endpoint);
        }
    }

    public void onJoin(InetAddress endpoint, EndPointState epState)
    {
        for (Map.Entry<String,ApplicationState> entry : epState.getSortedApplicationStates())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    public void onAlive(InetAddress endpoint, EndPointState state)
    {
        deliverHints(endpoint);
    }

    public void onDead(InetAddress endpoint, EndPointState state) {}

    /** raw load value */
    public double getLoad()
    {
        return FileUtils.getUsedDiskSpace();
    }

    public String getLoadString()
    {
        return FileUtils.stringifyFileSize(FileUtils.getUsedDiskSpace());
    }

    public Map<String, String> getLoadMap()
    {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<InetAddress,Double> entry : StorageLoadBalancer.instance().getLoadInfo().entrySet())
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
        HintedHandOffManager.instance().deliverHints(endpoint);
    }

    public Token getLocalToken()
    {
        return storageMetadata_.getToken();
    }

    /* This methods belong to the MBean interface */

    public String getToken()
    {
        return getLocalToken().toString();
    }

    public Set<String> getLiveNodes()
    {
        return stringify(Gossiper.instance().getLiveMembers());
    }

    public Set<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance().getUnreachableMembers());
    }

    private Set<String> stringify(Set<InetAddress> endPoints)
    {
        Set<String> stringEndPoints = new HashSet<String>();
        for (InetAddress ep : endPoints)
        {
            stringEndPoints.add(ep.getHostAddress());
        }
        return stringEndPoints;
    }

    private List<String> stringify(List<InetAddress> endPoints)
    {
        List<String> stringEndPoints = new ArrayList<String>();
        for (InetAddress ep : endPoints)
        {
            stringEndPoints.add(ep.getHostAddress());
        }
        return stringEndPoints;
    }

    public int getCurrentGenerationNumber()
    {
        return Gossiper.instance().getCurrentGenerationNumber(FBUtilities.getLocalAddress());
    }

    public void forceTableCleanup() throws IOException
    {
        List<String> tables = DatabaseDescriptor.getTables();
        for ( String tName : tables )
        {
            Table table = Table.open(tName);
            table.forceCleanup();
        }
    }
    
    public void forceTableCompaction() throws IOException
    {
        List<String> tables = DatabaseDescriptor.getTables();
        for ( String tName : tables )
        {
            Table table = Table.open(tName);
            table.forceCompaction();
        }        
    }

    /**
     * Takes the snapshot for a given table.
     * 
     * @param tableName the name of the table.
     * @param tag   the tag given to the snapshot (null is permissible)
     */
    public void takeSnapshot(String tableName, String tag) throws IOException
    {
        if (DatabaseDescriptor.getTable(tableName) == null)
        {
            throw new IOException("Table " + tableName + "does not exist");
        }
        Table tableInstance = Table.open(tableName);
        tableInstance.snapshot(tag);
    }
    
    /**
     * Takes a snapshot for every table.
     * 
     * @param tag the tag given to the snapshot (null is permissible)
     */
    public void takeAllSnapshot(String tag) throws IOException
    {
        for (String tableName: DatabaseDescriptor.getTables())
        {
            Table tableInstance = Table.open(tableName);
            tableInstance.snapshot(tag);
        }
    }

    /**
     * Remove all the existing snapshots.
     */
    public void clearSnapshot() throws IOException
    {
        for (String tableName: DatabaseDescriptor.getTables())
        {
            Table tableInstance = Table.open(tableName);
            tableInstance.clearSnapshot();
        }
        if (logger_.isDebugEnabled())
            logger_.debug("Cleared out all snapshot directories");
    }

    /**
     * Flush all memtables for a table and column families.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableFlush(String tableName, String... columnFamilies) throws IOException
    {
        if (DatabaseDescriptor.getTable(tableName) == null)
        {
            throw new IOException("Table " + tableName + "does not exist");
        }

        Table table = Table.open(tableName);
        Set<String> positiveColumnFamilies = table.getColumnFamilies();

        // no columnFamilies means flush'em all.
        if (columnFamilies == null || columnFamilies.length == 0)
        {
            columnFamilies = positiveColumnFamilies.toArray(new String[positiveColumnFamilies.size()]);
        }

        for (String columnFamily : columnFamilies)
        {

            if (positiveColumnFamilies.contains(columnFamily))
            {
                ColumnFamilyStore cfStore = table.getColumnFamilyStore(columnFamily);
                logger_.debug("Forcing binary flush on keyspace " + tableName + ", CF " + columnFamily);
                cfStore.forceFlushBinary();
                logger_.debug("Forcing flush on keyspace " + tableName + ", CF " + columnFamily);
                cfStore.forceFlush();
            }
            else
            {
                // this means there was a cf passed in that is not recognized in the keyspace. report it and continue.
                logger_.warn(String.format("Invalid column family specified: %s. Proceeding with others.", columnFamily));
            }
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
        return tokenMetadata_.getEndPoint(tokenMetadata_.getPredecessor(token));
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    public InetAddress getSuccessor(InetAddress ep)
    {
        Token token = tokenMetadata_.getToken(ep);
        return tokenMetadata_.getEndPoint(tokenMetadata_.getSuccessor(token));
    }

    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    public Range getPrimaryRangeForEndPoint(InetAddress ep)
    {
        return tokenMetadata_.getPrimaryRangeFor(tokenMetadata_.getToken(ep));
    }
    
    /**
     * Get all ranges an endpoint is responsible for.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range> getRangesForEndPoint(InetAddress ep)
    {
        return replicationStrategy_.getAddressRanges().get(ep);
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
     * This method returns the endpoint that is responsible for storing the
     * specified key.
     *
     * @param key - key for which we need to find the endpoint
     * @return value - the endpoint responsible for this key
     */
    public InetAddress getPrimary(String key)
    {
        InetAddress endpoint = FBUtilities.getLocalAddress();
        Token token = partitioner_.getToken(key);
        List tokens = new ArrayList<Token>(tokenMetadata_.sortedTokens());
        if (tokens.size() > 0)
        {
            int index = Collections.binarySearch(tokens, token);
            if (index >= 0)
            {
                /*
                 * retrieve the endpoint based on the token at this index in the
                 * tokens list
                 */
                endpoint = tokenMetadata_.getEndPoint((Token) tokens.get(index));
            }
            else
            {
                index = (index + 1) * (-1);
                if (index < tokens.size())
                    endpoint = tokenMetadata_.getEndPoint((Token) tokens.get(index));
                else
                    endpoint = tokenMetadata_.getEndPoint((Token) tokens.get(0));
            }
        }
        return endpoint;
    }

    /**
     * This method determines whether the local endpoint is the
     * primary for the given key.
     * @param key
     * @return true if the local endpoint is the primary replica.
    */
    public boolean isPrimary(String key)
    {
        InetAddress endpoint = getPrimary(key);
        return FBUtilities.getLocalAddress().equals(endpoint);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String key)
    {
        return replicationStrategy_.getNaturalEndpoints(partitioner_.getToken(key));
    }    
    
    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(String key)
    {
        List<InetAddress> liveEps = new ArrayList<InetAddress>();
        List<InetAddress> endpoints = getNaturalEndpoints(key);
        
        for ( InetAddress endpoint : endpoints )
        {
            if ( FailureDetector.instance().isAlive(endpoint) )
                liveEps.add(endpoint);
        }
        
        return liveEps;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public Map<InetAddress, InetAddress> getHintedEndpointMap(String key, List<InetAddress> naturalEndpoints)
    {
        return replicationStrategy_.getHintedEndpoints(partitioner_.getToken(key), naturalEndpoints);
    }

    /**
     * This function finds the most suitable endpoint given a key.
     * It checks for locality and alive test.
     */
    public InetAddress findSuitableEndPoint(String key) throws IOException, UnavailableException
    {
        List<InetAddress> endpoints = getNaturalEndpoints(key);
        for(InetAddress endPoint: endpoints)
        {
            if(endPoint.equals(FBUtilities.getLocalAddress()))
            {
                return endPoint;
            }
        }
        int j = 0;
        for ( ; j < endpoints.size(); ++j )
        {
            if ( StorageService.instance().isInSameDataCenter(endpoints.get(j)) && FailureDetector.instance().isAlive(endpoints.get(j)))
            {
                return endpoints.get(j);
            }
        }
        // We have tried to be really nice but looks like there are no servers 
        // in the local data center that are alive and can service this request so 
        // just send it to the first alive guy and see if we get anything.
        j = 0;
        for ( ; j < endpoints.size(); ++j )
        {
            if ( FailureDetector.instance().isAlive(endpoints.get(j)))
            {
                if (logger_.isDebugEnabled())
                  logger_.debug("InetAddress " + endpoints.get(j) + " is alive so get data from it.");
                return endpoints.get(j);
            }
        }

        throw new UnavailableException(); // no nodes that could contain key are alive
    }

    Map<String, String> getStringEndpointMap()
    {
        HashMap<String, String> map = new HashMap<String, String>();
        for (Token t : tokenMetadata_.sortedTokens())
        {
            map.put(t.toString(), tokenMetadata_.getEndPoint(t).getHostAddress());
        }
        return map;
    }

    public void setLog4jLevel(String classQualifier, String rawLevel)
    {
        Level level = Level.toLevel(rawLevel);
        Logger.getLogger(classQualifier).setLevel(level);
        logger_.info("set log level to " + level + " for classes under '" + classQualifier + "' (if the level doesn't look like '" + rawLevel + "' then log4j couldn't parse '" + rawLevel + "')");
    }

    /**
     * @param splits: number of ranges to break into. Minimum 2.
     * @return list of Tokens (_not_ keys!) breaking up the data this node is responsible for into `splits` pieces.
     * There will be 1 more token than splits requested.  So for splits of 2, tokens T1 T2 T3 will be returned,
     * where (T1, T2] is the first range and (T2, T3] is the second.  The first token will always be the left
     * Token of this node's primary range, and the last will always be the Right token of that range.
     */ 
    public List<String> getSplits(int splits)
    {
        assert splits > 1;
        // we use the actual Range token for the first and last brackets of the splits to ensure correctness
        // (we're only operating on 1/128 of the keys remember)
        Range range = getLocalPrimaryRange();
        List<String> tokens = new ArrayList<String>();
        tokens.add(range.left().toString());

        List<DecoratedKey> decoratedKeys = SSTableReader.getIndexedDecoratedKeys();
        if (decoratedKeys.size() < splits)
        {
            // not enough keys to generate good splits -- generate random ones instead
            // (since this only happens when we don't have many keys, it doesn't really matter that the splits are poor)
            for (int i = 1; i < splits; i++)
            {
                tokens.add(partitioner_.getRandomToken().toString());
            }
        }
        else
        {
            for (int i = 1; i < splits; i++)
            {
                int index = i * (decoratedKeys.size() / splits);
                tokens.add(decoratedKeys.get(index).token.toString());
            }
        }

        tokens.add(range.right().toString());
        return tokens;
    }

    public void decommission() throws InterruptedException
    {
        if (!tokenMetadata_.isMember(FBUtilities.getLocalAddress()))
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
        if (tokenMetadata_.sortedTokens().size() < 2)
            throw new UnsupportedOperationException("no other nodes in the ring; decommission would be pointless");
        if (tokenMetadata_.getPendingRanges(FBUtilities.getLocalAddress()).size() > 0)
            throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");

        logger_.info("DECOMMISSIONING");
        Gossiper.instance().addApplicationState(STATE_LEAVING, new ApplicationState(getLocalToken().toString()));
        logger_.info("decommission sleeping " + Streaming.RING_DELAY);
        Thread.sleep(Streaming.RING_DELAY);

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
                Gossiper.instance().stop();
                logger_.info("DECOMMISSION FINISHED.");
                // let op be responsible for killing the process
            }
        };
        unbootstrap(finishLeaving);
    }

    private void leaveRing()
    {
        SystemTable.setBootstrapped(false);
        tokenMetadata_.removeEndpoint(FBUtilities.getLocalAddress());
        replicationStrategy_.removeObsoletePendingRanges();

        if (logger_.isDebugEnabled())
            logger_.debug("");
        Gossiper.instance().addApplicationState(STATE_LEFT, new ApplicationState(getLocalToken().toString()));
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
        Multimap<Range, InetAddress> rangesMM = getChangedRangesForLeaving(FBUtilities.getLocalAddress());
        if (logger_.isDebugEnabled())
            logger_.debug("Ranges needing transfer are [" + StringUtils.join(rangesMM.keySet(), ",") + "]");

        if (rangesMM.isEmpty())
        {
            // nothing needs transfer, so leave immediately.  this can happen when replication factor == number of nodes.
            leaveRing();
            onFinish.run();
            return;
        }

        final Set<Map.Entry<Range, InetAddress>> pending = new HashSet<Map.Entry<Range, InetAddress>>(rangesMM.entries());
        for (final Map.Entry<Range, InetAddress> entry : rangesMM.entries())
        {
            final Range range = entry.getKey();
            final InetAddress newEndpoint = entry.getValue();
            final Runnable callback = new Runnable()
            {
                public synchronized void run()
                {
                    pending.remove(entry);
                    if (pending.isEmpty())
                    {
                        leaveRing();
                        onFinish.run();
                    }
                }
            };
            StageManager.getStage(StageManager.streamStage_).execute(new Runnable()
            {
                public void run()
                {
                    // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                    Streaming.transferRanges(newEndpoint, Arrays.asList(range), callback);
                }
            });
        }
    }

    public void move(String newToken) throws InterruptedException
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
    private void move(final Token token) throws InterruptedException
    {
        if (tokenMetadata_.getPendingRanges(FBUtilities.getLocalAddress()).size() > 0)
            throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");

        logger_.info("starting move. leaving token " + getLocalToken());
        Gossiper.instance().addApplicationState(STATE_LEAVING, new ApplicationState(getLocalToken().toString()));
        logger_.info("move sleeping " + Streaming.RING_DELAY);
        Thread.sleep(Streaming.RING_DELAY);

        Runnable finishMoving = new Runnable()
        {
            public void run()
            {
                try
                {
                    Token bootstrapToken = token;
                    if (bootstrapToken == null)
                        bootstrapToken = BootStrapper.getBalancedToken(tokenMetadata_, StorageLoadBalancer.instance().getLoadInfo());
                    logger_.info("re-bootstrapping to new token " + bootstrapToken);
                    startBootstrap(bootstrapToken);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
        unbootstrap(finishMoving);
    }

    public WriteResponseHandler getWriteResponseHandler(int blockFor, int consistency_level)
    {
        return replicationStrategy_.getWriteResponseHandler(blockFor, consistency_level);
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy_;
    }

    public void cancelPendingRanges()
    {
        tokenMetadata_.clearPendingRanges();
    }
}
