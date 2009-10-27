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
import java.lang.management.ManagementFactory;
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

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.commons.lang.StringUtils;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public final class StorageService implements IEndPointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = Logger.getLogger(StorageService.class);     

    private final static String NODE_ID = "NODE-ID";
    public final static String MODE = "MODE";
    public final static String MODE_MOVING = "move";
    public final static String MODE_NORMAL = "run";

    /* All stage identifiers */
    public final static String mutationStage_ = "ROW-MUTATION-STAGE";
    public final static String readStage_ = "ROW-READ-STAGE";
    
    /* All verb handler identifiers */
    public final static String mutationVerbHandler_ = "ROW-MUTATION-VERB-HANDLER";
    public final static String tokenVerbHandler_ = "TOKEN-VERB-HANDLER";
    public final static String binaryVerbHandler_ = "BINARY-VERB-HANDLER";
    public final static String readRepairVerbHandler_ = "READ-REPAIR-VERB-HANDLER";
    public final static String readVerbHandler_ = "ROW-READ-VERB-HANDLER";
    public final static String bootStrapInitiateVerbHandler_ = "BOOTSTRAP-INITIATE-VERB-HANDLER";
    public final static String bootStrapInitiateDoneVerbHandler_ = "BOOTSTRAP-INITIATE-DONE-VERB-HANDLER";
    public final static String bootStrapTerminateVerbHandler_ = "BOOTSTRAP-TERMINATE-VERB-HANDLER";
    public final static String dataFileVerbHandler_ = "DATA-FILE-VERB-HANDLER";
    public final static String bootstrapMetadataVerbHandler_ = "BS-METADATA-VERB-HANDLER";
    public final static String rangeVerbHandler_ = "RANGE-VERB-HANDLER";
    public final static String bootstrapTokenVerbHandler_ = "SPLITS-VERB-HANDLER";

    private static IPartitioner partitioner_ = DatabaseDescriptor.getPartitioner();

    private static volatile StorageService instance_;

    public static IPartitioner<?> getPartitioner() {
        return partitioner_;
    }

    public Set<Range> getLocalRanges()
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
            logger_.debug("Removed " + s + " as a bootstrap source");

        if (bootstrapSet.isEmpty())
        {
            SystemTable.setBootstrapped();
            Gossiper.instance().addApplicationState(MODE, new ApplicationState(MODE_NORMAL));
            logger_.info("Bootstrap completed! Now serving reads.");
        }
    }

    private void updateForeignToken(Token token, InetAddress endpoint)
    {
        tokenMetadata_.update(token, endpoint);
        SystemTable.updateToken(endpoint, token);
    }

    /** This method updates the local token on disk and starts broacasting it to others. */
    public void setToken(Token token)
    {
        SystemTable.updateToken(token);
        tokenMetadata_.update(token, FBUtilities.getLocalAddress());
    }

    public void setAndBroadcastToken(Token token)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Setting token to " + token + " and gossiping it");
        setToken(token);
        ApplicationState state = new ApplicationState(partitioner_.getTokenFactory().toString(token));
        Gossiper.instance().addApplicationState(StorageService.NODE_ID, state);
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
        MessagingService.instance().registerVerbHandlers(tokenVerbHandler_, new TokenUpdateVerbHandler());
        MessagingService.instance().registerVerbHandlers(binaryVerbHandler_, new BinaryVerbHandler());
        MessagingService.instance().registerVerbHandlers(mutationVerbHandler_, new RowMutationVerbHandler());
        MessagingService.instance().registerVerbHandlers(readRepairVerbHandler_, new ReadRepairVerbHandler());
        MessagingService.instance().registerVerbHandlers(readVerbHandler_, new ReadVerbHandler());
        MessagingService.instance().registerVerbHandlers(dataFileVerbHandler_, new DataFileVerbHandler() );
        MessagingService.instance().registerVerbHandlers(rangeVerbHandler_, new RangeVerbHandler());
        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(bootstrapTokenVerbHandler_, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(bootstrapMetadataVerbHandler_, new BootstrapMetadataVerbHandler() );
        MessagingService.instance().registerVerbHandlers(bootStrapInitiateVerbHandler_, new BootStrapper.BootStrapInitiateVerbHandler());
        MessagingService.instance().registerVerbHandlers(bootStrapInitiateDoneVerbHandler_, new BootStrapper.BootstrapInitiateDoneVerbHandler());
        MessagingService.instance().registerVerbHandlers(bootStrapTerminateVerbHandler_, new BootStrapper.BootstrapTerminateVerbHandler());

        StageManager.registerStage(StorageService.mutationStage_,
                                   new MultiThreadedStage(StorageService.mutationStage_, DatabaseDescriptor.getConcurrentWriters()));
        StageManager.registerStage(StorageService.readStage_,
                                   new MultiThreadedStage(StorageService.readStage_, DatabaseDescriptor.getConcurrentReaders()));

        Class cls = DatabaseDescriptor.getReplicaPlacementStrategyClass();
        Class [] parameterTypes = new Class[] { TokenMetadata.class, IPartitioner.class, int.class, int.class};
        try
        {
            replicationStrategy_ = (AbstractReplicationStrategy) cls.getConstructor(parameterTypes).newInstance(tokenMetadata_, partitioner_, DatabaseDescriptor.getReplicationFactor(), DatabaseDescriptor.getStoragePort());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start() throws IOException
    {
        storageMetadata_ = SystemTable.initMetadata();
        isBootstrapMode = DatabaseDescriptor.isAutoBootstrap()
                          && !(DatabaseDescriptor.getSeeds().contains(FBUtilities.getLocalAddress()) || SystemTable.isBootstrapped());

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

        if (isBootstrapMode)
        {
            logger_.info("Starting in bootstrap mode (first, sleeping to get load information)");
            Gossiper.instance().addApplicationState(MODE, new ApplicationState(MODE_MOVING));
            BootStrapper.guessTokenIfNotSpecified();
            new BootStrapper(replicationStrategy_, FBUtilities.getLocalAddress(), getLocalToken(), tokenMetadata_).startBootstrap(); // handles token update
        }
        else
        {
            SystemTable.setBootstrapped();
        }
        setAndBroadcastToken(storageMetadata_.getToken());

        assert tokenMetadata_.cloneTokenEndPointMap().size() > 0;
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
    
    /*
     * This method performs the requisite operations to make
     * sure that the N replicas are in sync. We do this in the
     * background when we do not care much about consistency.
     */
    public void doConsistencyCheck(Row row, List<InetAddress> endpoints, ReadCommand command)
    {
        Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, command);
        consistencyManager_.submit(consistencySentinel);
    }

    public Map<Range, List<String>> getRangeToEndPointMap()
    {
        /* Get the token to endpoint map. */
        Map<Token, InetAddress> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        /* All the ranges for the tokens */
        Range[] ranges = getAllRanges(tokenToEndPointMap.keySet());
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
    public Map<Range, List<InetAddress>> constructRangeToEndPointMap(Range[] ranges)
    {
        Map<Range, List<InetAddress>> rangeToEndPointMap = new HashMap<Range, List<InetAddress>>();
        for (Range range : ranges)
        {
            rangeToEndPointMap.put(range, replicationStrategy_.getNaturalEndpoints(range.right()));
        }
        return rangeToEndPointMap;
    }
    
    /**
     * Construct the range to endpoint mapping based on the view as dictated
     * by the mapping of token to endpoints passed in. 
     * @param ranges
     * @param tokenToEndPointMap mapping of token to endpoints.
     * @return mapping of ranges to the replicas responsible for them.
    */
    public Map<Range, List<InetAddress>> constructRangeToEndPointMap(Range[] ranges, Map<Token, InetAddress> tokenToEndPointMap)
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<InetAddress>> rangeToEndPointMap = new HashMap<Range, List<InetAddress>>();
        for ( Range range : ranges )
        {
            rangeToEndPointMap.put(range, replicationStrategy_.getNaturalEndpoints(range.right(), tokenToEndPointMap));
        }
        if (logger_.isDebugEnabled())
          logger_.debug("Done constructing range to endpoint map ...");
        return rangeToEndPointMap;
    }

    /**
     *  Called when there is a change in application state. In particular
     *  we are interested in new tokens as a result of a new node or an
     *  existing node moving to a new location on the ring.
    */
    public void onChange(InetAddress endpoint, EndPointState epState)
    {
        /* node identifier for this endpoint on the identifier space */
        ApplicationState nodeIdState = epState.getApplicationState(StorageService.NODE_ID);
        /* Check if this has a bootstrapping state message */
        ApplicationState modeState = epState.getApplicationState(StorageService.MODE);
        if (modeState != null)
        {
            String mode = modeState.getState();
            if (logger_.isDebugEnabled())
                logger_.debug(endpoint + " is in " + mode + " mode");
            boolean bootstrapState = mode.equals(MODE_MOVING);
            tokenMetadata_.setBootstrapping(endpoint,  bootstrapState);
        }

        if (nodeIdState != null)
        {
            Token newToken = getPartitioner().getTokenFactory().fromString(nodeIdState.getState());
            if (logger_.isDebugEnabled())
              logger_.debug("CHANGE IN STATE FOR " + endpoint + " - has token " + nodeIdState.getState());
            Token oldToken = tokenMetadata_.getToken(endpoint);

            if ( oldToken != null )
            {
                /*
                 * If oldToken equals the newToken then the node had crashed
                 * and is coming back up again. If oldToken is not equal to
                 * the newToken this means that the node is being relocated
                 * to another position in the ring.
                */
                if ( !oldToken.equals(newToken) )
                {
                    if (logger_.isDebugEnabled())
                      logger_.debug("Relocation for endpoint " + endpoint);
                    updateForeignToken(newToken, endpoint);
                }
                else
                {
                    /*
                     * This means the node crashed and is coming back up.
                     * Deliver the hints that we have for this endpoint.
                    */
                    if (logger_.isDebugEnabled())
                      logger_.debug("Sending hinted data to " + endpoint);
                    deliverHints(endpoint);
                }
            }
            else
            {
                /*
                 * This is a new node and we just update the token map.
                */
                updateForeignToken(newToken, endpoint);
            }
        }
        else
        {
            /*
             * If we are here and if this node is UP and already has an entry
             * in the token map. It means that the node was behind a network partition.
            */
            if ( epState.isAlive() && tokenMetadata_.isKnownEndPoint(endpoint) )
            {
                if (logger_.isDebugEnabled())
                  logger_.debug("InetAddress " + endpoint + " just recovered from a partition. Sending hinted data.");
                deliverHints(endpoint);
            }
        }
    }

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
        // gossiper doesn't bother sending to itself, so if there are no other nodes around
        // we need to cheat to get load information for the local node
        if (!map.containsKey(FBUtilities.getLocalAddress().getHostAddress()))
        {
            map.put(FBUtilities.getLocalAddress().getHostAddress(), getLoadString());
        }
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
        return tokenMetadata_.getToken(FBUtilities.getLocalAddress());
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
        return tokenMetadata_.getEndPoint(replicationStrategy_.getPredecessor(token, tokenMetadata_.cloneTokenEndPointMap()));
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    public InetAddress getSuccessor(InetAddress ep)
    {
        Token token = tokenMetadata_.getToken(ep);
        return tokenMetadata_.getEndPoint(replicationStrategy_.getSuccessor(token, tokenMetadata_.cloneTokenEndPointMap()));
    }

    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    public Range getPrimaryRangeForEndPoint(InetAddress ep)
    {
        Token right = tokenMetadata_.getToken(ep);
        return replicationStrategy_.getPrimaryRangeFor(right, tokenMetadata_.cloneTokenEndPointMap());
    }
    
    /**
     * Get all ranges an endpoint is responsible for.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Set<Range> getRangesForEndPoint(InetAddress ep)
    {
        return replicationStrategy_.getAddressRanges().get(ep);
    }
        
    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of 
     * ranges.
     * @return ranges in sorted order
    */
    public Range[] getAllRanges(Set<Token> tokens)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("computing ranges for " + StringUtils.join(tokens, ", "));
        List<Range> ranges = new ArrayList<Range>();
        List<Token> allTokens = new ArrayList<Token>(tokens);
        Collections.sort(allTokens);
        int size = allTokens.size();
        for ( int i = 1; i < size; ++i )
        {
            Range range = new Range( allTokens.get(i - 1), allTokens.get(i) );
            ranges.add(range);
        }
        Range range = new Range( allTokens.get(size - 1), allTokens.get(0) );
        ranges.add(range);
        return ranges.toArray( new Range[0] );
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
        Map<Token, InetAddress> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        if (tokens.size() > 0)
        {
            Collections.sort(tokens);
            int index = Collections.binarySearch(tokens, token);
            if (index >= 0)
            {
                /*
                 * retrieve the endpoint based on the token at this index in the
                 * tokens list
                 */
                endpoint = tokenToEndPointMap.get(tokens.get(index));
            }
            else
            {
                index = (index + 1) * (-1);
                if (index < tokens.size())
                    endpoint = tokenToEndPointMap.get(tokens.get(index));
                else
                    endpoint = tokenToEndPointMap.get(tokens.get(0));
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

    Map<Token, InetAddress> getLiveEndPointMap()
    {
        return tokenMetadata_.cloneTokenEndPointMap();
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
        for (int i = 1; i < splits; i++)
        {
            int index = i * (decoratedKeys.size() / splits);
            tokens.add(decoratedKeys.get(index).token.toString());
        }

        tokens.add(range.right().toString());
        return tokens;
    }

    public <T> QuorumResponseHandler<T> getResponseHandler(IResponseResolver<T> responseResolver, int blockFor, int consistency_level) throws InvalidRequestException, UnavailableException
    {
        return replicationStrategy_.getResponseHandler(responseResolver, blockFor, consistency_level);
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy_;
    }
}
