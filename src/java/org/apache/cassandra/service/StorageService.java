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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.tools.MembershipCleanerVerbHandler;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public final class StorageService implements IEndPointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = Logger.getLogger(StorageService.class);     
    private final static String nodeId_ = "NODE-IDENTIFIER";
    private final static String BOOTSTRAP_MODE = "BOOTSTRAP-MODE";
    /* Gossip load after every 5 mins. */
    private static final long threshold_ = 5 * 60 * 1000L;
    
    /* All stage identifiers */
    public final static String mutationStage_ = "ROW-MUTATION-STAGE";
    public final static String readStage_ = "ROW-READ-STAGE";
    
    /* All verb handler identifiers */
    public final static String mutationVerbHandler_ = "ROW-MUTATION-VERB-HANDLER";
    public final static String tokenVerbHandler_ = "TOKEN-VERB-HANDLER";
    public final static String loadVerbHandler_ = "LOAD-VERB-HANDLER";
    public final static String binaryVerbHandler_ = "BINARY-VERB-HANDLER";
    public final static String readRepairVerbHandler_ = "READ-REPAIR-VERB-HANDLER";
    public final static String readVerbHandler_ = "ROW-READ-VERB-HANDLER";
    public final static String bootStrapInitiateVerbHandler_ = "BOOTSTRAP-INITIATE-VERB-HANDLER";
    public final static String bootStrapInitiateDoneVerbHandler_ = "BOOTSTRAP-INITIATE-DONE-VERB-HANDLER";
    public final static String bootStrapTerminateVerbHandler_ = "BOOTSTRAP-TERMINATE-VERB-HANDLER";
    public final static String dataFileVerbHandler_ = "DATA-FILE-VERB-HANDLER";
    public final static String mbrshipCleanerVerbHandler_ = "MBRSHIP-CLEANER-VERB-HANDLER";
    public final static String bsMetadataVerbHandler_ = "BS-METADATA-VERB-HANDLER";
    public final static String calloutDeployVerbHandler_ = "CALLOUT-DEPLOY-VERB-HANDLER";
    public final static String rangeVerbHandler_ = "RANGE-VERB-HANDLER";

    public static enum ConsistencyLevel
    {
    	WEAK,
    	STRONG
    }

    private static StorageService instance_;
    /* Used to lock the factory for creation of StorageService instance */
    private static Lock createLock_ = new ReentrantLock();
    private static EndPoint tcpAddr_;
    private static EndPoint udpAddr_;
    private static IPartitioner partitioner_;

    public static EndPoint getLocalStorageEndPoint()
    {
        return tcpAddr_;
    }

    public static EndPoint getLocalControlEndPoint()
    {
        return udpAddr_;
    }

    public static IPartitioner getPartitioner() {
        return partitioner_;
    }
    
    static
    {
        partitioner_ = DatabaseDescriptor.getPartitioner();
    }


    public static class BootstrapInitiateDoneVerbHandler implements IVerbHandler
    {
        private static Logger logger_ = Logger.getLogger( BootstrapInitiateDoneVerbHandler.class );

        public void doVerb(Message message)
        {
            if (logger_.isDebugEnabled())
              logger_.debug("Received a bootstrap initiate done message ...");
            /* Let the Stream Manager do his thing. */
            StreamManager.instance(message.getFrom()).start();            
        }
    }

    /*
     * Factory method that gets an instance of the StorageService
     * class.
    */
    public static StorageService instance()
    {
        String bs = System.getProperty("bootstrap");
        boolean bootstrap = bs != null && bs.contains("true");
        
        if ( instance_ == null )
        {
            StorageService.createLock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    try
                    {
                        instance_ = new StorageService(bootstrap);
                    }
                    catch ( Throwable th )
                    {
                        logger_.error(LogUtil.throwableToString(th));
                        System.exit(1);
                    }
                }
            }
            finally
            {
                createLock_.unlock();
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

    /* Timer is used to disseminate load information */
    private Timer loadTimer_ = new Timer(false);

    /* This thread pool is used to do the bootstrap for a new node */
    private ExecutorService bootStrapper_ = new DebuggableThreadPoolExecutor("BOOT-STRAPPER");
    
    /* This thread pool does consistency checks when the client doesn't care about consistency */
    private ExecutorService consistencyManager_;

    /* This is the entity that tracks load information of all nodes in the cluster */
    private StorageLoadBalancer storageLoadBalancer_;
    /* We use this interface to determine where replicas need to be placed */
    private IReplicaPlacementStrategy nodePicker_;
    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    private Set<EndPoint> bootstrapSet;
  
    public synchronized void addBootstrapSource(EndPoint s)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Added " + s.getHost() + " as a bootstrap source");
        bootstrapSet.add(s);
    }
    
    public synchronized boolean removeBootstrapSource(EndPoint s)
    {
        bootstrapSet.remove(s);

        if (logger_.isDebugEnabled())
            logger_.debug("Removed " + s.getHost() + " as a bootstrap source");
        if (bootstrapSet.isEmpty())
        {
            isBootstrapMode = false;
            tokenMetadata_.update(storageMetadata_.getToken(), StorageService.tcpAddr_, false);

            logger_.info("Bootstrap completed! Now serving reads.");
            /* Tell others you're not bootstrapping anymore */
            Gossiper.instance().deleteApplicationState(BOOTSTRAP_MODE);
        }
        return isBootstrapMode;
    }

    /*
     * Registers with Management Server
     */
    private void init()
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(
                    "org.apache.cassandra.service:type=StorageService"));
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
        }
    }

    public StorageService(boolean isBootstrapMode)
    {
        this.isBootstrapMode = isBootstrapMode;
        bootstrapSet = new HashSet<EndPoint>();
        init();
        storageLoadBalancer_ = new StorageLoadBalancer(this);
        endPointSnitch_ = DatabaseDescriptor.getEndPointSnitch();

        /* register the verb handlers */
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.tokenVerbHandler_, new TokenUpdateVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.binaryVerbHandler_, new BinaryVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.loadVerbHandler_, new LoadVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.mutationVerbHandler_, new RowMutationVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.readRepairVerbHandler_, new ReadRepairVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.readVerbHandler_, new ReadVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bootStrapInitiateVerbHandler_, new Table.BootStrapInitiateVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bootStrapInitiateDoneVerbHandler_, new StorageService.BootstrapInitiateDoneVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bootStrapTerminateVerbHandler_, new StreamManager.BootstrapTerminateVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.dataFileVerbHandler_, new DataFileVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.mbrshipCleanerVerbHandler_, new MembershipCleanerVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bsMetadataVerbHandler_, new BootstrapMetadataVerbHandler() );        
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.calloutDeployVerbHandler_, new CalloutDeployVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.rangeVerbHandler_, new RangeVerbHandler());
        
        /* register the stage for the mutations */
        consistencyManager_ = new DebuggableThreadPoolExecutor(DatabaseDescriptor.getConsistencyThreads(),
                                                               DatabaseDescriptor.getConsistencyThreads(),
                                                               Integer.MAX_VALUE, TimeUnit.SECONDS,
                                                               new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl("CONSISTENCY-MANAGER"));
        
        StageManager.registerStage(StorageService.mutationStage_,
                                   new MultiThreadedStage(StorageService.mutationStage_, DatabaseDescriptor.getConcurrentWriters()));
        StageManager.registerStage(StorageService.readStage_,
                                   new MultiThreadedStage(StorageService.readStage_, DatabaseDescriptor.getConcurrentReaders()));

        Class cls = DatabaseDescriptor.getReplicaPlacementStrategyClass();
        Class [] parameterTypes = new Class[] { TokenMetadata.class, IPartitioner.class, int.class, int.class};
        try
        {
            nodePicker_ = (IReplicaPlacementStrategy) cls.getConstructor(parameterTypes).newInstance(tokenMetadata_, partitioner_, DatabaseDescriptor.getReplicationFactor(), DatabaseDescriptor.getStoragePort());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start() throws IOException
    {
        storageMetadata_ = SystemTable.initMetadata();
        tcpAddr_ = new EndPoint(DatabaseDescriptor.getStoragePort());
        udpAddr_ = new EndPoint(DatabaseDescriptor.getControlPort());
        /* Listen for application messages */
        MessagingService.getMessagingInstance().listen(tcpAddr_);
        /* Listen for control messages */
        MessagingService.getMessagingInstance().listenUDP(udpAddr_);

        SelectorManager.getSelectorManager().start();
        SelectorManager.getUdpSelectorManager().start();

        /* starts a load timer thread */
        loadTimer_.schedule( new LoadDisseminator(), StorageService.threshold_, StorageService.threshold_);
        
        /* Start the storage load balancer */
        storageLoadBalancer_.start();
        /* Register with the Gossiper for EndPointState notifications */
        Gossiper.instance().register(this);
        /*
         * Start the gossiper with the generation # retrieved from the System
         * table
         */
        Gossiper.instance().start(udpAddr_, storageMetadata_.getGeneration());
        /* Make sure this token gets gossiped around. */
        tokenMetadata_.update(storageMetadata_.getToken(), StorageService.tcpAddr_, isBootstrapMode);
        ApplicationState state = new ApplicationState(StorageService.getPartitioner().getTokenFactory().toString(storageMetadata_.getToken()));
        Gossiper.instance().addApplicationState(StorageService.nodeId_, state);
        if (isBootstrapMode)
        {
            logger_.info("Starting in bootstrap mode");
            doBootstrap(StorageService.getLocalStorageEndPoint());
            Gossiper.instance().addApplicationState(BOOTSTRAP_MODE, new ApplicationState(""));
        }
    }
    
    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata_.cloneMe();
    }

    /* TODO: used for testing */
    public void updateTokenMetadata(Token token, EndPoint endpoint)
    {
        tokenMetadata_.update(token, endpoint);
    }

    public IEndPointSnitch getEndPointSnitch()
    {
    	return endPointSnitch_;
    }
    
    /*
     * Given an EndPoint this method will report if the
     * endpoint is in the same data center as the local
     * storage endpoint.
    */
    public boolean isInSameDataCenter(EndPoint endpoint) throws IOException
    {
        return endPointSnitch_.isInSameDataCenter(StorageService.tcpAddr_, endpoint);
    }
    
    /*
     * This method performs the requisite operations to make
     * sure that the N replicas are in sync. We do this in the
     * background when we do not care much about consistency.
     */
    public void doConsistencyCheck(Row row, List<EndPoint> endpoints, ReadCommand command)
    {
        Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, command);
        consistencyManager_.submit(consistencySentinel);
    }

    public Map<Range, List<EndPoint>> getRangeToEndPointMap()
    {
        /* Get the token to endpoint map. */
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        /* All the ranges for the tokens */
        Range[] ranges = getAllRanges(tokenToEndPointMap.keySet());
        return constructRangeToEndPointMap(ranges);
    }

    /**
     * Construct the range to endpoint mapping based on the true view 
     * of the world. 
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    public Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges)
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
        for ( Range range : ranges )
        {
            EndPoint[] endpoints = getNStorageEndPoint(range.right());
            rangeToEndPointMap.put(range, new ArrayList<EndPoint>( Arrays.asList(endpoints) ) );
        }
        if (logger_.isDebugEnabled())
          logger_.debug("Done constructing range to endpoint map ...");
        return rangeToEndPointMap;
    }
    
    /**
     * Construct the range to endpoint mapping based on the view as dictated
     * by the mapping of token to endpoints passed in. 
     * @param ranges
     * @param tokenToEndPointMap mapping of token to endpoints.
     * @return mapping of ranges to the replicas responsible for them.
    */
    public Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges, Map<Token, EndPoint> tokenToEndPointMap)
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
        for ( Range range : ranges )
        {
            EndPoint[] endpoints = getNStorageEndPoint(range.right(), tokenToEndPointMap);
            rangeToEndPointMap.put(range, new ArrayList<EndPoint>( Arrays.asList(endpoints) ) );
        }
        if (logger_.isDebugEnabled())
          logger_.debug("Done constructing range to endpoint map ...");
        return rangeToEndPointMap;
    }
    
    /**
     * Construct a mapping from endpoint to ranges that endpoint is
     * responsible for.
     * @return the mapping from endpoint to the ranges it is responsible
     * for.
     */
    public Map<EndPoint, List<Range>> constructEndPointToRangesMap()
    {
        Map<EndPoint, List<Range>> endPointToRangesMap = new HashMap<EndPoint, List<Range>>();
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Collection<EndPoint> mbrs = tokenToEndPointMap.values();
        for ( EndPoint mbr : mbrs )
        {
            endPointToRangesMap.put(mbr, getRangesForEndPoint(mbr));
        }
        return endPointToRangesMap;
    }

    /**
     *  Called when there is a change in application state. In particular
     *  we are interested in new tokens as a result of a new node or an
     *  existing node moving to a new location on the ring.
    */
    public void onChange(EndPoint endpoint, EndPointState epState)
    {
        EndPoint ep = new EndPoint(endpoint.getHost(), DatabaseDescriptor.getStoragePort());
        /* node identifier for this endpoint on the identifier space */
        ApplicationState nodeIdState = epState.getApplicationState(StorageService.nodeId_);
        /* Check if this has a bootstrapping state message */
        boolean bootstrapState = epState.getApplicationState(StorageService.BOOTSTRAP_MODE) != null;
        if (bootstrapState)
        {
            if (logger_.isDebugEnabled())
                logger_.debug(ep.getHost() + " is in bootstrap state.");
        }
        if (nodeIdState != null)
        {
            Token newToken = getPartitioner().getTokenFactory().fromString(nodeIdState.getState());
            if (logger_.isDebugEnabled())
              logger_.debug("CHANGE IN STATE FOR " + endpoint + " - has token " + nodeIdState.getState());
            Token oldToken = tokenMetadata_.getToken(ep);

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
                      logger_.debug("Relocation for endpoint " + ep);
                    tokenMetadata_.update(newToken, ep, bootstrapState);                    
                }
                else
                {
                    /*
                     * This means the node crashed and is coming back up.
                     * Deliver the hints that we have for this endpoint.
                    */
                    if (logger_.isDebugEnabled())
                      logger_.debug("Sending hinted data to " + ep);
                    deliverHints(endpoint);
                }
            }
            else
            {
                /*
                 * This is a new node and we just update the token map.
                */
                tokenMetadata_.update(newToken, ep, bootstrapState);
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
                  logger_.debug("EndPoint " + ep + " just recovered from a partition. Sending hinted data.");
                deliverHints(ep);
            }
        }
    }

    /**
     * Get the count of primary keys from the sampler.
    */
    public String getLoadInfo()
    {
        long diskSpace = FileUtils.getUsedDiskSpace();
    	return FileUtils.stringifyFileSize(diskSpace);
    }

    /**
     * Get the primary count info for this endpoint.
     * This is gossiped around and cached in the
     * StorageLoadBalancer.
    */
    public String getLoadInfo(EndPoint ep)
    {
        LoadInfo li = storageLoadBalancer_.getLoad(ep);
        return ( li == null ) ? "N/A" : li.toString();
    }

    /*
     * This method updates the token on disk and modifies the cached
     * StorageMetadata instance. This is only for the local endpoint.
    */
    public void updateToken(Token token) throws IOException
    {
        /* update the token on disk */
        SystemTable.updateToken(token);
        /* Update the token maps */
        /* Get the old token. This needs to be removed. */
        tokenMetadata_.update(token, StorageService.tcpAddr_);
        /* Gossip this new token for the local storage instance */
        ApplicationState state = new ApplicationState(StorageService.getPartitioner().getTokenFactory().toString(token));
        Gossiper.instance().addApplicationState(StorageService.nodeId_, state);
    }
    
    /*
     * This method removes the state associated with this endpoint
     * from the TokenMetadata instance.
     * 
     *  @param endpoint remove the token state associated with this 
     *         endpoint.
     */
    public void removeTokenState(EndPoint endpoint) 
    {
        tokenMetadata_.remove(endpoint);
        /* Remove the state from the Gossiper */
        Gossiper.instance().removeFromMembership(endpoint);
    }
    
    /*
     * This method is invoked by the Loader process to force the
     * node to move from its current position on the token ring, to
     * a position to be determined based on the keys. This will help
     * all nodes to start off perfectly load balanced. The array passed
     * in is evaluated as follows by the loader process:
     * If there are 10 keys in the system and a totality of 5 nodes
     * then each node needs to have 2 keys i.e the array is made up
     * of every 2nd key in the total list of keys.
    */
    public void relocate(String[] keys) throws IOException
    {
    	if ( keys.length > 0 )
    	{
            Token token = tokenMetadata_.getToken(StorageService.tcpAddr_);
	        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
	        Token[] tokens = tokenToEndPointMap.keySet().toArray(new Token[tokenToEndPointMap.keySet().size()]);
	        Arrays.sort(tokens);
	        int index = Arrays.binarySearch(tokens, token) * (keys.length/tokens.length);
            Token newToken = partitioner_.getToken(keys[index]);
	        /* update the token */
	        updateToken(newToken);
    	}
    }
    
    /**
     * This method takes a colon separated string of nodes that need
     * to be bootstrapped. * <i>nodes</i> must be specified as A:B:C.
     * @throws UnknownHostException 
     * 
    */
    private void doBootstrap(String nodes) throws UnknownHostException
    {
        String[] allNodes = nodes.split(":");
        EndPoint[] endpoints = new EndPoint[allNodes.length];
        Token[] tokens = new Token[allNodes.length];
        
        for ( int i = 0; i < allNodes.length; ++i )
        {
            String host = allNodes[i].trim();
            InetAddress ip = InetAddress.getByName(host);
            host = ip.getHostAddress();
            endpoints[i] = new EndPoint( host, DatabaseDescriptor.getStoragePort() );
            tokens[i] = tokenMetadata_.getToken(endpoints[i]);
        }
        
        /* Start the bootstrap algorithm */
        bootStrapper_.submit( new BootStrapper(endpoints, tokens) );
    }

    /**
     * Starts the bootstrap operations for the specified endpoint.
     * @param endpoint
     */
    public final void doBootstrap(EndPoint endpoint)
    {
        Token token = tokenMetadata_.getToken(endpoint);
        bootStrapper_.submit(new BootStrapper(new EndPoint[]{endpoint}, token));
    }
    
    /**
     * Deliver hints to the specified node when it has crashed
     * and come back up/ marked as alive after a network partition
    */
    public final void deliverHints(EndPoint endpoint)
    {
        HintedHandOffManager.instance().deliverHints(endpoint);
    }

    /* This methods belong to the MBean interface */
    
    public String getToken(EndPoint ep)
    {
        // render a String representation of the Token corresponding to this endpoint
        // for a human-facing UI.  If there is no such Token then we use "" since
        // it is not a valid value either for BigIntegerToken or StringToken.
        EndPoint ep2 = new EndPoint(ep.getHost(), DatabaseDescriptor.getStoragePort());
        Token token = tokenMetadata_.getToken(ep2);
        // if there is no token for an endpoint, return an empty string to denote that
        return ( token == null ) ? "" : token.toString();
    }

    public String getToken()
    {
        return tokenMetadata_.getToken(StorageService.tcpAddr_).toString();
    }

    public String getLiveNodes()
    {
        return stringify(Gossiper.instance().getLiveMembers());
    }

    public String getUnreachableNodes()
    {
        return stringify(Gossiper.instance().getUnreachableMembers());
    }
    
    public int getCurrentGenerationNumber()
    {
        return Gossiper.instance().getCurrentGenerationNumber(udpAddr_);
    }

    /* Helper for the MBean interface */
    private String stringify(Set<EndPoint> eps)
    {
        StringBuilder sb = new StringBuilder("");
        for (EndPoint ep : eps)
        {
            sb.append(ep);
            sb.append(" ");
        }
        return sb.toString();
    }

    public void loadAll(String nodes) throws UnknownHostException
    {        
        doBootstrap(nodes);
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
    
    /**
     * Trigger the immediate compaction of all tables.
     */
    public void forceTableCompaction() throws IOException
    {
        List<String> tables = DatabaseDescriptor.getTables();
        for ( String tName : tables )
        {
            Table table = Table.open(tName);
            table.forceCompaction();
        }        
    }
    
    public void forceHandoff(List<String> dataDirectories, String host) throws IOException
    {       
        List<File> filesList = new ArrayList<File>();
        List<StreamContextManager.StreamContext> streamContexts = new ArrayList<StreamContextManager.StreamContext>();
        
        for (String dataDir : dataDirectories)
        {
            File directory = new File(dataDir);
            Collections.addAll(filesList, directory.listFiles());            
        

            for (File tableDir : directory.listFiles())
            {
                String tableName = tableDir.getName();

                for (File file : tableDir.listFiles())
                {
                    streamContexts.add(new StreamContextManager.StreamContext(file.getAbsolutePath(), file.length(), tableName));
                    if (logger_.isDebugEnabled())
                      logger_.debug("Stream context metadata " + streamContexts);
                }
            }
        }
        
        if ( streamContexts.size() > 0 )
        {
            EndPoint target = new EndPoint(host, DatabaseDescriptor.getStoragePort());
            /* Set up the stream manager with the files that need to streamed */
            final StreamContextManager.StreamContext[] contexts = streamContexts.toArray(new StreamContextManager.StreamContext[streamContexts.size()]);
            StreamManager.instance(target).addFilesToStream(contexts);
            /* Send the bootstrap initiate message */
            final StreamContextManager.StreamContext[] bootContexts = streamContexts.toArray(new StreamContextManager.StreamContext[streamContexts.size()]);
            BootstrapInitiateMessage biMessage = new BootstrapInitiateMessage(bootContexts);
            Message message = BootstrapInitiateMessage.makeBootstrapInitiateMessage(biMessage);
            if (logger_.isDebugEnabled())
              logger_.debug("Sending a bootstrap initiate message to " + target + " ...");
            MessagingService.getMessagingInstance().sendOneWay(message, target);                
            if (logger_.isDebugEnabled())
              logger_.debug("Waiting for transfer to " + target + " to complete");
            StreamManager.instance(target).waitForStreamCompletion();
            if (logger_.isDebugEnabled())
              logger_.debug("Done with transfer to " + target);  
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

    /* End of MBean interface methods */
    
    /**
     * This method returns the predecessor of the endpoint ep on the identifier
     * space.
     */
    EndPoint getPredecessor(EndPoint ep)
    {
        Token token = tokenMetadata_.getToken(ep);
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        return (index == 0) ? tokenToEndPointMap.get(tokens
                .get(tokens.size() - 1)) : tokenToEndPointMap.get(tokens
                .get(--index));
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    public EndPoint getSuccessor(EndPoint ep)
    {
        Token token = tokenMetadata_.getToken(ep);
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        return (index == (tokens.size() - 1)) ? tokenToEndPointMap
                .get(tokens.get(0))
                : tokenToEndPointMap.get(tokens.get(++index));
    }

    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    public Range getPrimaryRangeForEndPoint(EndPoint ep)
    {
        Token right = tokenMetadata_.getToken(ep);
        EndPoint predecessor = getPredecessor(ep);
        Token left = tokenMetadata_.getToken(predecessor);
        return new Range(left, right);
    }
    
    /**
     * Get all ranges an endpoint is responsible for.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    List<Range> getRangesForEndPoint(EndPoint ep)
    {
        List<Range> ranges = new ArrayList<Range>();
        ranges.add( getPrimaryRangeForEndPoint(ep) );
        
        EndPoint predecessor = ep;
        int count = DatabaseDescriptor.getReplicationFactor() - 1;
        for ( int i = 0; i < count; ++i )
        {
            predecessor = getPredecessor(predecessor);
            ranges.add( getPrimaryRangeForEndPoint(predecessor) );
        }
        
        return ranges;
    }
    
    /**
     * Get all ranges that span the ring as per
     * current snapshot of the token distribution.
     * @return all ranges in sorted order.
     */
    public Range[] getAllRanges()
    {
        return getAllRanges(tokenMetadata_.cloneTokenEndPointMap().keySet());
    }
    
    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of 
     * ranges.
     * @return ranges in sorted order
    */
    public Range[] getAllRanges(Set<Token> tokens)
    {
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
    public EndPoint getPrimary(String key)
    {
        EndPoint endpoint = StorageService.tcpAddr_;
        Token token = partitioner_.getToken(key);
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
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
        EndPoint endpoint = getPrimary(key);
        return StorageService.tcpAddr_.equals(endpoint);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public EndPoint[] getNStorageEndPoint(String key)
    {
        return nodePicker_.getStorageEndPoints(partitioner_.getToken(key));
    }
    
    private Map<String, EndPoint[]> getNStorageEndPoints(String[] keys)
    {
    	return nodePicker_.getStorageEndPoints(keys);
    }
    
    
    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<EndPoint> getNLiveStorageEndPoint(String key)
    {
    	List<EndPoint> liveEps = new ArrayList<EndPoint>();
    	EndPoint[] endpoints = getNStorageEndPoint(key);
    	
    	for ( EndPoint endpoint : endpoints )
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
    public Map<EndPoint, EndPoint> getNStorageEndPointMap(String key)
    {
        return nodePicker_.getHintedStorageEndPoints(partitioner_.getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified token i.e for replication.
     *
     * @param token - position on the ring
     */
    public EndPoint[] getNStorageEndPoint(Token token)
    {
        return nodePicker_.getStorageEndPoints(token);
    }
    
    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified token i.e for replication and are based on the token to endpoint 
     * mapping that is passed in.
     *
     * @param token - position on the ring
     * @param tokens - w/o the following tokens in the token list
     */
    protected EndPoint[] getNStorageEndPoint(Token token, Map<Token, EndPoint> tokenToEndPointMap)
    {
        return nodePicker_.getStorageEndPoints(token, tokenToEndPointMap);
    }
    
    /**
     * This function finds the most suitable endpoint given a key.
     * It checks for locality and alive test.
     */
	public EndPoint findSuitableEndPoint(String key) throws IOException
	{
		EndPoint[] endpoints = getNStorageEndPoint(key);
		for(EndPoint endPoint: endpoints)
		{
			if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
			{
				return endPoint;
			}
		}
		int j = 0;
		for ( ; j < endpoints.length; ++j )
		{
			if ( StorageService.instance().isInSameDataCenter(endpoints[j]) && FailureDetector.instance().isAlive(endpoints[j]))
			{
				return endpoints[j];
			}
		}
		// We have tried to be really nice but looks like there are no servers 
		// in the local data center that are alive and can service this request so 
		// just send it to the first alive guy and see if we get anything.
		j = 0;
		for ( ; j < endpoints.length; ++j )
		{
			if ( FailureDetector.instance().isAlive(endpoints[j]))
			{
				if (logger_.isDebugEnabled())
				  logger_.debug("EndPoint " + endpoints[j] + " is alive so get data from it.");
				return endpoints[j];
			}
		}
		return null;
	}
	
	/*
	 * TODO:
	 * This is used by the incomplete multiget implementation. Need to rewrite
	 * this to use findSuitableEndPoint above instead of copy/paste 
	 */
	public Map<String, EndPoint> findSuitableEndPoints(String[] keys) throws IOException
	{
		Map<String, EndPoint> suitableEndPoints = new HashMap<String, EndPoint>();
		Map<String, EndPoint[]> results = getNStorageEndPoints(keys);
		for ( String key : keys )
		{
			EndPoint[] endpoints = results.get(key);
			/* indicates if we have to move on to the next key */
			boolean moveOn = false;
			for(EndPoint endPoint: endpoints)
			{
				if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
				{
					suitableEndPoints.put(key, endPoint);
					moveOn = true;
					break;
				}
			}
			
			if ( moveOn )
				continue;
				
			int j = 0;
			for ( ; j < endpoints.length; ++j )
			{
				if ( StorageService.instance().isInSameDataCenter(endpoints[j]) && FailureDetector.instance().isAlive(endpoints[j]) )
				{
					if (logger_.isDebugEnabled())
					  logger_.debug("EndPoint " + endpoints[j] + " is in the same data center as local storage endpoint.");
					suitableEndPoints.put(key, endpoints[j]);
					moveOn = true;
					break;
				}
			}
			
			if ( moveOn )
				continue;
			
			// We have tried to be really nice but looks like there are no servers 
			// in the local data center that are alive and can service this request so 
			// just send it to the first alive guy and see if we get anything.
			j = 0;
			for ( ; j < endpoints.length; ++j )
			{
				if ( FailureDetector.instance().isAlive(endpoints[j]) )
				{
					if (logger_.isDebugEnabled())
					  logger_.debug("EndPoint " + endpoints[j] + " is alive so get data from it.");
					suitableEndPoints.put(key, endpoints[j]);
					break;
				}
			}
		}
		return suitableEndPoints;
	}
}
