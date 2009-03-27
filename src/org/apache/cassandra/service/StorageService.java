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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import org.apache.cassandra.analytics.AnalyticsContext;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.MultiThreadedStage;
import org.apache.cassandra.concurrent.SingleThreadedStage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BinaryVerbHandler;
import org.apache.cassandra.db.CalloutDeployVerbHandler;
import org.apache.cassandra.db.DBManager;
import org.apache.cassandra.db.DataFileVerbHandler;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.LoadVerbHandler;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.ReadVerbHandler;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutationVerbHandler;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TouchVerbHandler;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.BootstrapInitiateMessage;
import org.apache.cassandra.dht.BootstrapMetadataVerbHandler;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndPointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndPointStateChangeSubscriber;
import org.apache.cassandra.locator.EndPointSnitch;
import org.apache.cassandra.locator.IEndPointSnitch;
import org.apache.cassandra.locator.IReplicaPlacementStrategy;
import org.apache.cassandra.locator.RackAwareStrategy;
import org.apache.cassandra.locator.RackUnawareStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.http.HttpConnection;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.tools.MembershipCleanerVerbHandler;
import org.apache.cassandra.tools.TokenUpdateVerbHandler;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public final class StorageService implements IEndPointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = Logger.getLogger(StorageService.class);     
    private final static String nodeId_ = "NODE-IDENTIFIER";
    private final static String loadAll_ = "LOAD-ALL";
    /* Gossip load after every 5 mins. */
    private static final long threshold_ = 5 * 60 * 1000L;
    
    /* All stage identifiers */
    public final static String mutationStage_ = "ROW-MUTATION-STAGE";
    public final static String readStage_ = "ROW-READ-STAGE";
    public final static String mrStage_ = "MAP-REDUCE-STAGE";
    
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
    public final static String tokenInfoVerbHandler_ = "TOKEN-INFO-VERB-HANDLER";
    public final static String locationInfoVerbHandler_ = "LOCATION-INFO-VERB-HANDLER";
    public final static String dataFileVerbHandler_ = "DATA-FILE-VERB-HANDLER";
    public final static String mbrshipCleanerVerbHandler_ = "MBRSHIP-CLEANER-VERB-HANDLER";
    public final static String bsMetadataVerbHandler_ = "BS-METADATA-VERB-HANDLER";
    public final static String calloutDeployVerbHandler_ = "CALLOUT-DEPLOY-VERB-HANDLER";
    public final static String touchVerbHandler_ = "TOUCH-VERB-HANDLER";
    public static String rangeVerbHandler_ = "RANGE-VERB-HANDLER";

    public static enum ConsistencyLevel
    {
    	WEAK,
    	STRONG
    };
    
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

    public static String getHostUrl()
    {
        return "http://" + tcpAddr_.getHost() + ":" + DatabaseDescriptor.getHttpPort();
    }

    /**
     * This is a facade for the hashing 
     * function used by the system for
     * partitioning.
    */
    public static Token token(String key)
    {
        return partitioner_.getTokenForKey(key);
    }

    public static IPartitioner getPartitioner() {
        return partitioner_;
    }
    
    public static enum BootstrapMode
    {
        HINT,
        FULL
    };
    
    public static class BootstrapInitiateDoneVerbHandler implements IVerbHandler
    {
        private static Logger logger_ = Logger.getLogger( BootstrapInitiateDoneVerbHandler.class );

        public void doVerb(Message message)
        {
            logger_.debug("Received a bootstrap initiate done message ...");
            /* Let the Stream Manager do his thing. */
            StreamManager.instance(message.getFrom()).start();            
        }
    }

    private class ShutdownTimerTask extends TimerTask
    {
    	public void run()
    	{
    		StorageService.instance().shutdown();
    	}
    }

    /*
     * Factory method that gets an instance of the StorageService
     * class.
    */
    public static StorageService instance()
    {
        if ( instance_ == null )
        {
            StorageService.createLock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    try
                    {
                        instance_ = new StorageService();
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
    private DBManager.StorageMetadata storageMetadata_;

    /*
     * Maintains a list of all components that need to be shutdown
     * for a clean exit.
    */
    private Set<IComponentShutdown> components_ = new HashSet<IComponentShutdown>();
    /* Timer is used to disseminate load information */
    private Timer loadTimer_ = new Timer(false);

    /*
     * This variable indicates if the local storage instance
     * has been shutdown.
    */
    private AtomicBoolean isShutdown_ = new AtomicBoolean(false);

    /* This thread pool is used to do the bootstrap for a new node */
    private ExecutorService bootStrapper_ = new DebuggableThreadPoolExecutor(1, 1,
            Integer.MAX_VALUE, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl(
                    "BOOT-STRAPPER"));
    
    /* This thread pool does consistency checks when the client doesn't care about consistency */
    private ExecutorService consistencyManager_;

    /* This is the entity that tracks load information of all nodes in the cluster */
    private StorageLoadBalancer storageLoadBalancer_;
    /* We use this interface to determine where replicas need to be placed */
    private IReplicaPlacementStrategy nodePicker_;
    /* Handle to a ZooKeeper instance */
    private ZooKeeper zk_;
    
    /*
     * Registers with Management Server
     */
    private void init()
    {
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(
                    "com.facebook.infrastructure.service:type=StorageService"));
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
        }
    }

    public StorageService() throws Throwable
    {
        init();
        storageLoadBalancer_ = new StorageLoadBalancer(this);
        endPointSnitch_ = new EndPointSnitch();
        
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
        MessagingService.getMessagingInstance().registerVerbHandlers(HttpConnection.httpRequestVerbHandler_, new HttpRequestVerbHandler(this) );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.dataFileVerbHandler_, new DataFileVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.mbrshipCleanerVerbHandler_, new MembershipCleanerVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bsMetadataVerbHandler_, new BootstrapMetadataVerbHandler() );        
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.calloutDeployVerbHandler_, new CalloutDeployVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.touchVerbHandler_, new TouchVerbHandler());
        
        /* register the stage for the mutations */
        int threadCount = DatabaseDescriptor.getThreadsPerPool();
        consistencyManager_ = new DebuggableThreadPoolExecutor(threadCount,
        		threadCount,
                Integer.MAX_VALUE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl(
                        "CONSISTENCY-MANAGER"));
        
        StageManager.registerStage(StorageService.mutationStage_, new MultiThreadedStage(StorageService.mutationStage_, threadCount));
        StageManager.registerStage(StorageService.readStage_, new MultiThreadedStage(StorageService.readStage_, 2*threadCount));        
        StageManager.registerStage(StorageService.mrStage_, new MultiThreadedStage(StorageService.mrStage_, threadCount));
        /* Stage for handling the HTTP messages. */
        StageManager.registerStage(HttpConnection.httpStage_, new SingleThreadedStage("HTTP-REQUEST"));

        if ( DatabaseDescriptor.isRackAware() )
            nodePicker_ = new RackAwareStrategy(tokenMetadata_);
        else
            nodePicker_ = new RackUnawareStrategy(tokenMetadata_);
    }
    
    private void reportToZookeeper() throws Throwable
    {
        try
        {
            zk_ = new ZooKeeper(DatabaseDescriptor.getZkAddress(), DatabaseDescriptor.getZkSessionTimeout(), new Watcher()
                {
                    public void process(WatchedEvent we)
                    {                    
                        String path = "/Cassandra/" + DatabaseDescriptor.getClusterName() + "/Leader";
                        String eventPath = we.getPath();
                        logger_.debug("PROCESS EVENT : " + eventPath);
                        if ( eventPath != null && (eventPath.indexOf(path) != -1) )
                        {                                                           
                            logger_.debug("Signalling the leader instance ...");
                            LeaderElector.instance().signal();                                        
                        }                                                  
                    }
                });
            
            Stat stat = zk_.exists("/", false);
            if ( stat != null )
            {
                stat = zk_.exists("/Cassandra", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the Cassandra znode ...");
                    zk_.create("/Cassandra", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                
                String path = "/Cassandra/" + DatabaseDescriptor.getClusterName();
                stat = zk_.exists(path, false);
                if ( stat == null )
                {
                    logger_.debug("Creating the cluster znode " + path);
                    zk_.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                
                /* Create the Leader, Locks and Misc znode */
                stat = zk_.exists(path + "/Leader", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the leader znode " + path);
                    zk_.create(path + "/Leader", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                
                stat = zk_.exists(path + "/Locks", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the locks znode " + path);
                    zk_.create(path + "/Locks", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                                
                stat = zk_.exists(path + "/Misc", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the misc znode " + path);
                    zk_.create(path + "/Misc", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
        }
        catch ( KeeperException ke )
        {
            LogUtil.throwableToString(ke);
            /* do the re-initialize again. */
            reportToZookeeper();
        }
    }
    
    protected ZooKeeper getZooKeeperHandle()
    {
        return zk_;
    }
    
    public boolean isLeader(EndPoint endpoint)
    {
        EndPoint leader = getLeader();
        return leader.equals(endpoint);
    }
    
    public EndPoint getLeader()
    {
        return LeaderElector.instance().getLeader();
    }

    public void registerComponentForShutdown(IComponentShutdown component)
    {
    	components_.add(component);
    }

    static
    {
        try
        {
            Class cls = Class.forName(DatabaseDescriptor.getPartitionerClass());
            partitioner_ = (IPartitioner) cls.getConstructor().newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public void start() throws Throwable
    {
        /* Start the DB */
        storageMetadata_ = DBManager.instance().start();  
        /* Set up TCP endpoint */
        tcpAddr_ = new EndPoint(DatabaseDescriptor.getStoragePort());
        /* Set up UDP endpoint */
        udpAddr_ = new EndPoint(DatabaseDescriptor.getControlPort());
        /* Listen for application messages */
        MessagingService.getMessagingInstance().listen(tcpAddr_, false);
        /* Listen for control messages */
        MessagingService.getMessagingInstance().listenUDP(udpAddr_);
        /* Listen for HTTP messages */
        MessagingService.getMessagingInstance().listen( new EndPoint(DatabaseDescriptor.getHttpPort() ), true );
        /* start the analytics context package */
        AnalyticsContext.instance().start();
        /* starts a load timer thread */
        loadTimer_.schedule( new LoadDisseminator(), StorageService.threshold_, StorageService.threshold_);
        
        /* report our existence to ZooKeeper instance and start the leader election service */
        
        //reportToZookeeper(); 
        /* start the leader election algorithm */
        //LeaderElector.instance().start();
        /* start the map reduce framework */
        //startMapReduceFramework();
        
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
        tokenMetadata_.update(storageMetadata_.getStorageId(), StorageService.tcpAddr_);
        Gossiper.instance().addApplicationState(StorageService.nodeId_, new ApplicationState(storageMetadata_.getStorageId().toString()));
    }

    public void killMe() throws Throwable
    {
        isShutdown_.set(true);
        /* 
         * Shutdown the Gossiper to stop responding/sending Gossip messages.
         * This causes other nodes to detect you as dead and starting hinting
         * data for the local endpoint. 
        */
        Gossiper.instance().shutdown();
        final long nodeDeadDetectionTime = 25000L;
        Thread.sleep(nodeDeadDetectionTime);
        /* Now perform a force flush of the table */
        String table = DatabaseDescriptor.getTables().get(0);
        Table.open(table).flush(false);
        /* Now wait for the flush to complete */
        Thread.sleep(nodeDeadDetectionTime);
        /* Shutdown all other components */
        StorageService.instance().shutdown();
    }

    public boolean isShutdown()
    {
    	return isShutdown_.get();
    }

    public void shutdown()
    {
        bootStrapper_.shutdownNow();
        /* shut down all stages */
        StageManager.shutdown();
        /* shut down the messaging service */
        MessagingService.shutdown();
        /* shut down all memtables */
        Memtable.shutdown();
        /* shut down the load disseminator */
        loadTimer_.cancel();
        /* shut down the cleaner thread in FileUtils */
        FileUtils.shutdown();

        /* shut down all registered components */
        for ( IComponentShutdown component : components_ )
        {
        	component.shutdown();
        }
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata_.cloneMe();
    }

    /* TODO: remove later */
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
    public void doConsistencyCheck(Row row, List<EndPoint> endpoints, String columnFamily, int start, int count)
	{
		Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, columnFamily, start, count);
		consistencyManager_.submit(consistencySentinel);
	}
    
    public void doConsistencyCheck(Row row, List<EndPoint> endpoints, String columnFamily, long sinceTimestamp)
	{
		Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, columnFamily, sinceTimestamp);
		consistencyManager_.submit(consistencySentinel);
	}

    public void doConsistencyCheck(Row row, List<EndPoint> endpoints, String columnFamily, List<String> columns)
    {
    	Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, columnFamily, columns);
		consistencyManager_.submit(consistencySentinel);
    }

    public Map<Range, List<EndPoint>> getRangeToEndPointMap()
    {
        /* Get the token to endpoint map. */
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Set<Token> tokens = tokenToEndPointMap.keySet();
        /* All the ranges for the tokens */
        Range[] ranges = getAllRanges(tokens);
        Map<Range, List<EndPoint>> oldRangeToEndPointMap = constructRangeToEndPointMap(ranges);
        return oldRangeToEndPointMap;
    }

    /**
     * Construct the range to endpoint mapping based on the true view 
     * of the world. 
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    public Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges)
    {
        logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
        for ( Range range : ranges )
        {
            EndPoint[] endpoints = getNStorageEndPoint(range.right());
            rangeToEndPointMap.put(range, new ArrayList<EndPoint>( Arrays.asList(endpoints) ) );
        }
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
        logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
        for ( Range range : ranges )
        {
            EndPoint[] endpoints = getNStorageEndPoint(range.right(), tokenToEndPointMap);
            rangeToEndPointMap.put(range, new ArrayList<EndPoint>( Arrays.asList(endpoints) ) );
        }
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
        if (nodeIdState != null)
        {
            Token newToken = getPartitioner().getTokenFactory().fromString(nodeIdState.getState());
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
                    logger_.debug("Relocation for endpoint " + ep);
                    tokenMetadata_.update(newToken, ep);                    
                }
                else
                {
                    /*
                     * This means the node crashed and is coming back up.
                     * Deliver the hints that we have for this endpoint.
                    */
                    logger_.debug("Sending hinted data to " + ep);
                    doBootstrap(endpoint, BootstrapMode.HINT);
                }
            }
            else
            {
                /*
                 * This is a new node and we just update the token map.
                */
                tokenMetadata_.update(newToken, ep);
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
                logger_.debug("EndPoint " + ep + " just recovered from a partition. Sending hinted data.");
                doBootstrap(ep, BootstrapMode.HINT);
            }
        }

        /* Check if a bootstrap is in order */
        ApplicationState loadAllState = epState.getApplicationState(StorageService.loadAll_);
        if ( loadAllState != null )
        {
            String nodes = loadAllState.getState();
            if ( nodes != null )
            {
                doBootstrap(ep, BootstrapMode.FULL);
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
        SystemTable.openSystemTable(SystemTable.name_).updateToken(token);
        /* Update the storageMetadata cache */
        storageMetadata_.setStorageId(token);
        /* Update the token maps */
        /* Get the old token. This needs to be removed. */
        tokenMetadata_.update(token, StorageService.tcpAddr_);
        /* Gossip this new token for the local storage instance */
        Gossiper.instance().addApplicationState(StorageService.nodeId_, new ApplicationState(token.toString()));
    }
    
    /*
     * This method removes the state associated with this endpoint
     * from the TokenMetadata instance.
     * 
     *  param@ endpoint remove the token state associated with this 
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
	        Token newToken = token( keys[index] );
	        /* update the token */
	        updateToken(newToken);
    	}
    }

    /*
     * This is used to indicate that this node is done
     * with the loading of data.
    */
    public void resetLoadState()
    {
    }
    
    /**
     * This method takes a colon separated string of nodes that need
     * to be bootstrapped. It is also used to filter some source of 
     * data. Suppose the nodes to be bootstrapped are A, B and C. Then
     * <i>allNodes</i> must be specified as A:B:C.
     * 
    */
    private void doBootstrap(String nodes)
    {
        String[] allNodesAndFilter = nodes.split("-");
        String nodesToLoad = null;
        String filterSources = null;
        
        if ( allNodesAndFilter.length == 2 )
        {
            nodesToLoad = allNodesAndFilter[0];
            filterSources = allNodesAndFilter[1];
        }
        else
        {
            nodesToLoad = allNodesAndFilter[0];
        }        
        String[] allNodes = nodesToLoad.split(":");
        EndPoint[] endpoints = new EndPoint[allNodes.length];
        Token[] tokens = new Token[allNodes.length];
        
        for ( int i = 0; i < allNodes.length; ++i )
        {
            endpoints[i] = new EndPoint( allNodes[i].trim(), DatabaseDescriptor.getStoragePort() );
            tokens[i] = tokenMetadata_.getToken(endpoints[i]);
        }
        
        /* Start the bootstrap algorithm */
        if ( filterSources == null )
        bootStrapper_.submit( new BootStrapper(endpoints, tokens) );
        else
        {
            String[] allFilters = filterSources.split(":");
            EndPoint[] filters = new EndPoint[allFilters.length];
            for ( int i = 0; i < allFilters.length; ++i )
            {
                filters[i] = new EndPoint( allFilters[i].trim(), DatabaseDescriptor.getStoragePort() );
            }
            bootStrapper_.submit( new BootStrapper(endpoints, tokens, filters) );
        }
    }

    /**
     * Starts the bootstrap operations for the specified endpoint.
     * The name of this method is however a misnomer since it does
     * handoff of data to the specified node when it has crashed
     * and come back up, marked as alive after a network partition
     * and also when it joins the ring either as an old node being
     * relocated or as a brand new node.
    */
    public final void doBootstrap(EndPoint endpoint, BootstrapMode mode)
    {
        switch ( mode )
        {
            case FULL:
                Token token = tokenMetadata_.getToken(endpoint);
                bootStrapper_.submit(new BootStrapper(new EndPoint[]{endpoint}, token));
                break;

            case HINT:
                /* Deliver the hinted data to this endpoint. */
                HintedHandOffManager.instance().deliverHints(endpoint);
                break;

            default:
                break;
        }
    }

    /* This methods belong to the MBean interface */
    
    public String getToken(EndPoint ep)
    {
        EndPoint ep2 = new EndPoint(ep.getHost(), DatabaseDescriptor.getStoragePort());
        Token token = tokenMetadata_.getToken(ep2);
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

    public void loadAll(String nodes)
    {        
        doBootstrap(nodes);
    }
    
    public void doGC()
    {
        List<String> tables = DatabaseDescriptor.getTables();
        for ( String tName : tables )
        {
            Table table = Table.open(tName);
            table.doGC();
        }
    }
    
    public void forceHandoff(String directories, String host) throws IOException
    {       
        List<File> filesList = new ArrayList<File>();
        String[] sources = directories.split(":");
        for (String source : sources)
        {
            File directory = new File(source);
            Collections.addAll(filesList, directory.listFiles());            
        }
        
        File[] files = filesList.toArray(new File[0]);
        StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[files.length];
        int i = 0;
        for ( File file : files )
        {
            streamContexts[i] = new StreamContextManager.StreamContext(file.getAbsolutePath(), file.length());
            logger_.debug("Stream context metadata " + streamContexts[i]);
            ++i;
        }
        
        if ( files.length > 0 )
    {
            EndPoint target = new EndPoint(host, DatabaseDescriptor.getStoragePort());
            /* Set up the stream manager with the files that need to streamed */
            StreamManager.instance(target).addFilesToStream(streamContexts);
            /* Send the bootstrap initiate message */
            BootstrapInitiateMessage biMessage = new BootstrapInitiateMessage(streamContexts);
            Message message = BootstrapInitiateMessage.makeBootstrapInitiateMessage(biMessage);
            logger_.debug("Sending a bootstrap initiate message to " + target + " ...");
            MessagingService.getMessagingInstance().sendOneWay(message, target);                
            logger_.debug("Waiting for transfer to " + target + " to complete");
            StreamManager.instance(target).waitForStreamCompletion();
            logger_.debug("Done with transfer to " + target);  
        }
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
        EndPoint predecessor = (index == 0) ? tokenToEndPointMap.get(tokens
                .get(tokens.size() - 1)) : tokenToEndPointMap.get(tokens
                .get(--index));
        return predecessor;
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
        EndPoint successor = (index == (tokens.size() - 1)) ? tokenToEndPointMap
                .get(tokens.get(0))
                : tokenToEndPointMap.get(tokens.get(++index));
        return successor;
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
     * param @ key - key for which we need to find the endpoint
     * return value - the endpoint responsible for this key
     */
    public EndPoint getPrimary(String key)
    {
        EndPoint endpoint = StorageService.tcpAddr_;
        Token token = token(key);
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
     * param @ key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public EndPoint[] getNStorageEndPoint(String key)
    {
        Token token = token(key);
        return nodePicker_.getStorageEndPoints(token);
    }


    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * param @ key - key for which we need to find the endpoint return value -
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
     * param @ key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public Map<EndPoint, EndPoint> getNStorageEndPointMap(String key)
    {
        Token token = token(key);
        return nodePicker_.getHintedStorageEndPoints(token);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified token i.e for replication.
     *
     * param @ token - position on the ring
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
     * param @ token - position on the ring
     * param @ tokens - w/o the following tokens in the token list
     */
    protected EndPoint[] getNStorageEndPoint(Token token, Map<Token, EndPoint> tokenToEndPointMap)
    {
        return nodePicker_.getStorageEndPoints(token, tokenToEndPointMap);
    }

    /**
     * This function finds the most suitable endpoint given a key.
     * It checks for loclity and alive test.
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
			if ( StorageService.instance().isInSameDataCenter(endpoints[j]) && FailureDetector.instance().isAlive(endpoints[j]) )
			{
				logger_.debug("EndPoint " + endpoints[j] + " is in the same data center as local storage endpoint.");
				return endpoints[j];
			}
		}
		// We have tried to be really nice but looks like theer are no servers 
		// in the local data center that are alive and can service this request so 
		// just send it to teh first alive guy and see if we get anything.
		j = 0;
		for ( ; j < endpoints.length; ++j )
		{
			if ( FailureDetector.instance().isAlive(endpoints[j]) )
			{
				logger_.debug("EndPoint " + endpoints[j] + " is alive so get data from it.");
				return endpoints[j];
			}
		}
		return null;
	}

}
