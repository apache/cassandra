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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndPointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndPointStateChangeSubscriber;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

class LeaderElector implements IEndPointStateChangeSubscriber
{
    private static Logger logger_ = Logger.getLogger(LeaderElector.class);
    protected static final String leaderState_ = "LEADER";
    private static LeaderElector instance_ = null;
    private static Lock createLock_ = new ReentrantLock();
    
    /*
     * Factory method that gets an instance of the StorageService
     * class.
    */
    public static LeaderElector instance()
    {
        if ( instance_ == null )
        {
            LeaderElector.createLock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    instance_ = new LeaderElector();
                }
            }
            finally
            {
                createLock_.unlock();
            }
        }
        return instance_;
    }
    
    /* The elected leader. */
    private AtomicReference<EndPoint> leader_;
    private Condition condition_;
    private ExecutorService leaderElectionService_ = new DebuggableThreadPoolExecutor(1,
            1,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("LEADER-ELECTOR")
            );
    
    private class LeaderDeathMonitor implements Runnable
    {
        private String pathCreated_;
        
        LeaderDeathMonitor(String pathCreated)
        {
            pathCreated_ = pathCreated;
        }
        
        public void run()
        {            
            ZooKeeper zk = StorageService.instance().getZooKeeperHandle();
            String path = "/Cassandra/" + DatabaseDescriptor.getClusterName() + "/Leader";
            try
            {
                String createPath = path + "/L-";                                
                LeaderElector.createLock_.lock();
                while( true )
                {
                    /* Get all znodes under the Leader znode */
                    List<String> values = zk.getChildren(path, false);
                    SortedMap<Integer, String> suffixToZnode = getSuffixToZnodeMapping(values);
                    String value = suffixToZnode.get( suffixToZnode.firstKey() );
                    /*
                     * Get the first znode and if it is the 
                     * pathCreated created above then the data
                     * in that znode is the leader's identity. 
                    */
                    if ( leader_ == null )
                    {
                        leader_ = new AtomicReference<EndPoint>( EndPoint.fromBytes( zk.getData(path + "/" + value, false, null) ) );
                    }
                    else
                    {
                        leader_.set( EndPoint.fromBytes( zk.getData(path + "/" + value, false, null) ) );
                        /* Disseminate the state as to who the leader is. */
                        onLeaderElection();
                    }
                    logger_.debug("Elected leader is " + leader_ + " @ znode " + ( path + "/" + value ) );                                     
                    /* We need only the last portion of this znode */
                    int index = getLocalSuffix();                   
                    if ( index > suffixToZnode.firstKey() )
                    {
                        String pathToCheck = path + "/" + getImmediatelyPrecedingZnode(suffixToZnode, index);
                        Stat stat = zk.exists(pathToCheck, true);
                        if ( stat != null )
                        {
                            logger_.debug("Awaiting my turn ...");
                            condition_.await();
                            logger_.debug("Checking to see if leader is around ...");
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch ( InterruptedException ex )
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
            catch ( IOException ex )
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
            catch ( KeeperException ex )
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
            finally
            {
                LeaderElector.createLock_.unlock();
            }
        }
        
        private SortedMap<Integer, String> getSuffixToZnodeMapping(List<String> values)
        {
            SortedMap<Integer, String> suffixZNodeMap = new TreeMap<Integer, String>();
            for ( String znode : values )
            {
                String[] peices = znode.split("-");
                suffixZNodeMap.put(Integer.parseInt( peices[1] ), znode);
            }
            return suffixZNodeMap;
        }
        
        private String getImmediatelyPrecedingZnode(SortedMap<Integer, String> suffixToZnode, int index)
        {
            List<Integer> suffixes = new ArrayList<Integer>( suffixToZnode.keySet() );            
            Collections.sort(suffixes);
            int position = Collections.binarySearch(suffixes, index);
            return suffixToZnode.get( suffixes.get( position - 1 ) );
        }
        
        /**
         * If the local node's leader related znode is L-3
         * this method will return 3.
         * @return suffix portion of L-3.
         */
        private int getLocalSuffix()
        {
            String[] peices = pathCreated_.split("/");
            String leaderPeice = peices[peices.length - 1];
            String[] leaderPeices = leaderPeice.split("-");
            return Integer.parseInt( leaderPeices[1] );
        }
    }
    
    private LeaderElector()
    {
        condition_ = LeaderElector.createLock_.newCondition();
    }
    
    /**
     * Use to inform interested parties about the change in the state
     * for specified endpoint
     * 
     * @param endpoint endpoint for which the state change occured.
     * @param epState state that actually changed for the above endpoint.
     */
    public void onChange(EndPoint endpoint, EndPointState epState)
    {        
        /* node identifier for this endpoint on the identifier space */
        ApplicationState leaderState = epState.getApplicationState(LeaderElector.leaderState_);
        if (leaderState != null && !leader_.equals(endpoint))
        {
            EndPoint leader = EndPoint.fromString( leaderState.getState() );
            logger_.debug("New leader in the cluster is " + leader);
            leader_.set(endpoint);
        }
    }
    
    void start() throws Throwable
    {
        /* Register with the Gossiper for EndPointState notifications */
        Gossiper.instance().register(this);
        logger_.debug("Starting the leader election process ...");
        ZooKeeper zk = StorageService.instance().getZooKeeperHandle();
        String path = "/Cassandra/" + DatabaseDescriptor.getClusterName() + "/Leader";
        String createPath = path + "/L-";
        
        /* Create the znodes under the Leader znode */       
        logger_.debug("Attempting to create znode " + createPath);
        String pathCreated = zk.create(createPath, EndPoint.toBytes( StorageService.getLocalControlEndPoint() ), Ids.OPEN_ACL_UNSAFE, (CreateMode.EPHEMERAL_SEQUENTIAL) );             
        logger_.debug("Created znode under leader znode " + pathCreated);            
        leaderElectionService_.submit(new LeaderDeathMonitor(pathCreated));
    }
    
    void signal()
    {
        logger_.debug("Signalling others to check on leader ...");
        try
        {
            LeaderElector.createLock_.lock();
            condition_.signal();
        }
        finally
        {
            LeaderElector.createLock_.unlock();
        }
    }
    
    EndPoint getLeader()
    {
        return (leader_ != null ) ? leader_.get() : StorageService.getLocalStorageEndPoint();
    }
    
    private void onLeaderElection() throws InterruptedException, IOException
    {
        /*
         * If the local node is the leader then not only does he 
         * diseminate the information but also starts the M/R job
         * tracker. Non leader nodes start the M/R task tracker 
         * thereby initializing the M/R subsystem.
        */
        if ( StorageService.instance().isLeader(leader_.get()) )
        {
            Gossiper.instance().addApplicationState(LeaderElector.leaderState_, new ApplicationState(leader_.toString()));              
        }
    }
}
