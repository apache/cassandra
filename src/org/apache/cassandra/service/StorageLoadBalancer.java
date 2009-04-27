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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.SingleThreadedStage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndPointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndPointStateChangeSubscriber;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

/*
 * The load balancing algorithm here is an implementation of
 * the algorithm as described in the paper "Scalable range query
 * processing for large-scale distributed database applications".
 * This class keeps track of load information across the system.
 * It registers itself with the Gossiper for ApplicationState namely
 * load information i.e number of requests processed w.r.t distinct
 * keys at an Endpoint. Monitor load infomation for a 5 minute
 * interval and then do load balancing operations if necessary.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
final class StorageLoadBalancer implements IEndPointStateChangeSubscriber, IComponentShutdown
{
    class LoadBalancer implements Runnable
    {
        LoadBalancer()
        {
            /* Copy the entries in loadInfo_ into loadInfo2_ and use it for all calculations */
            loadInfo2_.putAll(loadInfo_);
        }

        /**
         * Obtain a node which is a potential target. Start with
         * the neighbours i.e either successor or predecessor.
         * Send the target a MoveMessage. If the node cannot be
         * relocated on the ring then we pick another candidate for
         * relocation.
        */        
        public void run()
        {
            /*
            int threshold = (int)(StorageLoadBalancer.ratio_ * averageSystemLoad());
            int myLoad = localLoad();            
            EndPoint predecessor = storageService_.getPredecessor(StorageService.getLocalStorageEndPoint());
            logger_.debug("Trying to relocate the predecessor " + predecessor);
            boolean value = tryThisNode(myLoad, threshold, predecessor);
            if ( !value )
            {
                loadInfo2_.remove(predecessor);
                EndPoint successor = storageService_.getSuccessor(StorageService.getLocalStorageEndPoint());
                logger_.debug("Trying to relocate the successor " + successor);
                value = tryThisNode(myLoad, threshold, successor);
                if ( !value )
                {
                    loadInfo2_.remove(successor);
                    while ( !loadInfo2_.isEmpty() )
                    {
                        EndPoint target = findARandomLightNode();
                        if ( target != null )
                        {
                            logger_.debug("Trying to relocate the random node " + target);
                            value = tryThisNode(myLoad, threshold, target);
                            if ( !value )
                            {
                                loadInfo2_.remove(target);
                            }
                            else
                            {
                                break;
                            }
                        }
                        else
                        {
                            // No light nodes available - this is NOT good.
                            logger_.warn("Not even a single lightly loaded node is available ...");
                            break;
                        }
                    }

                    loadInfo2_.clear();                    
                     // If we are here and no node was available to
                     // perform load balance with we need to report and bail.                    
                    if ( !value )
                    {
                        logger_.warn("Load Balancing operations weren't performed for this node");
                    }
                }                
            }
            */        
        }

        /*
        private boolean tryThisNode(int myLoad, int threshold, EndPoint target)
        {
            boolean value = false;
            LoadInfo li = loadInfo2_.get(target);
            int pLoad = li.count();
            if ( ((myLoad + pLoad) >> 1) <= threshold )
            {
                //calculate the number of keys to be transferred
                int keyCount = ( (myLoad - pLoad) >> 1 );
                logger_.debug("Number of keys we attempt to transfer to " + target + " " + keyCount);
                // Determine the token that the target should join at.         
                BigInteger targetToken = BootstrapAndLbHelper.getTokenBasedOnPrimaryCount(keyCount);
                // Send a MoveMessage and see if this node is relocateable
                MoveMessage moveMessage = new MoveMessage(targetToken);
                Message message = new Message(StorageService.getLocalStorageEndPoint(), StorageLoadBalancer.lbStage_, StorageLoadBalancer.moveMessageVerbHandler_, new Object[]{moveMessage});
                logger_.debug("Sending a move message to " + target);
                IAsyncResult result = MessagingService.getMessagingInstance().sendRR(message, target);
                value = (Boolean)result.get()[0];
                logger_.debug("Response for query to relocate " + target + " is " + value);
            }
            return value;
        }
        */
    }

    class MoveMessageVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message)
        {
            Message reply = message.getReply(StorageService.getLocalStorageEndPoint(), new Object[]{isMoveable_.get()});
            MessagingService.getMessagingInstance().sendOneWay(reply, message.getFrom());
            if ( isMoveable_.get() )
            {
                MoveMessage moveMessage = (MoveMessage)message.getMessageBody()[0];
                /* Start the leave operation and join the ring at the position specified */
                isMoveable_.set(false);
            }
        }
    }

    private static final Logger logger_ = Logger.getLogger(StorageLoadBalancer.class);
    private static final String lbStage_ = "LOAD-BALANCER-STAGE";
    private static final String moveMessageVerbHandler_ = "MOVE-MESSAGE-VERB-HANDLER";
    /* time to delay in minutes the actual load balance procedure if heavily loaded */
    private static final int delay_ = 5;
    /* Ratio of highest loaded node and the average load. */
    private static final double ratio_ = 1.5;

    private StorageService storageService_;
    /* this indicates whether this node is already helping someone else */
    private AtomicBoolean isMoveable_ = new AtomicBoolean(false);
    private Map<EndPoint, LoadInfo> loadInfo_ = new HashMap<EndPoint, LoadInfo>();
    /* This map is a clone of the one above and is used for various calculations during LB operation */
    private Map<EndPoint, LoadInfo> loadInfo2_ = new HashMap<EndPoint, LoadInfo>();
    /* This thread pool is used for initiating load balancing operations */
    private ScheduledThreadPoolExecutor lb_ = new DebuggableScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryImpl("LB-OPERATIONS")
            );
    /* This thread pool is used by target node to leave the ring. */
    private ExecutorService lbOperations_ = new DebuggableThreadPoolExecutor(1,
            1,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("LB-TARGET")
            );

    StorageLoadBalancer(StorageService storageService)
    {
        storageService_ = storageService;
        /* register the load balancer stage */
        StageManager.registerStage(StorageLoadBalancer.lbStage_, new SingleThreadedStage(StorageLoadBalancer.lbStage_));
        /* register the load balancer verb handler */
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageLoadBalancer.moveMessageVerbHandler_, new MoveMessageVerbHandler());
        /* register with the StorageService */
        storageService_.registerComponentForShutdown(this);
    }

    public void start()
    {
        /* Register with the Gossiper for EndPointState notifications */
        Gossiper.instance().register(this);
    }

    public void shutdown()
    {
        lbOperations_.shutdownNow();
        lb_.shutdownNow();
    }

    public void onChange(EndPoint endpoint, EndPointState epState)
    {
        logger_.debug("CHANGE IN STATE FOR @ StorageLoadBalancer " + endpoint);
        // load information for this specified endpoint for load balancing 
        ApplicationState loadInfoState = epState.getApplicationState(LoadDisseminator.loadInfo_);
        if ( loadInfoState != null )
        {
            String lInfoState = loadInfoState.getState();
            LoadInfo lInfo = new LoadInfo(lInfoState);
            loadInfo_.put(endpoint, lInfo);
            
            /*
            int currentLoad = Integer.parseInt(loadInfoState.getState());
            // update load information for this endpoint
            loadInfo_.put(endpoint, currentLoad);

            // clone load information to perform calculations
            loadInfo2_.putAll(loadInfo_);
            // Perform the analysis for load balance operations
            if ( isHeavyNode() )
            {
                logger_.debug(StorageService.getLocalStorageEndPoint() + " is a heavy node with load " + localLoad());
                // lb_.schedule( new LoadBalancer(), StorageLoadBalancer.delay_, TimeUnit.MINUTES );
            }
            */
        }       
    }

    /*
     * Load information associated with a given endpoint.
    */
    LoadInfo getLoad(EndPoint ep)
    {
        LoadInfo li = loadInfo_.get(ep);        
        return li;        
    }

    /*
    private boolean isMoveable()
    {
        if ( !isMoveable_.get() )
            return false;
        int myload = localLoad();
        EndPoint successor = storageService_.getSuccessor(StorageService.getLocalStorageEndPoint());
        LoadInfo li = loadInfo2_.get(successor);
        // "load" is NULL means that the successor node has not
        // yet gossiped its load information. We should return
        // false in this case since we want to err on the side
        // of caution.
        if ( li == null )
            return false;
        else
        {            
            if ( ( myload + li.count() ) > StorageLoadBalancer.ratio_*averageSystemLoad() )
                return false;
            else
                return true;
        }
    }
    */

    /*
    private int localLoad()
    {
        LoadInfo value = loadInfo2_.get(StorageService.getLocalStorageEndPoint());
        return (value == null) ? 0 : value.count();
    }
    */

    /*
    private int averageSystemLoad()
    {
        int nodeCount = loadInfo2_.size();
        Set<EndPoint> nodes = loadInfo2_.keySet();

        int systemLoad = 0;
        for ( EndPoint node : nodes )
        {
            LoadInfo load = loadInfo2_.get(node);
            if ( load != null )
                systemLoad += load.count();
        }
        int averageLoad = (nodeCount > 0) ? (systemLoad / nodeCount) : 0;
        logger_.debug("Average system load should be " + averageLoad);
        return averageLoad;
    }
    */
    
    /*
    private boolean isHeavyNode()
    {
        return ( localLoad() > ( StorageLoadBalancer.ratio_ * averageSystemLoad() ) );
    }
    */
    
    /*
    private boolean isMoveable(EndPoint target)
    {
        int threshold = (int)(StorageLoadBalancer.ratio_ * averageSystemLoad());
        if ( isANeighbour(target) )
        {
            // If the target is a neighbour then it is
            // moveable if its
            LoadInfo load = loadInfo2_.get(target);
            if ( load == null )
                return false;
            else
            {
                int myload = localLoad();
                int avgLoad = (load.count() + myload) >> 1;
                if ( avgLoad <= threshold )
                    return true;
                else
                    return false;
            }
        }
        else
        {
            EndPoint successor = storageService_.getSuccessor(target);
            LoadInfo sLoad = loadInfo2_.get(successor);
            LoadInfo targetLoad = loadInfo2_.get(target);
            if ( (sLoad.count() + targetLoad.count()) > threshold )
                return false;
            else
                return true;
        }
    }
    */

    private boolean isANeighbour(EndPoint neighbour)
    {
        EndPoint predecessor = storageService_.getPredecessor(StorageService.getLocalStorageEndPoint());
        if ( predecessor.equals(neighbour) )
            return true;

        EndPoint successor = storageService_.getSuccessor(StorageService.getLocalStorageEndPoint());
        if ( successor.equals(neighbour) )
            return true;

        return false;
    }

    /*
     * Determine the nodes that are lightly loaded. Choose at
     * random one of the lightly loaded nodes and use them as
     * a potential target for load balance.
    */
    /*
    private EndPoint findARandomLightNode()
    {
        List<EndPoint> potentialCandidates = new ArrayList<EndPoint>();
        Set<EndPoint> allTargets = loadInfo2_.keySet();
        int avgLoad =  averageSystemLoad();

        for( EndPoint target : allTargets )
        {
            LoadInfo load = loadInfo2_.get(target);
            if ( load.count() < avgLoad )
                potentialCandidates.add(target);
        }

        if ( potentialCandidates.size() > 0 )
        {
            Random random = new Random();
            int index = random.nextInt(potentialCandidates.size());
            return potentialCandidates.get(index);
        }
        return null;
    }
    */
}

class MoveMessage implements Serializable
{
    private Token targetToken_;

    private MoveMessage()
    {
    }

    MoveMessage(Token targetToken)
    {
        targetToken_ = targetToken;
    }

    Token getTargetToken()
    {
        return targetToken_;
    }
}
