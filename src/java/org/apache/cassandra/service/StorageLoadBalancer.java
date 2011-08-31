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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.*;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

/*
 * The load balancing algorithm here is an implementation of
 * the algorithm as described in the paper "Scalable range query
 * processing for large-scale distributed database applications".
 * This class keeps track of load information across the system.
 * It registers itself with the Gossiper for ApplicationState namely
 * load information i.e number of requests processed w.r.t distinct
 * keys at an Endpoint. Monitor load information for a 5 minute
 * interval and then do load balancing operations if necessary.
 */
public class StorageLoadBalancer implements IEndpointStateChangeSubscriber
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
            int threshold = (int)(StorageLoadBalancer.TOPHEAVY_RATIO * averageSystemLoad());
            int myLoad = localLoad();            
            InetAddress predecessor = StorageService.instance.getPredecessor(StorageService.getLocalStorageEndpoint());
            if (logger_.isDebugEnabled())
              logger_.debug("Trying to relocate the predecessor {}", predecessor);
            boolean value = tryThisNode(myLoad, threshold, predecessor);
            if ( !value )
            {
                loadInfo2_.remove(predecessor);
                InetAddress successor = StorageService.instance.getSuccessor(StorageService.getLocalStorageEndpoint());
                if (logger_.isDebugEnabled())
                  logger_.debug("Trying to relocate the successor {}", successor);
                value = tryThisNode(myLoad, threshold, successor);
                if ( !value )
                {
                    loadInfo2_.remove(successor);
                    while ( !loadInfo2_.isEmpty() )
                    {
                        InetAddress target = findARandomLightNode();
                        if ( target != null )
                        {
                            if (logger_.isDebugEnabled())
                              logger_.debug("Trying to relocate the random node {}", target);
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
        private boolean tryThisNode(int myLoad, int threshold, InetAddress target)
        {
            boolean value = false;
            LoadInfo li = loadInfo2_.get(target);
            int pLoad = li.count();
            if ( ((myLoad + pLoad) >> 1) <= threshold )
            {
                //calculate the number of keys to be transferred
                int keyCount = ( (myLoad - pLoad) >> 1 );
                if (logger_.isDebugEnabled())
                  logger_.debug("Number of keys we attempt to transfer to " + target + " " + keyCount);
                // Determine the token that the target should join at.         
                BigInteger targetToken = BootstrapAndLbHelper.getTokenBasedOnPrimaryCount(keyCount);
                // Send a MoveMessage and see if this node is relocateable
                MoveMessage moveMessage = new MoveMessage(targetToken);
                Message message = new Message(StorageService.getLocalStorageEndpoint(), StorageLoadBalancer.lbStage_, StorageLoadBalancer.moveMessageVerbHandler_, new Object[]{moveMessage});
                if (logger_.isDebugEnabled())
                  logger_.debug("Sending a move message to {}", target);
                IAsyncResult result = MessagingService.getMessagingInstance().sendRR(message, target);
                value = (Boolean)result.get()[0];
                if (logger_.isDebugEnabled())
                  logger_.debug("Response for query to relocate " + target + " is " + value);
            }
            return value;
        }
        */
    }

    private static final int BROADCAST_INTERVAL = 60 * 1000;

    public static final StorageLoadBalancer instance = new StorageLoadBalancer();

    private static final Logger logger_ = LoggerFactory.getLogger(StorageLoadBalancer.class);
    /* time to delay in minutes the actual load balance procedure if heavily loaded */
    private static final int delay_ = 5;
    /* If a node's load is this factor more than the average, it is considered Heavy */
    private static final double TOPHEAVY_RATIO = 1.5;

    /* this indicates whether this node is already helping someone else */
    private AtomicBoolean isMoveable_ = new AtomicBoolean(false);
    private Map<InetAddress, Double> loadInfo_ = new HashMap<InetAddress, Double>();
    /* This map is a clone of the one above and is used for various calculations during LB operation */
    private Map<InetAddress, Double> loadInfo2_ = new HashMap<InetAddress, Double>();

    private StorageLoadBalancer()
    {
        Gossiper.instance.register(this);
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo_.put(endpoint, Double.valueOf(value.value));

        /*
        // clone load information to perform calculations
        loadInfo2_.putAll(loadInfo_);
        // Perform the analysis for load balance operations
        if ( isHeavyNode() )
        {
            if (logger_.isDebugEnabled())
              logger_.debug(StorageService.getLocalStorageEndpoint() + " is a heavy node with load " + localLoad());
            // lb_.schedule( new LoadBalancer(), StorageLoadBalancer.delay_, TimeUnit.MINUTES );
        }
        */
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.LOAD, localValue);
        }
    }

    public void onAlive(InetAddress endpoint, EndpointState state) {}

    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRestart(InetAddress endpoint, EndpointState state) { }

    public void onRemove(InetAddress endpoint) {}

/*
    private boolean isMoveable()
    {
        if ( !isMoveable_.get() )
            return false;
        int myload = localLoad();
        InetAddress successor = StorageService.instance.getSuccessor(StorageService.getLocalStorageEndpoint());
        LoadInfo li = loadInfo2_.get(successor);
        // "load" is NULL means that the successor node has not
        // yet gossiped its load information. We should return
        // false in this case since we want to err on the side
        // of caution.
        if ( li == null )
            return false;
        else
        {            
            return ( ( myload + li.count() ) <= StorageLoadBalancer.TOPHEAVY_RATIO*averageSystemLoad() );
        }
    }
    */

    private double localLoad()
    {
        Double load = loadInfo2_.get(FBUtilities.getLocalAddress());
        return load == null ? 0 : load;
    }

    private double averageSystemLoad()
    {
        int nodeCount = loadInfo2_.size();
        Set<InetAddress> nodes = loadInfo2_.keySet();

        double systemLoad = 0;
        for (InetAddress node : nodes)
        {
            systemLoad += loadInfo2_.get(node);
        }
        double averageLoad = (nodeCount > 0) ? (systemLoad / nodeCount) : 0;
        if (logger_.isDebugEnabled())
            logger_.debug("Average system load is {}", averageLoad);
        return averageLoad;
    }

    private boolean isHeavyNode()
    {
        return ( localLoad() > ( StorageLoadBalancer.TOPHEAVY_RATIO * averageSystemLoad() ) );
    }

    private boolean isMoveable(InetAddress target)
    {
        double threshold = StorageLoadBalancer.TOPHEAVY_RATIO * averageSystemLoad();
        if (isANeighbour(target))
        {
            // If the target is a neighbour then it is
            // moveable if its
            Double load = loadInfo2_.get(target);
            if (load == null)
            {
                return false;
            }
            else
            {
                double myload = localLoad();
                double avgLoad = (load + myload) / 2;
                return avgLoad <= threshold;
            }
        }
        else
        {
            InetAddress successor = StorageService.instance.getSuccessor(target);
            double sLoad = loadInfo2_.get(successor);
            double targetLoad = loadInfo2_.get(target);
            return (sLoad + targetLoad) <= threshold;
        }
    }

    private boolean isANeighbour(InetAddress neighbour)
    {
        InetAddress predecessor = StorageService.instance.getPredecessor(FBUtilities.getLocalAddress());
        if ( predecessor.equals(neighbour) )
            return true;

        InetAddress successor = StorageService.instance.getSuccessor(FBUtilities.getLocalAddress());
        if ( successor.equals(neighbour) )
            return true;

        return false;
    }

    /*
     * Determine the nodes that are lightly loaded. Choose at
     * random one of the lightly loaded nodes and use them as
     * a potential target for load balance.
    */
    private InetAddress findARandomLightNode()
    {
        List<InetAddress> potentialCandidates = new ArrayList<InetAddress>();
        Set<InetAddress> allTargets = loadInfo2_.keySet();
        double avgLoad = averageSystemLoad();

        for (InetAddress target : allTargets)
        {
            double load = loadInfo2_.get(target);
            if (load < avgLoad)
            {
                potentialCandidates.add(target);
            }
        }

        if (potentialCandidates.size() > 0)
        {
            Random random = new Random();
            int index = random.nextInt(potentialCandidates.size());
            return potentialCandidates.get(index);
        }
        return null;
    }

    public Map<InetAddress, Double> getLoadInfo()
    {
        return loadInfo_;
    }

    public void startBroadcasting()
    {
        // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Disseminating load info ...");
                Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD,
                                                           StorageService.instance.valueFactory.load(StorageService.instance.getLoad()));
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(runnable, 2 * Gossiper.intervalInMillis, BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait for at least BROADCAST_INTERVAL ms, to give all nodes enough time to
     * report in.
     */
    public void waitForLoadInfo()
    {
        int duration = BROADCAST_INTERVAL + StorageService.RING_DELAY;
        try
        {
            logger_.info("Sleeping {} ms to wait for load information...", duration);
            Thread.sleep(duration);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }
}

