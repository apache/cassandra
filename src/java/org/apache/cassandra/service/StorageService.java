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
import org.apache.cassandra.service.AntiEntropyService.TreeRequestVerbHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.Streaming;
import org.apache.cassandra.io.StreamRequestVerbHandler;
import org.apache.cassandra.io.util.FileUtils;

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

    public final static String MOVE_STATE = "MOVE";

    // this must be a char that cannot be present in any token
    public final static char Delimiter = ',';

    public final static String STATE_BOOTSTRAPPING = "BOOT";
    public final static String STATE_NORMAL = "NORMAL";
    public final static String STATE_LEAVING = "LEAVING";
    public final static String STATE_LEFT = "LEFT";

    public final static String REMOVE_TOKEN = "remove";
    public final static String LEFT_NORMALLY = "left";

    /* All verb handler identifiers */
    public final static String mutationVerbHandler_ = "ROW-MUTATION-VERB-HANDLER";
    public final static String binaryVerbHandler_ = "BINARY-VERB-HANDLER";
    public final static String readRepairVerbHandler_ = "READ-REPAIR-VERB-HANDLER";
    public final static String readVerbHandler_ = "ROW-READ-VERB-HANDLER";
    public final static String streamInitiateVerbHandler_ = "BOOTSTRAP-INITIATE-VERB-HANDLER";
    public final static String streamInitiateDoneVerbHandler_ = "BOOTSTRAP-INITIATE-DONE-VERB-HANDLER";
    public final static String streamFinishedVerbHandler_ = "BOOTSTRAP-TERMINATE-VERB-HANDLER";
    public final static String streamRequestVerbHandler_ = "BS-METADATA-VERB-HANDLER";
    public final static String rangeVerbHandler_ = "RANGE-VERB-HANDLER";
    public final static String rangeSliceVerbHandler_ = "RANGE-SLICE-VERB-HANDLER";
    public final static String bootstrapTokenVerbHandler_ = "SPLITS-VERB-HANDLER";

    private static IPartitioner partitioner_ = DatabaseDescriptor.getPartitioner();

    public static final StorageService instance = new StorageService();

    public static IPartitioner getPartitioner() {
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
    private ExecutorService consistencyManager_ = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getConsistencyThreads(),
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
    /* when intialized as a client, we shouldn't write to the system table. */
    private boolean isClientMode;
  
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
        Gossiper.instance.addApplicationState(MOVE_STATE, new ApplicationState(STATE_NORMAL + Delimiter + partitioner_.getTokenFactory().toString(getLocalToken())));
        logger_.info("Bootstrap/move completed! Now serving reads.");
    }

    /** This method updates the local token on disk  */
    public void setToken(Token token)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Setting token to " + token);
        SystemTable.updateToken(token);
        tokenMetadata_.updateNormalToken(token, FBUtilities.getLocalAddress());
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
        MessagingService.instance().registerVerbHandlers(rangeVerbHandler_, new RangeVerbHandler());
        MessagingService.instance().registerVerbHandlers(rangeSliceVerbHandler_, new RangeSliceVerbHandler());
        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance().registerVerbHandlers(bootstrapTokenVerbHandler_, new BootStrapper.BootstrapTokenVerbHandler());
        MessagingService.instance().registerVerbHandlers(streamRequestVerbHandler_, new StreamRequestVerbHandler() );
        MessagingService.instance().registerVerbHandlers(streamInitiateVerbHandler_, new Streaming.StreamInitiateVerbHandler());
        MessagingService.instance().registerVerbHandlers(streamInitiateDoneVerbHandler_, new Streaming.StreamInitiateDoneVerbHandler());
        MessagingService.instance().registerVerbHandlers(streamFinishedVerbHandler_, new Streaming.StreamFinishedVerbHandler());

        replicationStrategy_ = getReplicationStrategy(tokenMetadata_);
    }

    public static AbstractReplicationStrategy getReplicationStrategy(TokenMetadata tokenMetadata)
    {
        AbstractReplicationStrategy replicationStrategy = null;
        Class<AbstractReplicationStrategy> cls = DatabaseDescriptor.getReplicaPlacementStrategyClass();
        Class [] parameterTypes = new Class[] { TokenMetadata.class, int.class};
        try
        {
            Constructor<AbstractReplicationStrategy> constructor = cls.getConstructor(parameterTypes);
            replicationStrategy = constructor.newInstance(tokenMetadata, DatabaseDescriptor.getReplicationFactor());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return replicationStrategy;
    }

    public void stopClient()
    {
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.shutdown();
    }

    public void initClient() throws IOException
    {
        isClientMode = true;
        logger_.info("Starting up client gossip");
        MessagingService.instance().listen(FBUtilities.getLocalAddress());
        MessagingService.instance().listenUDP(FBUtilities.getLocalAddress());

        SelectorManager.getSelectorManager().start();
        SelectorManager.getUdpSelectorManager().start();

        Gossiper.instance.register(this);
        Gossiper.instance.start(FBUtilities.getLocalAddress(), (int)(System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
    }
    
    public void initServer() throws IOException
    {
        isClientMode = false;
        storageMetadata_ = SystemTable.initMetadata();
        DatabaseDescriptor.createAllDirectories();
        logger_.info("Starting up server gossip");

        /* Listen for application messages */
        MessagingService.instance().listen(FBUtilities.getLocalAddress());
        /* Listen for control messages */
        MessagingService.instance().listenUDP(FBUtilities.getLocalAddress());

        SelectorManager.getSelectorManager().start();
        SelectorManager.getUdpSelectorManager().start();

        StorageLoadBalancer.instance.startBroadcasting();

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a nodeId to our state, below.)
        Gossiper.instance.register(this);
        Gossiper.instance.start(FBUtilities.getLocalAddress(), storageMetadata_.getGeneration()); // needed for node-ring gathering.

        if (DatabaseDescriptor.isAutoBootstrap()
            && !(DatabaseDescriptor.getSeeds().contains(FBUtilities.getLocalAddress()) || SystemTable.isBootstrapped()))
        {
            logger_.info("Starting in bootstrap mode (first, sleeping to get load information)");
            StorageLoadBalancer.instance.waitForLoadInfo();
            logger_.info("... got load info");
            if (tokenMetadata_.isMember(FBUtilities.getLocalAddress()))
            {
                String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                throw new UnsupportedOperationException(s);
            }
            Token token = BootStrapper.getBootstrapToken(tokenMetadata_, StorageLoadBalancer.instance.getLoadInfo());
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
            tokenMetadata_.updateNormalToken(token, FBUtilities.getLocalAddress());
            Gossiper.instance.addApplicationState(MOVE_STATE, new ApplicationState(STATE_NORMAL + Delimiter + partitioner_.getTokenFactory().toString(token)));
        }

        assert tokenMetadata_.sortedTokens().size() > 0;
    }

    private void startBootstrap(Token token) throws IOException
    {
        isBootstrapMode = true;
        SystemTable.updateToken(token); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        Gossiper.instance.addApplicationState(MOVE_STATE, new ApplicationState(STATE_BOOTSTRAPPING + Delimiter + partitioner_.getTokenFactory().toString(token)));
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
    public void onChange(InetAddress endpoint, String apStateName, ApplicationState apState)
    {
        if (!MOVE_STATE.equals(apStateName))
            return;

        String apStateValue = apState.getValue();
        int index = apStateValue.indexOf(Delimiter);
        assert (index != -1);

        String moveName = apStateValue.substring(0, index);
        String moveValue = apStateValue.substring(index+1);

        if (moveName.equals(STATE_BOOTSTRAPPING))
            handleStateBootstrap(endpoint, moveValue);
        else if (moveName.equals(STATE_NORMAL))
            handleStateNormal(endpoint, moveValue);
        else if (moveName.equals(STATE_LEAVING))
            handleStateLeaving(endpoint, moveValue);
        else if (moveName.equals(STATE_LEFT))
            handleStateLeft(endpoint, moveValue);
    }

    /**
     * Handle node bootstrap
     *
     * @param endPoint bootstrapping node
     * @param moveValue bootstrap token as string
     */
    private void handleStateBootstrap(InetAddress endPoint, String moveValue)
    {
        Token token = getPartitioner().getTokenFactory().fromString(moveValue);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endPoint + " state bootstrapping, token " + token);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata_.isMember(endPoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetadata_.isLeaving(endPoint))
                logger_.info("Node " + endPoint + " state jump to bootstrap");
            tokenMetadata_.removeEndpoint(endPoint);
        }

        tokenMetadata_.addBootstrapToken(token, endPoint);
        calculatePendingRanges();
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endPoint node
     * @param moveValue token as string
     */
    private void handleStateNormal(InetAddress endPoint, String moveValue)
    {
        Token token = getPartitioner().getTokenFactory().fromString(moveValue);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endPoint + " state normal, token " + token);

        if (tokenMetadata_.isMember(endPoint))
            logger_.info("Node " + endPoint + " state jump to normal");

        tokenMetadata_.updateNormalToken(token, endPoint);
        calculatePendingRanges();
        if (!isClientMode)
            SystemTable.updateToken(endPoint, token);
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endPoint node
     * @param moveValue token as string
     */
    private void handleStateLeaving(InetAddress endPoint, String moveValue)
    {
        Token token = getPartitioner().getTokenFactory().fromString(moveValue);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endPoint + " state leaving, token " + token);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata_.isMember(endPoint))
        {
            logger_.info("Node " + endPoint + " state jump to leaving");
            tokenMetadata_.updateNormalToken(token, endPoint);
        }
        else if (!tokenMetadata_.getToken(endPoint).equals(token))
        {
            logger_.warn("Node " + endPoint + " 'leaving' token mismatch. Long network partition?");
            tokenMetadata_.updateNormalToken(token, endPoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata_.addLeavingEndPoint(endPoint);
        calculatePendingRanges();
    }

    /**
     * Handle node leaving the ring. This can be either because the node was removed manually by
     * removetoken command or because of decommission or loadbalance
     *
     * @param endPoint If reason for leaving is decommission or loadbalance (LEFT_NORMALLY),
     * endPoint is the leaving node. If reason manual removetoken (REMOVE_TOKEN), endPoint
     * parameter is ignored and the operation is based on the token inside moveValue.
     * @param moveValue (REMOVE_TOKEN|LEFT_NORMALLY)<Delimiter><token>
     */
    private void handleStateLeft(InetAddress endPoint, String moveValue)
    {
        int index = moveValue.indexOf(Delimiter);
        assert (index != -1);
        String typeOfState = moveValue.substring(0, index);
        Token token = getPartitioner().getTokenFactory().fromString(moveValue.substring(index + 1));

        // endPoint itself is leaving
        if (typeOfState.equals(LEFT_NORMALLY))
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Node " + endPoint + " state left, token " + token);

            // If the node is member, remove all references to it. If not, call
            // removeBootstrapToken just in case it is there (very unlikely chain of events)
            if (tokenMetadata_.isMember(endPoint))
            {
                if (!tokenMetadata_.getToken(endPoint).equals(token))
                    logger_.warn("Node " + endPoint + " 'left' token mismatch. Long network partition?");
                tokenMetadata_.removeEndpoint(endPoint);
            }
        }
        else
        {
            // if we're here, endPoint is not leaving but broadcasting remove token command
            assert (typeOfState.equals(REMOVE_TOKEN));
            InetAddress endPointThatLeft = tokenMetadata_.getEndPoint(token);
            if (logger_.isDebugEnabled())
                logger_.debug("Token " + token + " removed manually (endpoint was " + ((endPointThatLeft == null) ? "unknown" : endPointThatLeft) + ")");
            if (endPointThatLeft != null)
            {
                restoreReplicaCount(endPointThatLeft);
                tokenMetadata_.removeEndpoint(endPointThatLeft);
            }
        }

        // remove token from bootstrap tokens just in case it is still there
        tokenMetadata_.removeBootstrapToken(token);
        calculatePendingRanges();
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
        calculatePendingRanges(tokenMetadata_, replicationStrategy_);
    }

    // public & static for testing purposes
    public static void calculatePendingRanges(TokenMetadata tm, AbstractReplicationStrategy strategy)
    {
        Multimap<Range, InetAddress> pendingRanges = HashMultimap.create();
        Map<Token, InetAddress> bootstrapTokens = tm.getBootstrapTokens();
        Set<InetAddress> leavingEndPoints = tm.getLeavingEndPoints();

        if (bootstrapTokens.isEmpty() && leavingEndPoints.isEmpty())
        {
            if (logger_.isDebugEnabled())
                logger_.debug("No bootstrapping or leaving nodes -> empty pending ranges");
            tm.setPendingRanges(pendingRanges);
            return;
        }

        Multimap<InetAddress, Range> addressRanges = strategy.getAddressRanges();

        // Copy of metadata reflecting the situation after all leave operations are finished.
        TokenMetadata allLeftMetadata = tm.cloneAfterAllLeft();

        // get all ranges that will be affected by leaving nodes
        Set<Range> affectedRanges = new HashSet<Range>();
        for (InetAddress endPoint : leavingEndPoints)
            affectedRanges.addAll(addressRanges.get(endPoint));

        // for each of those ranges, find what new nodes will be responsible for the range when
        // all leaving nodes are gone.
        for (Range range : affectedRanges)
        {
            List<InetAddress> currentEndPoints = strategy.getNaturalEndpoints(range.right(), tm);
            List<InetAddress> newEndPoints = strategy.getNaturalEndpoints(range.right(), allLeftMetadata);
            newEndPoints.removeAll(currentEndPoints);
            pendingRanges.putAll(range, newEndPoints);
        }

        // At this stage pendingRanges has been updated according to leave operations. We can
        // now finish the calculation by checking bootstrapping nodes.

        // For each of the bootstrapping nodes, simply add and remove them one by one to
        // allLeftMetadata and check in between what their ranges would be.
        for (Map.Entry<Token, InetAddress> entry : bootstrapTokens.entrySet())
        {
            InetAddress endPoint = entry.getValue();

            allLeftMetadata.updateNormalToken(entry.getKey(), endPoint);
            for (Range range : strategy.getAddressRanges(allLeftMetadata).get(endPoint))
                pendingRanges.put(range, endPoint);
            allLeftMetadata.removeEndpoint(endPoint);
        }

        tm.setPendingRanges(pendingRanges);

        if (logger_.isDebugEnabled())
            logger_.debug("Pending ranges:\n" + (pendingRanges.isEmpty() ? "<empty>" : tm.printPendingRanges()));
    }

    /**
     * Called when endPoint is removed from the ring without proper
     * STATE_LEAVING -> STATE_LEFT sequence. This function checks
     * whether this node becomes responsible for new ranges as a
     * consequence and streams data if needed.
     *
     * This is rather ineffective, but it does not matter so much
     * since this is called very seldom
     *
     * @param endPoint node that has left
     */
    private void restoreReplicaCount(InetAddress endPoint)
    {
        InetAddress myAddress = FBUtilities.getLocalAddress();

        // get all ranges that change ownership (that is, a node needs
        // to take responsibility for new range)
        Multimap<Range, InetAddress> changedRanges = getChangedRangesForLeaving(endPoint);

        // check if any of these ranges are coming our way
        Set<Range> myNewRanges = new HashSet<Range>();
        for (Map.Entry<Range, InetAddress> entry : changedRanges.entries())
        {
            if (entry.getValue().equals(myAddress))
                myNewRanges.add(entry.getKey());
        }

        if (!myNewRanges.isEmpty())
        {
            if (logger_.isDebugEnabled())
                logger_.debug(endPoint + " was removed, my added ranges: " + StringUtils.join(myNewRanges, ", "));

            Multimap<Range, InetAddress> rangeAddresses = replicationStrategy_.getRangeAddresses(tokenMetadata_);
            Multimap<InetAddress, Range> sourceRanges = HashMultimap.create();
            IFailureDetector failureDetector = FailureDetector.instance;

            // find alive sources for our new ranges
            for (Range myNewRange : myNewRanges)
            {
                List<InetAddress> sources = DatabaseDescriptor.getEndPointSnitch().getSortedListByProximity(myAddress, rangeAddresses.get(myNewRange));

                assert (!sources.contains(myAddress));

                for (InetAddress source : sources)
                {
                    if (source.equals(endPoint))
                        continue;

                    if (failureDetector.isAlive(source))
                    {
                        sourceRanges.put(source, myNewRange);
                        break;
                    }
                }
            }

            // Finally we have a list of addresses and ranges to
            // stream. Proceed to stream
            for (Map.Entry<InetAddress, Collection<Range>> entry : sourceRanges.asMap().entrySet())
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Requesting from " + entry.getKey() + " ranges " + StringUtils.join(entry.getValue(), ", "));
                Streaming.requestRanges(entry.getKey(), entry.getValue());
            }
        }
    }

    private Multimap<Range, InetAddress> getChangedRangesForLeaving(InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range> ranges = getRangesForEndPoint(endpoint);

        if (logger_.isDebugEnabled())
            logger_.debug("Node " + endpoint + " ranges [" + StringUtils.join(ranges, ", ") + "]");

        Map<Range, ArrayList<InetAddress>> currentReplicaEndpoints = new HashMap<Range, ArrayList<InetAddress>>();

        // Find (for each range) all nodes that store replicas for these ranges as well
        for (Range range : ranges)
            currentReplicaEndpoints.put(range, replicationStrategy_.getNaturalEndpoints(range.right(), tokenMetadata_));

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
            ArrayList<InetAddress> newReplicaEndpoints = replicationStrategy_.getNaturalEndpoints(range.right(), temp);
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

    public void onJoin(InetAddress endpoint, EndPointState epState)
    {
        for (Map.Entry<String,ApplicationState> entry : epState.getSortedApplicationStates())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    public void onAlive(InetAddress endpoint, EndPointState state)
    {
        if (!isClientMode)
            deliverHints(endpoint);
    }

    public void onDead(InetAddress endpoint, EndPointState state) {}

    /** raw load value */
    public double getLoad()
    {
        double bytes = 0;
        for (String tableName : Table.getAllTableNames())
        {
            Table table;
            try
            {
                table = Table.open(tableName);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            for (String cfName : table.getColumnFamilies())
            {
                ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
                bytes += cfs.getLiveDiskSpaceUsed();
            }
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
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public Set<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers());
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
        return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getLocalAddress());
    }

    public void forceTableCleanup() throws IOException
    {
        List<String> tables = DatabaseDescriptor.getTables();
        for (String tName : tables)
        {
            if (tName.equals(Table.SYSTEM_TABLE))
                continue;
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
        Table tableInstance = getValidTable(tableName);
        tableInstance.snapshot(tag);
    }

    private Table getValidTable(String tableName) throws IOException
    {
        if (DatabaseDescriptor.getTable(tableName) == null)
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

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(String tableName, String... columnFamilies) throws IOException
    {
        Table table = getValidTable(tableName);
        Set<ColumnFamilyStore> valid = new HashSet<ColumnFamilyStore>();

        for (String cfName : columnFamilies.length == 0 ? table.getColumnFamilies() : Arrays.asList(columnFamilies))
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
    public void forceTableFlush(final String tableName, final String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            logger_.debug("Forcing binary flush on keyspace " + tableName + ", CF " + cfStore.getColumnFamilyName());
            cfStore.forceFlushBinary();
            logger_.debug("Forcing flush on keyspace " + tableName + ", CF " + cfStore.getColumnFamilyName());
            cfStore.forceFlush();
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
        // request that all relevant endpoints generate trees
        final MessagingService ms = MessagingService.instance();
        final List<InetAddress> endpoints = getNaturalEndpoints(getLocalToken());
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(tableName, columnFamilies))
        {
            Message request = TreeRequestVerbHandler.makeVerb(tableName, cfStore.getColumnFamilyName());
            for (InetAddress endpoint : endpoints)
                ms.sendOneWay(request, endpoint);
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
        return getPrimary(partitioner_.getToken(key));
    }

    public InetAddress getPrimary(Token token)
    {
        InetAddress endpoint = FBUtilities.getLocalAddress();
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
        return getNaturalEndpoints(partitioner_.getToken(key));
    }    

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param token - token for which we need to find the endpoint return value -
     * the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(Token token)
    {
        return replicationStrategy_.getNaturalEndpoints(token);
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
        return getLiveNaturalEndpoints(partitioner_.getToken(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(Token token)
    {
        List<InetAddress> liveEps = new ArrayList<InetAddress>();
        List<InetAddress> endpoints = replicationStrategy_.getNaturalEndpoints(token);

        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
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
     * This function finds the closest live endpoint that contains a given key.
     */
    public InetAddress findSuitableEndPoint(String key) throws IOException, UnavailableException
    {
        List<InetAddress> endpoints = getNaturalEndpoints(key);
        endPointSnitch_.sortByProximity(FBUtilities.getLocalAddress(), endpoints);
        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                return endpoint;
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

    /**
     * Broadcast leaving status and update local tokenMetadata_ accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addApplicationState(MOVE_STATE, new ApplicationState(STATE_LEAVING + Delimiter + getLocalToken().toString()));
        tokenMetadata_.addLeavingEndPoint(FBUtilities.getLocalAddress());
        calculatePendingRanges();
    }

    public void decommission() throws InterruptedException
    {
        if (!tokenMetadata_.isMember(FBUtilities.getLocalAddress()))
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
        if (tokenMetadata_.cloneAfterAllLeft().sortedTokens().size() < 2)
            throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
        if (tokenMetadata_.getPendingRanges(FBUtilities.getLocalAddress()).size() > 0)
            throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");

        logger_.info("DECOMMISSIONING");
        startLeaving();
        logger_.info("decommission sleeping " + Streaming.RING_DELAY);
        Thread.sleep(Streaming.RING_DELAY);

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
                Gossiper.instance.stop();
                MessagingService.shutdown();
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
        calculatePendingRanges();

        if (logger_.isDebugEnabled())
            logger_.debug("");
        Gossiper.instance.addApplicationState(MOVE_STATE, new ApplicationState(STATE_LEFT + Delimiter + LEFT_NORMALLY + Delimiter + getLocalToken().toString()));
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
            StageManager.getStage(StageManager.STREAM_STAGE).execute(new Runnable()
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
        startLeaving();
        logger_.info("move sleeping " + Streaming.RING_DELAY);
        Thread.sleep(Streaming.RING_DELAY);

        Runnable finishMoving = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                Token bootstrapToken = token;
                if (bootstrapToken == null)
                    bootstrapToken = BootStrapper.getBalancedToken(tokenMetadata_, StorageLoadBalancer.instance.getLoadInfo());
                logger_.info("re-bootstrapping to new token " + bootstrapToken);
                startBootstrap(bootstrapToken);
            }
        };
        unbootstrap(finishMoving);
    }

    public void removeToken(String tokenString)
    {
        Token token = partitioner_.getTokenFactory().fromString(tokenString);

        // Here we could refuse the operation from continuing if we
        // cannot find the endpoint for this token from metadata, but
        // that would prevent this command from being issued by a node
        // that has never seen the failed node.
        InetAddress endPoint = tokenMetadata_.getEndPoint(token);
        if (endPoint != null)
        {
            // Let's make sure however that we're not removing a live
            // token (member)
            if (Gossiper.instance.getLiveMembers().contains(endPoint))
                throw new UnsupportedOperationException("Node " + endPoint + " is alive and owns this token. Use decommission command to remove it from the ring");

            restoreReplicaCount(endPoint);
            tokenMetadata_.removeEndpoint(endPoint);
            calculatePendingRanges();
        }

        // This is not the cleanest way as we're adding STATE_LEFT for
        // a foreign token to our own EP state. Another way would be
        // to add new AP state for this command, but that would again
        // increase the amount of data to be gossiped in the cluster -
        // not good. REMOVE_TOKEN|LEFT_NORMALLY is used to distinguish
        // between removetoken command and normal state left, so it is
        // not so bad.
        Gossiper.instance.addApplicationState(MOVE_STATE, new ApplicationState(STATE_LEFT + Delimiter + REMOVE_TOKEN + Delimiter + token.toString()));
    }

    public WriteResponseHandler getWriteResponseHandler(int blockFor, ConsistencyLevel consistency_level)
    {
        return replicationStrategy_.getWriteResponseHandler(blockFor, consistency_level);
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy_;
    }

    public boolean isClientMode()
    {
        return isClientMode;
    }

    // Never ever do this at home. Used by tests.
    AbstractReplicationStrategy setReplicationStrategyUnsafe(AbstractReplicationStrategy newStrategy)
    {
        AbstractReplicationStrategy oldStrategy = replicationStrategy_;
        replicationStrategy_ = newStrategy;
        return oldStrategy;
    }

    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = partitioner_;
        partitioner_ = newPartitioner;
        return oldPartitioner;
    }

}
