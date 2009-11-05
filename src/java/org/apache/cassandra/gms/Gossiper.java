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

package org.apache.cassandra.gms;

import java.io.*;
import java.util.*;
import java.net.InetAddress;

import org.apache.cassandra.concurrent.SingleThreadedStage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import java.net.InetAddress;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 */

public class Gossiper implements IFailureDetectionEventListener, IEndPointStateChangePublisher
{
    private class GossipTimerTask extends TimerTask
    {
        public void run()
        {
            try
            {
                synchronized( Gossiper.instance() )
                {
                	/* Update the local heartbeat counter. */
                    endPointStateMap_.get(localEndPoint_).getHeartBeatState().updateHeartBeat();
                    List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
                    Gossiper.instance().makeRandomGossipDigest(gDigests);

                    if ( gDigests.size() > 0 )
                    {
                        Message message = makeGossipDigestSynMessage(gDigests);
                        /* Gossip to some random live member */
                        boolean bVal = doGossipToLiveMember(message);

                        /* Gossip to some unreachable member with some probability to check if he is back up */
                        doGossipToUnreachableMember(message);

                        /* Gossip to the seed. */
                        if ( !bVal )
                            doGossipToSeed(message);

                        if (logger_.isTraceEnabled())
                            logger_.trace("Performing status check ...");
                        doStatusCheck();
                    }
                }
            }
            catch ( Throwable th )
            {
                logger_.info( LogUtil.throwableToString(th) );
            }
        }
    }

    final static int MAX_GOSSIP_PACKET_SIZE = 1428;
    /* GS - abbreviation for GOSSIPER_STAGE */
    final static String GOSSIP_STAGE = "GS";
    /* GSV - abbreviation for GOSSIP-DIGEST-SYN-VERB */
    final static String JOIN_VERB_HANDLER = "JVH";
    /* GSV - abbreviation for GOSSIP-DIGEST-SYN-VERB */
    final static String GOSSIP_DIGEST_SYN_VERB = "GSV";
    /* GAV - abbreviation for GOSSIP-DIGEST-ACK-VERB */
    final static String GOSSIP_DIGEST_ACK_VERB = "GAV";
    /* GA2V - abbreviation for GOSSIP-DIGEST-ACK2-VERB */
    final static String GOSSIP_DIGEST_ACK2_VERB = "GA2V";
    public final static int intervalInMillis_ = 1000;
    private static Logger logger_ = Logger.getLogger(Gossiper.class);
    static Gossiper gossiper_;

    public synchronized static Gossiper instance()
    {
        if ( gossiper_ == null )
        {
            gossiper_ = new Gossiper();
        }
        return gossiper_;
    }

    private Timer gossipTimer_ = new Timer(false);
    private InetAddress localEndPoint_;
    private long aVeryLongTime_;
    private Random random_ = new Random();
    /* round robin index through live endpoint set */
    private int rrIndex_ = 0;

    /* subscribers for interest in EndPointState change */
    private List<IEndPointStateChangeSubscriber> subscribers_ = new ArrayList<IEndPointStateChangeSubscriber>();

    /* live member set */
    private Set<InetAddress> liveEndpoints_ = new HashSet<InetAddress>();

    /* unreachable member set */
    private Set<InetAddress> unreachableEndpoints_ = new HashSet<InetAddress>();

    /* initial seeds for joining the cluster */
    private Set<InetAddress> seeds_ = new HashSet<InetAddress>();

    /* map where key is the endpoint and value is the state associated with the endpoint */
    Map<InetAddress, EndPointState> endPointStateMap_ = new Hashtable<InetAddress, EndPointState>();

    private Gossiper()
    {
        aVeryLongTime_ = 259200 * 1000;
        /* register with the Failure Detector for receiving Failure detector events */
        FailureDetector.instance().registerFailureDetectionEventListener(this);
        /* register the verbs */
        MessagingService.instance().registerVerbHandlers(JOIN_VERB_HANDLER, new JoinVerbHandler());
        MessagingService.instance().registerVerbHandlers(GOSSIP_DIGEST_SYN_VERB, new GossipDigestSynVerbHandler());
        MessagingService.instance().registerVerbHandlers(GOSSIP_DIGEST_ACK_VERB, new GossipDigestAckVerbHandler());
        MessagingService.instance().registerVerbHandlers(GOSSIP_DIGEST_ACK2_VERB, new GossipDigestAck2VerbHandler());
        /* register the Gossip stage */
        StageManager.registerStage( Gossiper.GOSSIP_STAGE, new SingleThreadedStage("GMFD") );
    }

    /** Register with the Gossiper for EndPointState notifications */
    public void register(IEndPointStateChangeSubscriber subscriber)
    {
        subscribers_.add(subscriber);
    }

    public void unregister(IEndPointStateChangeSubscriber subscriber)
    {
        subscribers_.remove(subscriber);
    }

    public Set<InetAddress> getAllMembers()
    {
        Set<InetAddress> allMbrs = new HashSet<InetAddress>();
        allMbrs.addAll(getLiveMembers());
        allMbrs.addAll(getUnreachableMembers());
        return allMbrs;
    }

    public Set<InetAddress> getLiveMembers()
    {
        Set<InetAddress> liveMbrs = new HashSet<InetAddress>(liveEndpoints_);
        liveMbrs.add(localEndPoint_);
        return liveMbrs;
    }

    public Set<InetAddress> getUnreachableMembers()
    {
        return new HashSet<InetAddress>(unreachableEndpoints_);
    }

    /**
     * This method is used to forcibly remove a node from the membership
     * set. He is forgotten locally immediately.
     *
     * param@ ep the endpoint to be removed from membership.
     */
    public synchronized void removeFromMembership(InetAddress ep)
    {
        endPointStateMap_.remove(ep);
        liveEndpoints_.remove(ep);
        unreachableEndpoints_ .remove(ep);
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * param @ endpoint end point that is convicted.
    */

    public void convict(InetAddress endpoint)
    {
        EndPointState epState = endPointStateMap_.get(endpoint);
        if (epState != null)
        {
            if (!epState.isAlive() && epState.isAGossiper())
            {
                // just to be sure - should already have been done by suspect()
                if (liveEndpoints_.contains(endpoint))
                {
                    logger_.info("InetAddress " + endpoint + " is now dead.");
                    isAlive(endpoint, epState, false);
                }
                epState.isAGossiper(false);
            }
        }
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it suspects an end point.
     *
     * param @ endpoint end point that is suspected.
    */
    public void suspect(InetAddress endpoint)
    {
        EndPointState epState = endPointStateMap_.get(endpoint);
        if (epState.isAlive())
        {
            logger_.info("InetAddress " + endpoint + " is now dead.");
            isAlive(endpoint, epState, false);
        }
    }

    int getMaxEndPointStateVersion(EndPointState epState)
    {
        List<Integer> versions = new ArrayList<Integer>();
        versions.add( epState.getHeartBeatState().getHeartBeatVersion() );
        Map<String, ApplicationState> appStateMap = epState.getApplicationState();

        Set<String> keys = appStateMap.keySet();
        for ( String key : keys )
        {
            int stateVersion = appStateMap.get(key).getStateVersion();
            versions.add( stateVersion );
        }

        /* sort to get the max version to build GossipDigest for this endpoint */
        Collections.sort(versions);
        int maxVersion = versions.get(versions.size() - 1);
        versions.clear();
        return maxVersion;
    }

    /**
     * Removes the endpoint from unreachable endpoint set
     *
     * @param endpoint endpoint to be removed from the current membership.
    */
    void evictFromMembership(InetAddress endpoint)
    {
        unreachableEndpoints_.remove(endpoint);
    }

    /* No locking required since it is called from a method that already has acquired a lock */
    @Deprecated
    void makeGossipDigest(List<GossipDigest> gDigests)
    {
        /* Add the local endpoint state */
        EndPointState epState = endPointStateMap_.get(localEndPoint_);
        int generation = epState.getHeartBeatState().getGeneration();
        int maxVersion = getMaxEndPointStateVersion(epState);
        gDigests.add( new GossipDigest(localEndPoint_, generation, maxVersion) );

        for ( InetAddress liveEndPoint : liveEndpoints_ )
        {
            epState = endPointStateMap_.get(liveEndPoint);
            if ( epState != null )
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxEndPointStateVersion(epState);
                gDigests.add( new GossipDigest(liveEndPoint, generation, maxVersion) );
            }
            else
            {
            	gDigests.add( new GossipDigest(liveEndPoint, 0, 0) );
            }
        }
    }

    /**
     * No locking required since it is called from a method that already
     * has acquired a lock. The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param gDigests list of Gossip Digests.
    */
    void makeRandomGossipDigest(List<GossipDigest> gDigests)
    {
        /* Add the local endpoint state */
        EndPointState epState = endPointStateMap_.get(localEndPoint_);
        int generation = epState.getHeartBeatState().getGeneration();
        int maxVersion = getMaxEndPointStateVersion(epState);
        gDigests.add( new GossipDigest(localEndPoint_, generation, maxVersion) );

        List<InetAddress> endpoints = new ArrayList<InetAddress>( liveEndpoints_ );
        Collections.shuffle(endpoints, random_);
        for ( InetAddress liveEndPoint : endpoints )
        {
            epState = endPointStateMap_.get(liveEndPoint);
            if ( epState != null )
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxEndPointStateVersion(epState);
                gDigests.add( new GossipDigest(liveEndPoint, generation, maxVersion) );
            }
            else
            {
            	gDigests.add( new GossipDigest(liveEndPoint, 0, 0) );
            }
        }

        /* FOR DEBUG ONLY - remove later */
        StringBuilder sb = new StringBuilder();
        for ( GossipDigest gDigest : gDigests )
        {
            sb.append(gDigest);
            sb.append(" ");
        }
        if (logger_.isTraceEnabled())
            logger_.trace("Gossip Digests are : " + sb.toString());
    }

    public int getCurrentGenerationNumber(InetAddress endpoint)
    {
    	return endPointStateMap_.get(endpoint).getHeartBeatState().getGeneration();
    }

    Message makeGossipDigestSynMessage(List<GossipDigest> gDigests) throws IOException
    {
        GossipDigestSynMessage gDigestMessage = new GossipDigestSynMessage(DatabaseDescriptor.getClusterName(), gDigests);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Gossiper.MAX_GOSSIP_PACKET_SIZE);
        DataOutputStream dos = new DataOutputStream( bos );
        GossipDigestSynMessage.serializer().serialize(gDigestMessage, dos);
        Message message = new Message(localEndPoint_, Gossiper.GOSSIP_STAGE, GOSSIP_DIGEST_SYN_VERB, bos.toByteArray());
        return message;
    }

    Message makeGossipDigestAckMessage(GossipDigestAckMessage gDigestAckMessage) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Gossiper.MAX_GOSSIP_PACKET_SIZE);
        DataOutputStream dos = new DataOutputStream(bos);
        GossipDigestAckMessage.serializer().serialize(gDigestAckMessage, dos);
        if (logger_.isTraceEnabled())
            logger_.trace("@@@@ Size of GossipDigestAckMessage is " + bos.toByteArray().length);
        Message message = new Message(localEndPoint_, Gossiper.GOSSIP_STAGE, GOSSIP_DIGEST_ACK_VERB, bos.toByteArray());
        return message;
    }

    Message makeGossipDigestAck2Message(GossipDigestAck2Message gDigestAck2Message) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Gossiper.MAX_GOSSIP_PACKET_SIZE);
        DataOutputStream dos = new DataOutputStream(bos);
        GossipDigestAck2Message.serializer().serialize(gDigestAck2Message, dos);
        Message message = new Message(localEndPoint_, Gossiper.GOSSIP_STAGE, GOSSIP_DIGEST_ACK2_VERB, bos.toByteArray());
        return message;
    }

    boolean sendGossipToLiveNode(Message message)
    {
        int size = liveEndpoints_.size();
        List<InetAddress> eps = new ArrayList<InetAddress>(liveEndpoints_);

        if ( rrIndex_ >= size )
        {
            rrIndex_ = -1;
        }

        InetAddress to = eps.get(++rrIndex_);
        if (logger_.isTraceEnabled())
            logger_.trace("Sending a GossipDigestSynMessage to " + to + " ...");
        MessagingService.instance().sendUdpOneWay(message, to);
        return seeds_.contains(to);
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     *  @param message message to sent
     *  @param epSet a set of endpoint from which a random endpoint is chosen.
     *  @return true if the chosen endpoint is also a seed.
     */
    boolean sendGossip(Message message, Set<InetAddress> epSet)
    {
        int size = epSet.size();
        /* Generate a random number from 0 -> size */
        List<InetAddress> liveEndPoints = new ArrayList<InetAddress>(epSet);
        int index = (size == 1) ? 0 : random_.nextInt(size);
        InetAddress to = liveEndPoints.get(index);
        if (logger_.isTraceEnabled())
            logger_.trace("Sending a GossipDigestSynMessage to " + to + " ...");
        MessagingService.instance().sendUdpOneWay(message, to);
        return seeds_.contains(to);
    }

    /* Sends a Gossip message to a live member and returns a reference to the member */
    boolean doGossipToLiveMember(Message message)
    {
        int size = liveEndpoints_.size();
        if ( size == 0 )
            return false;
        // return sendGossipToLiveNode(message);
        /* Use this for a cluster size >= 30 */
        return sendGossip(message, liveEndpoints_);
    }

    /* Sends a Gossip message to an unreachable member */
    void doGossipToUnreachableMember(Message message)
    {
        double liveEndPoints = liveEndpoints_.size();
        double unreachableEndPoints = unreachableEndpoints_.size();
        if ( unreachableEndPoints > 0 )
        {
            /* based on some probability */
            double prob = unreachableEndPoints / (liveEndPoints + 1);
            double randDbl = random_.nextDouble();
            if ( randDbl < prob )
                sendGossip(message, unreachableEndpoints_);
        }
    }

    /* Gossip to a seed for facilitating partition healing */
    void doGossipToSeed(Message message)
    {
        int size = seeds_.size();
        if ( size > 0 )
        {
            if ( size == 1 && seeds_.contains(localEndPoint_) )
            {
                return;
            }

            if ( liveEndpoints_.size() == 0 )
            {
                sendGossip(message, seeds_);
            }
            else
            {
                /* Gossip with the seed with some probability. */
                double probability = seeds_.size() / (double)( liveEndpoints_.size() + unreachableEndpoints_.size() );
                double randDbl = random_.nextDouble();
                if ( randDbl <= probability )
                    sendGossip(message, seeds_);
            }
        }
    }

    void doStatusCheck()
    {
        Set<InetAddress> eps = endPointStateMap_.keySet();

        for ( InetAddress endpoint : eps )
        {
            if ( endpoint.equals(localEndPoint_) )
                continue;

            FailureDetector.instance().interpret(endpoint);
            EndPointState epState = endPointStateMap_.get(endpoint);
            if ( epState != null )
            {
                long duration = System.currentTimeMillis() - epState.getUpdateTimestamp();
                if ( !epState.isAlive() && (duration > aVeryLongTime_) )
                {
                    evictFromMembership(endpoint);
                }
            }
        }
    }

    EndPointState getEndPointStateForEndPoint(InetAddress ep)
    {
        return endPointStateMap_.get(ep);
    }

    synchronized EndPointState getStateForVersionBiggerThan(InetAddress forEndpoint, int version)
    {
        if (logger_.isTraceEnabled())
            logger_.trace("Scanning for state greater than " + version + " for " + forEndpoint);
        EndPointState epState = endPointStateMap_.get(forEndpoint);
        EndPointState reqdEndPointState = null;

        if ( epState != null )
        {
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
            int localHbVersion = epState.getHeartBeatState().getHeartBeatVersion();
            if ( localHbVersion > version )
            {
                reqdEndPointState = new EndPointState(epState.getHeartBeatState());
            }
            Map<String, ApplicationState> appStateMap = epState.getApplicationState();
            /* Accumulate all application states whose versions are greater than "version" variable */
            Set<String> keys = appStateMap.keySet();
            for ( String key : keys )
            {
                ApplicationState appState = appStateMap.get(key);
                if ( appState.getStateVersion() > version )
                {
                    if ( reqdEndPointState == null )
                    {
                        reqdEndPointState = new EndPointState(epState.getHeartBeatState());
                    }
                    if (logger_.isTraceEnabled())
                        logger_.trace("Adding state " + key + ": " + appState.getState());
                    reqdEndPointState.addApplicationState(key, appState);
                }
            }
        }
        return reqdEndPointState;
    }

    /*
     * This method is called only from the JoinVerbHandler. This happens
     * when a new node coming up multicasts the JoinMessage. Here we need
     * to add the endPoint to the list of live endpoints.
    */
    synchronized void join(InetAddress from)
    {
        if ( !from.equals( localEndPoint_ ) )
        {
            /* Mark this endpoint as "live" */
        	liveEndpoints_.add(from);
            unreachableEndpoints_.remove(from);
        }
    }

    void notifyFailureDetector(List<GossipDigest> gDigests)
    {
        IFailureDetector fd = FailureDetector.instance();
        for ( GossipDigest gDigest : gDigests )
        {
            EndPointState localEndPointState = endPointStateMap_.get(gDigest.endPoint_);
            /*
             * If the local endpoint state exists then report to the FD only
             * if the versions workout.
            */
            if ( localEndPointState != null )
            {
                int localGeneration = endPointStateMap_.get(gDigest.endPoint_).getHeartBeatState().generation_;
                int remoteGeneration = gDigest.generation_;
                if ( remoteGeneration > localGeneration )
                {
                    fd.report(gDigest.endPoint_);
                    continue;
                }

                if ( remoteGeneration == localGeneration )
                {
                    int localVersion = getMaxEndPointStateVersion(localEndPointState);
                    //int localVersion = endPointStateMap_.get(gDigest.endPoint_).getHeartBeatState().getHeartBeatVersion();
                    int remoteVersion = gDigest.maxVersion_;
                    if ( remoteVersion > localVersion )
                    {
                        fd.report(gDigest.endPoint_);
                    }
                }
            }
        }
    }

    void notifyFailureDetector(Map<InetAddress, EndPointState> remoteEpStateMap)
    {
        IFailureDetector fd = FailureDetector.instance();
        Set<InetAddress> endpoints = remoteEpStateMap.keySet();
        for ( InetAddress endpoint : endpoints )
        {
            EndPointState remoteEndPointState = remoteEpStateMap.get(endpoint);
            EndPointState localEndPointState = endPointStateMap_.get(endpoint);
            /*
             * If the local endpoint state exists then report to the FD only
             * if the versions workout.
            */
            if ( localEndPointState != null )
            {
                int localGeneration = localEndPointState.getHeartBeatState().generation_;
                int remoteGeneration = remoteEndPointState.getHeartBeatState().generation_;
                if ( remoteGeneration > localGeneration )
                {
                    fd.report(endpoint);
                    continue;
                }

                if ( remoteGeneration == localGeneration )
                {
                    int localVersion = getMaxEndPointStateVersion(localEndPointState);
                    //int localVersion = localEndPointState.getHeartBeatState().getHeartBeatVersion();
                    int remoteVersion = remoteEndPointState.getHeartBeatState().getHeartBeatVersion();
                    if ( remoteVersion > localVersion )
                    {
                        fd.report(endpoint);
                    }
                }
            }
        }
    }

    void markAlive(InetAddress addr, EndPointState localState)
    {
        if (logger_.isTraceEnabled())
            logger_.trace("marking as alive " + addr);
        if ( !localState.isAlive() )
        {
            isAlive(addr, localState, true);
            logger_.info("InetAddress " + addr + " is now UP");
        }
    }

    private void handleNewJoin(InetAddress ep, EndPointState epState)
    {
    	logger_.info("Node " + ep + " has now joined.");
        /* Mark this endpoint as "live" */
        endPointStateMap_.put(ep, epState);
        isAlive(ep, epState, true);
        /* Notify interested parties about endpoint state change */
        doNotifications(ep, epState);
    }

    synchronized void applyStateLocally(Map<InetAddress, EndPointState> epStateMap)
    {
        Set<InetAddress> eps = epStateMap.keySet();
        for( InetAddress ep : eps )
        {
            if ( ep.equals( localEndPoint_ ) )
                continue;

            EndPointState localEpStatePtr = endPointStateMap_.get(ep);
            EndPointState remoteState = epStateMap.get(ep);
            /*
                If state does not exist just add it. If it does then add it only if the version
                of the remote copy is greater than the local copy.
            */
            if ( localEpStatePtr != null )
            {
            	int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
            	int remoteGeneration = remoteState.getHeartBeatState().getGeneration();

            	if (remoteGeneration > localGeneration)
            	{
            		handleNewJoin(ep, remoteState);
            	}
            	else if ( remoteGeneration == localGeneration )
            	{
	                /* manage the membership state */
	                int localMaxVersion = getMaxEndPointStateVersion(localEpStatePtr);
	                int remoteMaxVersion = getMaxEndPointStateVersion(remoteState);
	                if ( remoteMaxVersion > localMaxVersion )
	                {
	                    markAlive(ep, localEpStatePtr);
	                    applyHeartBeatStateLocally(ep, localEpStatePtr, remoteState);
	                    /* apply ApplicationState */
	                    applyApplicationStateLocally(ep, localEpStatePtr, remoteState);
	                }
            	}
            }
            else
            {
            	handleNewJoin(ep, remoteState);
            }
        }
    }

    void applyHeartBeatStateLocally(InetAddress addr, EndPointState localState, EndPointState remoteState)
    {
        HeartBeatState localHbState = localState.getHeartBeatState();
        HeartBeatState remoteHbState = remoteState.getHeartBeatState();

        if ( remoteHbState.getGeneration() > localHbState.getGeneration() )
        {
            markAlive(addr, localState);
            localState.setHeartBeatState(remoteHbState);
        }
        if ( localHbState.getGeneration() == remoteHbState.getGeneration() )
        {
            if ( remoteHbState.getHeartBeatVersion() > localHbState.getHeartBeatVersion() )
            {
                int oldVersion = localHbState.getHeartBeatVersion();
                localState.setHeartBeatState(remoteHbState);
                if (logger_.isTraceEnabled())
                    logger_.trace("Updating heartbeat state version to " + localState.getHeartBeatState().getHeartBeatVersion() + " from " + oldVersion + " for " + addr + " ...");
            }
        }
    }

    void applyApplicationStateLocally(InetAddress addr, EndPointState localStatePtr, EndPointState remoteStatePtr)
    {
        Map<String, ApplicationState> localAppStateMap = localStatePtr.getApplicationState();
        Map<String, ApplicationState> remoteAppStateMap = remoteStatePtr.getApplicationState();

        Set<String> remoteKeys = remoteAppStateMap.keySet();
        for ( String remoteKey : remoteKeys )
        {
            ApplicationState remoteAppState = remoteAppStateMap.get(remoteKey);
            ApplicationState localAppState = localAppStateMap.get(remoteKey);

            /* If state doesn't exist locally for this key then just apply it */
            if ( localAppState == null )
            {
                localStatePtr.addApplicationState(remoteKey, remoteAppState);
                /* notify interested parties of endpoint state change */
                EndPointState deltaState = new EndPointState(localStatePtr.getHeartBeatState());
                deltaState.addApplicationState(remoteKey, remoteAppState);
                doNotifications(addr, deltaState);
                continue;
            }

            int remoteGeneration = remoteStatePtr.getHeartBeatState().getGeneration();
            int localGeneration = localStatePtr.getHeartBeatState().getGeneration();
            assert remoteGeneration >= localGeneration; // SystemTable makes sure we never generate a smaller generation on start

            /* If the remoteGeneration is greater than localGeneration then apply state blindly */
            if ( remoteGeneration > localGeneration )
            {
                localStatePtr.addApplicationState(remoteKey, remoteAppState);
                /* notify interested parties of endpoint state change */
                EndPointState deltaState = new EndPointState(localStatePtr.getHeartBeatState());
                deltaState.addApplicationState(remoteKey, remoteAppState);
                doNotifications(addr, deltaState);
                continue;
            }

            /* If the generations are the same then apply state if the remote version is greater than local version. */
            if ( remoteGeneration == localGeneration )
            {
                int remoteVersion = remoteAppState.getStateVersion();
                int localVersion = localAppState.getStateVersion();

                if ( remoteVersion > localVersion )
                {
                    localStatePtr.addApplicationState(remoteKey, remoteAppState);
                    /* notify interested parties of endpoint state change */
                    EndPointState deltaState = new EndPointState(localStatePtr.getHeartBeatState());
                    deltaState.addApplicationState(remoteKey, remoteAppState);
                    doNotifications(addr, deltaState);
                }
            }
        }
    }

    void doNotifications(InetAddress addr, EndPointState epState)
    {
        for ( IEndPointStateChangeSubscriber subscriber : subscribers_ )
        {
            subscriber.onChange(addr, epState);
        }
    }

    synchronized void isAlive(InetAddress addr, EndPointState epState, boolean value)
    {
        epState.isAlive(value);
        if (value)
        {
            liveEndpoints_.add(addr);
            unreachableEndpoints_.remove(addr);
            for (IEndPointStateChangeSubscriber subscriber : subscribers_)
                subscriber.onAlive(addr, epState);
        }
        else
        {
            liveEndpoints_.remove(addr);
            unreachableEndpoints_.add(addr);
            for (IEndPointStateChangeSubscriber subscriber : subscribers_)
                subscriber.onDead(addr, epState);
        }
        if (epState.isAGossiper())
            return;
        epState.isAGossiper(true);
    }

    /* These are helper methods used from GossipDigestSynVerbHandler */
    Map<InetAddress, GossipDigest> getEndPointGossipDigestMap(List<GossipDigest> gDigestList)
    {
        Map<InetAddress, GossipDigest> epMap = new HashMap<InetAddress, GossipDigest>();
        for( GossipDigest gDigest : gDigestList )
        {
            epMap.put( gDigest.getEndPoint(), gDigest );
        }
        return epMap;
    }

    /* This is a helper method to get all EndPoints from a list of GossipDigests */
    InetAddress[] getEndPointsFromGossipDigest(List<GossipDigest> gDigestList)
    {
        Set<InetAddress> set = new HashSet<InetAddress>();
        for ( GossipDigest gDigest : gDigestList )
        {
            set.add( gDigest.getEndPoint() );
        }
        return set.toArray( new InetAddress[0] );
    }

    /* Request all the state for the endpoint in the gDigest */
    void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration)
    {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndPoint(), remoteGeneration, 0) );
    }

    /* Send all the data with version greater than maxRemoteVersion */
    void sendAll(GossipDigest gDigest, Map<InetAddress, EndPointState> deltaEpStateMap, int maxRemoteVersion)
    {
        EndPointState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getEndPoint(), maxRemoteVersion) ;
        if ( localEpStatePtr != null )
            deltaEpStateMap.put(gDigest.getEndPoint(), localEpStatePtr);
    }

    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    synchronized void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<InetAddress, EndPointState> deltaEpStateMap)
    {
        for ( GossipDigest gDigest : gDigestList )
        {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            EndPointState epStatePtr = endPointStateMap_.get(gDigest.getEndPoint());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
                request all the data for this endpoint.
            */
            if ( epStatePtr != null )
            {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this endpoint */
                int maxLocalVersion = getMaxEndPointStateVersion(epStatePtr);
                if ( remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion )
                    continue;

                if ( remoteGeneration > localGeneration )
                {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                }
                if ( remoteGeneration < localGeneration )
                {
                    /* send all data with generation = localgeneration and version > 0 */
                    sendAll(gDigest, deltaEpStateMap, 0);
                }
                if ( remoteGeneration == localGeneration )
                {
                    /*
                        If the max remote version is greater then we request the remote endpoint send us all the data
                        for this endpoint with version greater than the max version number we have locally for this
                        endpoint.
                        If the max remote version is lesser, then we send all the data we have locally for this endpoint
                        with version greater than the max remote version.
                    */
                    if ( maxRemoteVersion > maxLocalVersion )
                    {
                        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndPoint(), remoteGeneration, maxLocalVersion) );
                    }
                    if ( maxRemoteVersion < maxLocalVersion )
                    {
                        /* send all data with generation = localgeneration and version > maxRemoteVersion */
                        sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
                    }
                }
            }
            else
            {
                /* We are here since we have no data for this endpoint locally so request everything. */
                requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }
        }
    }

    /**
     * Start the gossiper with the generation # retrieved from the System
     * table
     */
    public void start(InetAddress localEndPoint, int generationNbr) throws IOException
    {
        localEndPoint_ = localEndPoint;
        /* Get the seeds from the config and initialize them. */
        Set<InetAddress> seedHosts = DatabaseDescriptor.getSeeds();
        for (InetAddress seed : seedHosts)
        {
            if (seed.equals(localEndPoint))
                continue;
            seeds_.add(seed);
        }

        /* initialize the heartbeat state for this localEndPoint */
        EndPointState localState = endPointStateMap_.get(localEndPoint_);
        if ( localState == null )
        {
            HeartBeatState hbState = new HeartBeatState(generationNbr, 0);
            localState = new EndPointState(hbState);
            localState.isAlive(true);
            localState.isAGossiper(true);
            endPointStateMap_.put(localEndPoint_, localState);
        }

        /* starts a timer thread */
        gossipTimer_.schedule( new GossipTimerTask(), Gossiper.intervalInMillis_, Gossiper.intervalInMillis_);
    }

    public synchronized void addApplicationState(String key, ApplicationState appState)
    {
        EndPointState epState = endPointStateMap_.get(localEndPoint_);
        assert epState != null;
        epState.addApplicationState(key, appState);
    }

    public void stop()
    {
        gossipTimer_.cancel();
    }
}

class JoinVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( JoinVerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isDebugEnabled())
          logger_.debug("Received a JoinMessage from " + from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );

        JoinMessage joinMessage = null;
        try
        {
            joinMessage = JoinMessage.serializer().deserialize(dis);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        if ( joinMessage.clusterId_.equals( DatabaseDescriptor.getClusterName() ) )
        {
            Gossiper.instance().join(from);
        }
    }
}

class GossipDigestSynVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( GossipDigestSynVerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isTraceEnabled())
            logger_.trace("Received a GossipDigestSynMessage from " + from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );

        try
        {
            GossipDigestSynMessage gDigestMessage = GossipDigestSynMessage.serializer().deserialize(dis);
            /* If the message is from a different cluster throw it away. */
            if ( !gDigestMessage.clusterId_.equals(DatabaseDescriptor.getClusterName()) )
                return;

            List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();
            /* Notify the Failure Detector */
            Gossiper.instance().notifyFailureDetector(gDigestList);

            doSort(gDigestList);

            List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
            Map<InetAddress, EndPointState> deltaEpStateMap = new HashMap<InetAddress, EndPointState>();
            Gossiper.instance().examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);

            GossipDigestAckMessage gDigestAck = new GossipDigestAckMessage(deltaGossipDigestList, deltaEpStateMap);
            Message gDigestAckMessage = Gossiper.instance().makeGossipDigestAckMessage(gDigestAck);
            if (logger_.isTraceEnabled())
                logger_.trace("Sending a GossipDigestAckMessage to " + from);
            MessagingService.instance().sendUdpOneWay(gDigestAckMessage, from);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /*
     * First construct a map whose key is the endpoint in the GossipDigest and the value is the
     * GossipDigest itself. Then build a list of version differences i.e difference between the
     * version in the GossipDigest and the version in the local state for a given InetAddress.
     * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
     * to the endpoint from the map that was initially constructed.
    */
    private void doSort(List<GossipDigest> gDigestList)
    {
        /* Construct a map of endpoint to GossipDigest. */
        Map<InetAddress, GossipDigest> epToDigestMap = new HashMap<InetAddress, GossipDigest>();
        for ( GossipDigest gDigest : gDigestList )
        {
            epToDigestMap.put(gDigest.getEndPoint(), gDigest);
        }

        /*
         * These digests have their maxVersion set to the difference of the version
         * of the local EndPointState and the version found in the GossipDigest.
        */
        List<GossipDigest> diffDigests = new ArrayList<GossipDigest>();
        for ( GossipDigest gDigest : gDigestList )
        {
            InetAddress ep = gDigest.getEndPoint();
            EndPointState epState = Gossiper.instance().getEndPointStateForEndPoint(ep);
            int version = (epState != null) ? Gossiper.instance().getMaxEndPointStateVersion( epState ) : 0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion() );
            diffDigests.add( new GossipDigest(ep, gDigest.getGeneration(), diffVersion) );
        }

        gDigestList.clear();
        Collections.sort(diffDigests);
        int size = diffDigests.size();
        /*
         * Report the digests in descending order. This takes care of the endpoints
         * that are far behind w.r.t this local endpoint
        */
        for ( int i = size - 1; i >= 0; --i )
        {
            gDigestList.add( epToDigestMap.get(diffDigests.get(i).getEndPoint()) );
        }
    }
}

class GossipDigestAckVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(GossipDigestAckVerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isTraceEnabled())
            logger_.trace("Received a GossipDigestAckMessage from " + from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );

        try
        {
            GossipDigestAckMessage gDigestAckMessage = GossipDigestAckMessage.serializer().deserialize(dis);
            List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
            Map<InetAddress, EndPointState> epStateMap = gDigestAckMessage.getEndPointStateMap();

            if ( epStateMap.size() > 0 )
            {
                /* Notify the Failure Detector */
                Gossiper.instance().notifyFailureDetector(epStateMap);
                Gossiper.instance().applyStateLocally(epStateMap);
            }

            /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
            Map<InetAddress, EndPointState> deltaEpStateMap = new HashMap<InetAddress, EndPointState>();
            for( GossipDigest gDigest : gDigestList )
            {
                InetAddress addr = gDigest.getEndPoint();
                EndPointState localEpStatePtr = Gossiper.instance().getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
                if ( localEpStatePtr != null )
                    deltaEpStateMap.put(addr, localEpStatePtr);
            }

            GossipDigestAck2Message gDigestAck2 = new GossipDigestAck2Message(deltaEpStateMap);
            Message gDigestAck2Message = Gossiper.instance().makeGossipDigestAck2Message(gDigestAck2);
            if (logger_.isTraceEnabled())
                logger_.trace("Sending a GossipDigestAck2Message to " + from);
            MessagingService.instance().sendUdpOneWay(gDigestAck2Message, from);
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
    }
}

class GossipDigestAck2VerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(GossipDigestAck2VerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isTraceEnabled())
            logger_.trace("Received a GossipDigestAck2Message from " + from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );
        GossipDigestAck2Message gDigestAck2Message = null;
        try
        {
            gDigestAck2Message = GossipDigestAck2Message.serializer().deserialize(dis);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        Map<InetAddress, EndPointState> remoteEpStateMap = gDigestAck2Message.getEndPointStateMap();
        /* Notify the Failure Detector */
        Gossiper.instance().notifyFailureDetector(remoteEpStateMap);
        Gossiper.instance().applyStateLocally(remoteEpStateMap);
    }
}

