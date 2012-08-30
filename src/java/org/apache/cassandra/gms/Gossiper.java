/*
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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 * Upon hearing a GossipShutdownMessage, this module will instantly mark the remote node as down in
 * the Failure Detector.
 */

public class Gossiper implements IFailureDetectionEventListener, GossiperMBean
{
    private static final String MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";

    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("GossipTasks");

    static final ApplicationState[] STATES = ApplicationState.values();
    static final List<String> DEAD_STATES = Arrays.asList(VersionedValue.REMOVING_TOKEN, VersionedValue.REMOVED_TOKEN,
                                                          VersionedValue.STATUS_LEFT, VersionedValue.HIBERNATE);

    private ScheduledFuture<?> scheduledGossipTask;
    public final static int intervalInMillis = 1000;
    public final static int QUARANTINE_DELAY = StorageService.RING_DELAY * 2;
    private static final Logger logger = LoggerFactory.getLogger(Gossiper.class);
    public static final Gossiper instance = new Gossiper();

    public static final long aVeryLongTime = 259200 * 1000; // 3 days
    private long FatClientTimeout;
    private final Random random = new Random();
    private final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress addr1,  InetAddress addr2)
        {
            return addr1.getHostAddress().compareTo(addr2.getHostAddress());
        }
    };

    /* subscribers for interest in EndpointState change */
    private final List<IEndpointStateChangeSubscriber> subscribers = new CopyOnWriteArrayList<IEndpointStateChangeSubscriber>();

    /* live member set */
    private final Set<InetAddress> liveEndpoints = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    /* unreachable member set */
    private final Map<InetAddress, Long> unreachableEndpoints = new ConcurrentHashMap<InetAddress, Long>();

    /* initial seeds for joining the cluster */
    private final Set<InetAddress> seeds = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    /* map where key is the endpoint and value is the state associated with the endpoint */
    final Map<InetAddress, EndpointState> endpointStateMap = new ConcurrentHashMap<InetAddress, EndpointState>();

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    private final Map<InetAddress, Long> justRemovedEndpoints = new ConcurrentHashMap<InetAddress, Long>();

    private final Map<InetAddress, Long> expireTimeEndpointMap = new ConcurrentHashMap<InetAddress, Long>();

    private class GossipTask implements Runnable
    {
        public void run()
        {
            try
            {
                //wait on messaging service to start listening
                MessagingService.instance().waitUntilListening();

                /* Update the local heartbeat counter. */
                endpointStateMap.get(FBUtilities.getBroadcastAddress()).getHeartBeatState().updateHeartBeat();
                if (logger.isTraceEnabled())
                    logger.trace("My heartbeat is now " + endpointStateMap.get(FBUtilities.getBroadcastAddress()).getHeartBeatState().getHeartBeatVersion());
                final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
                Gossiper.instance.makeRandomGossipDigest(gDigests);

                if ( gDigests.size() > 0 )
                {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                                                                           DatabaseDescriptor.getPartitionerName(),
                                                                           gDigests);
                    MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                                                        digestSynMessage,
                                                                                                        GossipDigestSyn.serializer);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = doGossipToLiveMember(message);

                    /* Gossip to some unreachable member with some probability to check if he is back up */
                    doGossipToUnreachableMember(message);

                    /* Gossip to a seed if we did not do so above, or we have seen less nodes
                       than there are seeds.  This prevents partitions where each group of nodes
                       is only gossiping to a subset of the seeds.

                       The most straightforward check would be to check that all the seeds have been
                       verified either as live or unreachable.  To avoid that computation each round,
                       we reason that:

                       either all the live nodes are seeds, in which case non-seeds that come online
                       will introduce themselves to a member of the ring by definition,

                       or there is at least one non-seed node in the list, in which case eventually
                       someone will gossip to it, and then do a gossip to a random seed from the
                       gossipedToSeed check.

                       See CASSANDRA-150 for more exposition. */
                    if (!gossipedToSeed || liveEndpoints.size() < seeds.size())
                        doGossipToSeed(message);

                    if (logger.isTraceEnabled())
                        logger.trace("Performing status check ...");
                    doStatusCheck();
                }
            }
            catch (Exception e)
            {
                logger.error("Gossip error", e);
            }
        }
    }

    private Gossiper()
    {
        // half of QUARATINE_DELAY, to ensure justRemovedEndpoints has enough leeway to prevent re-gossip
        FatClientTimeout = (long)(QUARANTINE_DELAY / 2);
        /* register with the Failure Detector for receiving Failure detector events */
        FailureDetector.instance.registerFailureDetectionEventListener(this);

        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Register for interesting state changes.
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    public void register(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers.add(subscriber);
    }

    /**
     * Unregister interest for state changes.
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    public void unregister(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers.remove(subscriber);
    }

    public Set<InetAddress> getLiveMembers()
    {
        Set<InetAddress> liveMbrs = new HashSet<InetAddress>(liveEndpoints);
        if (!liveMbrs.contains(FBUtilities.getBroadcastAddress()))
            liveMbrs.add(FBUtilities.getBroadcastAddress());
        return liveMbrs;
    }

    public Set<InetAddress> getUnreachableMembers()
    {
        return unreachableEndpoints.keySet();
    }

    public long getEndpointDowntime(InetAddress ep)
    {
        Long downtime = unreachableEndpoints.get(ep);
        if (downtime != null)
            return System.currentTimeMillis() - downtime;
        else
            return 0L;
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param endpoint end point that is convicted.
    */
    public void convict(InetAddress endpoint, double phi)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        if (epState.isAlive() && !isDeadState(epState))
        {
            markDead(endpoint, epState);
        }
    }

    /**
     * Return either: the greatest heartbeat or application state
     * @param epState
     * @return
     */
    int getMaxEndpointStateVersion(EndpointState epState)
    {
        int maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
        for (VersionedValue value : epState.getApplicationStateMap().values())
            maxVersion = Math.max(maxVersion,  value.version);
        return maxVersion;
    }

    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
    */
    private void evictFromMembership(InetAddress endpoint)
    {
        unreachableEndpoints.remove(endpoint);
        endpointStateMap.remove(endpoint);
        expireTimeEndpointMap.remove(endpoint);
        quarantineEndpoint(endpoint);
        if (logger.isDebugEnabled())
            logger.debug("evicting " + endpoint + " from gossip");
    }

    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    public void removeEndpoint(InetAddress endpoint)
    {
        // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onRemove(endpoint);

        liveEndpoints.remove(endpoint);
        unreachableEndpoints.remove(endpoint);
        // do not remove endpointState until the quarantine expires
        FailureDetector.instance.remove(endpoint);
        MessagingService.instance().resetVersion(endpoint);
        quarantineEndpoint(endpoint);
        if (logger.isDebugEnabled())
            logger.debug("removing endpoint " + endpoint);
    }

    /**
     * Quarantines the endpoint for QUARANTINE_DELAY
     * @param endpoint
     */
    private void quarantineEndpoint(InetAddress endpoint)
    {
        justRemovedEndpoints.put(endpoint, System.currentTimeMillis());
    }

    /**
     * Remove the Endpoint and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     * @param endpoint The endpoint that has been replaced
     */
    public void replacedEndpoint(InetAddress endpoint)
    {
        removeEndpoint(endpoint);
        evictFromMembership(endpoint);
    }

    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param gDigests list of Gossip Digests.
    */
    private void makeRandomGossipDigest(List<GossipDigest> gDigests)
    {
        EndpointState epState;
        int generation = 0;
        int maxVersion = 0;

        // local epstate will be part of endpointStateMap
        List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMap.keySet());
        Collections.shuffle(endpoints, random);
        for (InetAddress endpoint : endpoints)
        {
            epState = endpointStateMap.get(endpoint);
            if (epState != null)
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxEndpointStateVersion(epState);
            }
            gDigests.add(new GossipDigest(endpoint, generation, maxVersion));
        }

        if (logger.isTraceEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for ( GossipDigest gDigest : gDigests )
            {
                sb.append(gDigest);
                sb.append(" ");
            }
                logger.trace("Gossip Digests are : " + sb.toString());
        }
    }

    /**
     * This method will begin removing an existing endpoint from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removetoken' invoked
     *
     * @param endpoint - the endpoint being removed
     * @param hostId - the ID of the host being removed
     * @param localHostId - my own host ID for replication coordination
     */
    public void advertiseRemoving(InetAddress endpoint, UUID hostId, UUID localHostId)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        // remember this node's generation
        int generation = epState.getHeartBeatState().getGeneration();
        logger.info("Removing host: {}", hostId);
        logger.info("Sleeping for " + StorageService.RING_DELAY + "ms to ensure " + endpoint + " does not change");
        try
        {
            Thread.sleep(StorageService.RING_DELAY);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        // make sure it did not change
        epState = endpointStateMap.get(endpoint);
        if (epState.getHeartBeatState().getGeneration() != generation)
            throw new RuntimeException("Endpoint " + endpoint + " generation changed while trying to remove it");
        // update the other node's generation to mimic it as if it had changed it itself
        logger.info("Advertising removal for " + endpoint);
        epState.updateTimestamp(); // make sure we don't evict it too soon
        epState.getHeartBeatState().forceNewerGenerationUnsafe();
        epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.removingNonlocal(hostId));
        epState.addApplicationState(ApplicationState.REMOVAL_COORDINATOR, StorageService.instance.valueFactory.removalCoordinator(localHostId));
        endpointStateMap.put(endpoint, epState);
    }

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertiseRemoving
     * @param endpoint
     * @param hostId
     */
    public void advertiseTokenRemoved(InetAddress endpoint, UUID hostId)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        epState.updateTimestamp(); // make sure we don't evict it too soon
        epState.getHeartBeatState().forceNewerGenerationUnsafe();
        epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.removedNonlocal(hostId,computeExpireTime()));
        logger.info("Completing removal of " + endpoint);
        endpointStateMap.put(endpoint, epState);
        // ensure at least one gossip round occurs before returning
        try
        {
            Thread.sleep(intervalInMillis * 2);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any endpoint from the ring,
     * even if it does not know about it.
     * This should only ever be called by human via JMX.
     * @param  address
     * @throws UnknownHostException
     */
    public void unsafeAssassinateEndpoint(String address) throws UnknownHostException
    {
        InetAddress endpoint = InetAddress.getByName(address);
        EndpointState epState = endpointStateMap.get(endpoint);
        Collection<Token> tokens = null;
        logger.warn("Assassinating {} via gossip", endpoint);
        if (epState == null)
        {
            epState = new EndpointState(new HeartBeatState((int)((System.currentTimeMillis() + 60000) / 1000), 9999));
        }
        else
        {
            try
            {
                tokens = StorageService.instance.getTokenMetadata().getTokens(endpoint);
            }
            catch (AssertionError e)
            {
            }
            int generation = epState.getHeartBeatState().getGeneration();
            logger.info("Sleeping for " + StorageService.RING_DELAY + "ms to ensure " + endpoint + " does not change");
            try
            {
                Thread.sleep(StorageService.RING_DELAY);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            // make sure it did not change
            epState = endpointStateMap.get(endpoint);
            if (epState.getHeartBeatState().getGeneration() != generation)
                throw new RuntimeException("Endpoint " + endpoint + " generation changed while trying to remove it");
            epState.updateTimestamp(); // make sure we don't evict it too soon
            epState.getHeartBeatState().forceNewerGenerationUnsafe();
        }
        if (tokens == null)
            tokens = Arrays.asList(StorageService.instance.getBootstrapToken());
        // do not pass go, do not collect 200 dollars, just gtfo
        epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.left(tokens, computeExpireTime()));
        handleMajorStateChange(endpoint, epState);
        try
        {
            Thread.sleep(intervalInMillis * 4);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        logger.warn("Finished killing {}", endpoint);
    }

    public boolean isKnownEndpoint(InetAddress endpoint)
    {
        return endpointStateMap.containsKey(endpoint);
    }

    public int getCurrentGenerationNumber(InetAddress endpoint)
    {
        return endpointStateMap.get(endpoint).getHeartBeatState().getGeneration();
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     *
     * @param message
     * @param epSet a set of endpoint from which a random endpoint is chosen.
     *  @return true if the chosen endpoint is also a seed.
     */
    private boolean sendGossip(MessageOut<GossipDigestSyn> message, Set<InetAddress> epSet)
    {
        int size = epSet.size();
        if (size < 1)
            return false;
        /* Generate a random number from 0 -> size */
        List<InetAddress> liveEndpoints = new ArrayList<InetAddress>(epSet);
        int index = (size == 1) ? 0 : random.nextInt(size);
        InetAddress to = liveEndpoints.get(index);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestSyn to {} ...", to);
        MessagingService.instance().sendOneWay(message, to);
        return seeds.contains(to);
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    private boolean doGossipToLiveMember(MessageOut<GossipDigestSyn> message)
    {
        int size = liveEndpoints.size();
        if ( size == 0 )
            return false;
        return sendGossip(message, liveEndpoints);
    }

    /* Sends a Gossip message to an unreachable member */
    private void doGossipToUnreachableMember(MessageOut<GossipDigestSyn> message)
    {
        double liveEndpointCount = liveEndpoints.size();
        double unreachableEndpointCount = unreachableEndpoints.size();
        if ( unreachableEndpointCount > 0 )
        {
            /* based on some probability */
            double prob = unreachableEndpointCount / (liveEndpointCount + 1);
            double randDbl = random.nextDouble();
            if ( randDbl < prob )
                sendGossip(message, unreachableEndpoints.keySet());
        }
    }

    /* Gossip to a seed for facilitating partition healing */
    private void doGossipToSeed(MessageOut<GossipDigestSyn> prod)
    {
        int size = seeds.size();
        if ( size > 0 )
        {
            if ( size == 1 && seeds.contains(FBUtilities.getBroadcastAddress()) )
            {
                return;
            }

            if ( liveEndpoints.size() == 0 )
            {
                sendGossip(prod, seeds);
            }
            else
            {
                /* Gossip with the seed with some probability. */
                double probability = seeds.size() / (double)( liveEndpoints.size() + unreachableEndpoints.size() );
                double randDbl = random.nextDouble();
                if ( randDbl <= probability )
                    sendGossip(prod, seeds);
            }
        }
    }

    private void doStatusCheck()
    {
        long now = System.currentTimeMillis();

        Set<InetAddress> eps = endpointStateMap.keySet();
        for ( InetAddress endpoint : eps )
        {
            if ( endpoint.equals(FBUtilities.getBroadcastAddress()) )
                continue;

            FailureDetector.instance.interpret(endpoint);
            EndpointState epState = endpointStateMap.get(endpoint);
            if ( epState != null )
            {
                long duration = now - epState.getUpdateTimestamp();

                // check if this is a fat client. fat clients are removed automatically from
                // gossip after FatClientTimeout.  Do not remove dead states here.
                if (!isDeadState(epState) && !epState.isAlive() && !StorageService.instance.getTokenMetadata().isMember(endpoint) && !justRemovedEndpoints.containsKey(endpoint) && (duration > FatClientTimeout))
                {
                    logger.info("FatClient " + endpoint + " has been silent for " + FatClientTimeout + "ms, removing from gossip");
                    removeEndpoint(endpoint); // will put it in justRemovedEndpoints to respect quarantine delay
                    evictFromMembership(endpoint); // can get rid of the state immediately
                }

                // check for dead state removal
                long expireTime = getExpireTimeForEndpoint(endpoint);
                if (!epState.isAlive() && (now > expireTime)
                        && (!StorageService.instance.getTokenMetadata().isMember(endpoint)))
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("time is expiring for endpoint : " + endpoint + " (" + expireTime + ")");
                    }
                    evictFromMembership(endpoint);
                }
            }
        }

        if (!justRemovedEndpoints.isEmpty())
        {
            for (Entry<InetAddress, Long> entry : justRemovedEndpoints.entrySet())
            {
                if ((now - entry.getValue()) > QUARANTINE_DELAY)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(QUARANTINE_DELAY + " elapsed, " + entry.getKey() + " gossip quarantine over");
                    justRemovedEndpoints.remove(entry.getKey());
                }
            }
        }
    }

    protected long getExpireTimeForEndpoint(InetAddress endpoint)
    {
        /* default expireTime is aVeryLongTime */
        long expireTime = computeExpireTime();
        if (expireTimeEndpointMap.containsKey(endpoint))
        {
            expireTime = expireTimeEndpointMap.get(endpoint);
        }
        return expireTime;
    }

    public EndpointState getEndpointStateForEndpoint(InetAddress ep)
    {
        return endpointStateMap.get(ep);
    }

    public Set<Entry<InetAddress, EndpointState>> getEndpointStates()
    {
        return endpointStateMap.entrySet();
    }

    EndpointState getStateForVersionBiggerThan(InetAddress forEndpoint, int version)
    {
        EndpointState epState = endpointStateMap.get(forEndpoint);
        EndpointState reqdEndpointState = null;

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
                reqdEndpointState = new EndpointState(epState.getHeartBeatState());
                if (logger.isTraceEnabled())
                    logger.trace("local heartbeat version " + localHbVersion + " greater than " + version + " for " + forEndpoint);
            }
            /* Accumulate all application states whose versions are greater than "version" variable */
            for (Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet())
            {
                VersionedValue value = entry.getValue();
                if ( value.version > version )
                {
                    if ( reqdEndpointState == null )
                    {
                        reqdEndpointState = new EndpointState(epState.getHeartBeatState());
                    }
                    final ApplicationState key = entry.getKey();
                    if (logger.isTraceEnabled())
                        logger.trace("Adding state " + key + ": " + value.value);
                    reqdEndpointState.addApplicationState(key, value);
                }
            }
        }
        return reqdEndpointState;
    }

    /** determine which endpoint started up earlier */
    public int compareEndpointStartup(InetAddress addr1, InetAddress addr2)
    {
        EndpointState ep1 = getEndpointStateForEndpoint(addr1);
        EndpointState ep2 = getEndpointStateForEndpoint(addr2);
        assert ep1 != null && ep2 != null;
        return ep1.getHeartBeatState().getGeneration() - ep2.getHeartBeatState().getGeneration();
    }

    void notifyFailureDetector(List<GossipDigest> gDigests)
    {
        for ( GossipDigest gDigest : gDigests )
        {
            notifyFailureDetector(gDigest.endpoint, endpointStateMap.get(gDigest.endpoint));
        }
    }

    void notifyFailureDetector(Map<InetAddress, EndpointState> remoteEpStateMap)
    {
        for (Entry<InetAddress, EndpointState> entry : remoteEpStateMap.entrySet())
        {
            notifyFailureDetector(entry.getKey(), entry.getValue());
        }
    }

    void notifyFailureDetector(InetAddress endpoint, EndpointState remoteEndpointState)
    {
        EndpointState localEndpointState = endpointStateMap.get(endpoint);
        /*
         * If the local endpoint state exists then report to the FD only
         * if the versions workout.
        */
        if ( localEndpointState != null )
        {
            IFailureDetector fd = FailureDetector.instance;
            int localGeneration = localEndpointState.getHeartBeatState().getGeneration();
            int remoteGeneration = remoteEndpointState.getHeartBeatState().getGeneration();
            if ( remoteGeneration > localGeneration )
            {
                localEndpointState.updateTimestamp();
                // this node was dead and the generation changed, this indicates a reboot, or possibly a takeover
                // we will clean the fd intervals for it and relearn them
                if (!localEndpointState.isAlive())
                {
                    logger.debug("Clearing interval times for {} due to generation change", endpoint);
                    fd.clear(endpoint);
                }
                fd.report(endpoint);
                return;
            }

            if ( remoteGeneration == localGeneration )
            {
                int localVersion = getMaxEndpointStateVersion(localEndpointState);
                int remoteVersion = remoteEndpointState.getHeartBeatState().getHeartBeatVersion();
                if ( remoteVersion > localVersion )
                {
                    localEndpointState.updateTimestamp();
                    // just a version change, report to the fd
                    fd.report(endpoint);
                }
            }
        }

    }

    private void markAlive(InetAddress addr, EndpointState localState)
    {
        if (logger.isTraceEnabled())
            logger.trace("marking as alive {}", addr);
        localState.markAlive();
        localState.updateTimestamp(); // prevents doStatusCheck from racing us and evicting if it was down > aVeryLongTime
        liveEndpoints.add(addr);
        unreachableEndpoints.remove(addr);
        expireTimeEndpointMap.remove(addr);
        logger.debug("removing expire time for endpoint : " + addr);
        logger.info("InetAddress {} is now UP", addr);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onAlive(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified " + subscribers);
    }

    private void markDead(InetAddress addr, EndpointState localState)
    {
        if (logger.isTraceEnabled())
            logger.trace("marking as dead {}", addr);
        localState.markDead();
        liveEndpoints.remove(addr);
        unreachableEndpoints.put(addr, System.currentTimeMillis());
        logger.info("InetAddress {} is now dead.", addr);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onDead(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified " + subscribers);
    }

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep endpoint
     * @param epState EndpointState for the endpoint
     */
    private void handleMajorStateChange(InetAddress ep, EndpointState epState)
    {
        if (!isDeadState(epState))
        {
            if (endpointStateMap.get(ep) != null)
                logger.info("Node {} has restarted, now UP", ep);
            else
                logger.info("Node {} is now part of the cluster", ep);
        }
        if (logger.isTraceEnabled())
            logger.trace("Adding endpoint state for " + ep);
        endpointStateMap.put(ep, epState);

        // the node restarted: it is up to the subscriber to take whatever action is necessary
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onRestart(ep, epState);

        if (!isDeadState(epState))
            markAlive(ep, epState);
        else
        {
            logger.debug("Not marking " + ep + " alive due to dead state");
            markDead(ep, epState);
        }
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onJoin(ep, epState);
    }

    private Boolean isDeadState(EndpointState epState)
    {
        if (epState.getApplicationState(ApplicationState.STATUS) == null)
            return false;
        String value = epState.getApplicationState(ApplicationState.STATUS).value;
        String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        String state = pieces[0];
        for (String deadstate : DEAD_STATES)
        {
            if (state.equals(deadstate))
                return true;
        }
        return false;
    }

    void applyStateLocally(Map<InetAddress, EndpointState> epStateMap)
    {
        for (Entry<InetAddress, EndpointState> entry : epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            if ( ep.equals(FBUtilities.getBroadcastAddress()))
                continue;
            if (justRemovedEndpoints.containsKey(ep))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Ignoring gossip for " + ep + " because it is quarantined");
                continue;
            }

            EndpointState localEpStatePtr = endpointStateMap.get(ep);
            EndpointState remoteState = entry.getValue();
            /*
                If state does not exist just add it. If it does then add it if the remote generation is greater.
                If there is a generation tie, attempt to break it by heartbeat version.
            */
            if ( localEpStatePtr != null )
            {
                int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
                int remoteGeneration = remoteState.getHeartBeatState().getGeneration();
                if (logger.isTraceEnabled())
                    logger.trace(ep + "local generation " + localGeneration + ", remote generation " + remoteGeneration);

                if (remoteGeneration > localGeneration)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Updating heartbeat state generation to " + remoteGeneration + " from " + localGeneration + " for " + ep);
                    // major state change will handle the update by inserting the remote state directly
                    handleMajorStateChange(ep, remoteState);
                }
                else if ( remoteGeneration == localGeneration ) // generation has not changed, apply new states
                {
                    /* find maximum state */
                    int localMaxVersion = getMaxEndpointStateVersion(localEpStatePtr);
                    int remoteMaxVersion = getMaxEndpointStateVersion(remoteState);
                    if ( remoteMaxVersion > localMaxVersion )
                    {
                        // apply states, but do not notify since there is no major change
                        applyNewStates(ep, localEpStatePtr, remoteState);
                    }
                    else if (logger.isTraceEnabled())
                            logger.trace("Ignoring remote version " + remoteMaxVersion + " <= " + localMaxVersion + " for " + ep);
                    if (!localEpStatePtr.isAlive() && !isDeadState(localEpStatePtr)) // unless of course, it was dead
                        markAlive(ep, localEpStatePtr);
                }
                else
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Ignoring remote generation " + remoteGeneration + " < " + localGeneration);
                }
            }
            else
            {
                // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not alive
                FailureDetector.instance.report(ep);
                handleMajorStateChange(ep, remoteState);
            }
        }
    }

    private void applyNewStates(InetAddress addr, EndpointState localState, EndpointState remoteState)
    {
        // don't assert here, since if the node restarts the version will go back to zero
        int oldVersion = localState.getHeartBeatState().getHeartBeatVersion();

        localState.setHeartBeatState(remoteState.getHeartBeatState());
        if (logger.isTraceEnabled())
            logger.trace("Updating heartbeat state version to " + localState.getHeartBeatState().getHeartBeatVersion() + " from " + oldVersion + " for " + addr + " ...");

        for (Entry<ApplicationState, VersionedValue> remoteEntry : remoteState.getApplicationStateMap().entrySet())
        {
            ApplicationState remoteKey = remoteEntry.getKey();
            VersionedValue remoteValue = remoteEntry.getValue();

            assert remoteState.getHeartBeatState().getGeneration() == localState.getHeartBeatState().getGeneration();
            localState.addApplicationState(remoteKey, remoteValue);
            doNotifications(addr, remoteKey, remoteValue);
        }
    }

    // notify that an application state has changed
    private void doNotifications(InetAddress addr, ApplicationState state, VersionedValue value)
    {
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
        {
            subscriber.onChange(addr, state, value);
        }
    }

    /* Request all the state for the endpoint in the gDigest */
    private void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration)
    {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndpoint(), remoteGeneration, 0) );
        if (logger.isTraceEnabled())
            logger.trace("requestAll for " + gDigest.getEndpoint());
    }

    /* Send all the data with version greater than maxRemoteVersion */
    private void sendAll(GossipDigest gDigest, Map<InetAddress, EndpointState> deltaEpStateMap, int maxRemoteVersion)
    {
        EndpointState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getEndpoint(), maxRemoteVersion) ;
        if ( localEpStatePtr != null )
            deltaEpStateMap.put(gDigest.getEndpoint(), localEpStatePtr);
    }

    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<InetAddress, EndpointState> deltaEpStateMap)
    {
        for ( GossipDigest gDigest : gDigestList )
        {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            EndpointState epStatePtr = endpointStateMap.get(gDigest.getEndpoint());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
                request all the data for this endpoint.
            */
            if ( epStatePtr != null )
            {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this endpoint */
                int maxLocalVersion = getMaxEndpointStateVersion(epStatePtr);
                if ( remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion )
                    continue;

                if ( remoteGeneration > localGeneration )
                {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                }
                else if ( remoteGeneration < localGeneration )
                {
                    /* send all data with generation = localgeneration and version > 0 */
                    sendAll(gDigest, deltaEpStateMap, 0);
                }
                else if ( remoteGeneration == localGeneration )
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
                        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndpoint(), remoteGeneration, maxLocalVersion) );
                    }
                    else if ( maxRemoteVersion < maxLocalVersion )
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
    public void start(int generationNbr)
    {
        /* Get the seeds from the config and initialize them. */
        Set<InetAddress> seedHosts = DatabaseDescriptor.getSeeds();
        for (InetAddress seed : seedHosts)
        {
            if (seed.equals(FBUtilities.getBroadcastAddress()))
                continue;
            seeds.add(seed);
        }

        /* initialize the heartbeat state for this localEndpoint */
        maybeInitializeLocalState(generationNbr);
        EndpointState localState = endpointStateMap.get(FBUtilities.getBroadcastAddress());

        //notify snitches that Gossiper is about to start
        DatabaseDescriptor.getEndpointSnitch().gossiperStarting();
        if (logger.isTraceEnabled())
            logger.trace("gossip started with generation " + localState.getHeartBeatState().getGeneration());

        scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(),
                                                              Gossiper.intervalInMillis,
                                                              Gossiper.intervalInMillis,
                                                              TimeUnit.MILLISECONDS);
    }

    // initialize local HB state if needed.
    public void maybeInitializeLocalState(int generationNbr)
    {
        EndpointState localState = endpointStateMap.get(FBUtilities.getBroadcastAddress());
        if ( localState == null )
        {
            HeartBeatState hbState = new HeartBeatState(generationNbr);
            localState = new EndpointState(hbState);
            localState.markAlive();
            endpointStateMap.put(FBUtilities.getBroadcastAddress(), localState);
        }
    }


    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    public void addSavedEndpoint(InetAddress ep)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
        {
            logger.debug("Attempt to add self as saved endpoint");
            return;
        }
        EndpointState epState = new EndpointState(new HeartBeatState(0));
        epState.markDead();
        endpointStateMap.put(ep, epState);
        unreachableEndpoints.put(ep, System.currentTimeMillis());
        if (logger.isTraceEnabled())
            logger.trace("Adding saved endpoint " + ep + " " + epState.getHeartBeatState().getGeneration());
    }

    public void addLocalApplicationState(ApplicationState state, VersionedValue value)
    {
        EndpointState epState = endpointStateMap.get(FBUtilities.getBroadcastAddress());
        assert epState != null;
        epState.addApplicationState(state, value);
        doNotifications(FBUtilities.getBroadcastAddress(), state, value);
    }

    public void stop()
    {
        scheduledGossipTask.cancel(false);
        logger.info("Announcing shutdown");
        try
        {
            Thread.sleep(intervalInMillis);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        MessageOut message = new MessageOut(MessagingService.Verb.GOSSIP_SHUTDOWN);
        for (InetAddress ep : liveEndpoints)
            MessagingService.instance().sendOneWay(message, ep);
    }

    public boolean isEnabled()
    {
        return (scheduledGossipTask != null) && (!scheduledGossipTask.isCancelled());
    }

    /**
     * This should *only* be used for testing purposes.
     */
    public void initializeNodeUnsafe(InetAddress addr, int generationNbr) {
        /* initialize the heartbeat state for this localEndpoint */
        EndpointState localState = endpointStateMap.get(addr);
        if ( localState == null )
        {
            HeartBeatState hbState = new HeartBeatState(generationNbr);
            localState = new EndpointState(hbState);
            localState.markAlive();
            endpointStateMap.put(addr, localState);
        }
        // always add the version state
        localState.addApplicationState(ApplicationState.NET_VERSION, StorageService.instance.valueFactory.networkVersion());
    }

    public long getEndpointDowntime(String address) throws UnknownHostException
    {
        return getEndpointDowntime(InetAddress.getByName(address));
    }

    public int getCurrentGenerationNumber(String address) throws UnknownHostException
    {
        return getCurrentGenerationNumber(InetAddress.getByName(address));
    }

    public void addExpireTimeForEndpoint(InetAddress endpoint, long expireTime)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("adding expire time for endpoint : " + endpoint + " (" + expireTime + ")");
        }
        expireTimeEndpointMap.put(endpoint, expireTime);
    }

    public static long computeExpireTime() {
        return System.currentTimeMillis() + Gossiper.aVeryLongTime;
    }

}
