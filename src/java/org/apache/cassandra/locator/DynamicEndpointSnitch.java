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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.LatencyMeasurementType;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.PingMessage;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.net.MessagingService.Verb.PING;
import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;


/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 * Note that the subclasses (e.g. {@link DynamicEndpointSnitchHistogram}) are responsible for actually measuring
 * latency and populating the {@link #scores} map.
 */
public abstract class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicEndpointSnitch.class);

    // Latency measurement and ranking. The samples contain latency measurements and the scores are used for ranking
    protected boolean registered = false;
    protected static final boolean USE_SEVERITY = !Boolean.getBoolean("cassandra.ignore_dynamic_snitch_severity");
    protected volatile Map<InetAddressAndPort, Double> scores = new HashMap<>();
    protected final Map<InetAddressAndPort, AnnotatedMeasurement> samples = new ConcurrentHashMap<>();

    // Latency probe functionality for actively probing endpoints that we haven't measured recently but are ranking
    public static final long MAX_PROBE_INTERVAL_MS = Long.getLong("cassandra.dynamic_snitch_max_probe_interval_ms", 60 * 10 * 1000L);
    public static final long MIN_PROBE_INTERVAL_MS = Long.getLong("cassandra.dynamic_snitch_min_probe_interval_ms", 10 * 1000L) ;
    protected enum ProbeType {EXP, CONSTANT, NO}
    protected final Set<Long> probeTimes = new HashSet<>();
    protected final List<InetAddressAndPort> latencyProbeSequence = new ArrayList<>();
    protected volatile int currentProbePosition = 0;

    // User configuration of the snitch tunables
    protected volatile int dynamicUpdateInterval = -1;
    protected volatile int dynamicLatencyProbeInterval = -1;
    protected volatile double dynamicBadnessThreshold = 0;

    // the score for a merged set of endpoints must be this much worse than the score for separate endpoints to
    // warrant not merging two ranges into a single range
    private static final double RANGE_MERGING_PREFERENCE = 1.5;

    private String mbeanName;
    private boolean mbeanRegistered = false;

    public final IEndpointSnitch subsnitch;

    private volatile ScheduledFuture<?> updateScheduler;
    private volatile ScheduledFuture<?> latencyProbeScheduler;

    private final Runnable update = this::updateScores;
    private final Runnable latencyProbe = this::maybeSendLatencyProbe;

    public DynamicEndpointSnitch(IEndpointSnitch snitch)
    {
        this(snitch, null);
    }

    protected DynamicEndpointSnitch(IEndpointSnitch snitch, String instance)
    {
        mbeanName = "org.apache.cassandra.db:type=DynamicEndpointSnitch";
        if (instance != null)
            mbeanName += ",instance=" + instance;
        subsnitch = snitch;

        if (DatabaseDescriptor.isDaemonInitialized())
        {
            applyConfigChanges(DatabaseDescriptor.getDynamicUpdateInterval(),
                               DatabaseDescriptor.getDynamicLatencyProbeInterval(),
                               DatabaseDescriptor.getDynamicBadnessThreshold());
            registerMBean();
        }
    }

    /**
     * Allows subclasses to inject new ways of measuring latency back to this abstract base class.
     */
    protected interface ISnitchMeasurement
    {
        void sample(long value);
        double measure();
        Iterable<Double> measurements();
    }

    /**
     * Adds some boookeeping that the DES uses over top of the various metrics techniques used by the
     * implementations. This is used to allow CASSANDRA-14459 latency probes
     *
     * recentlyRequested is updated through {@link DynamicEndpointSnitch#sortedByProximity(InetAddressAndPort, ReplicaCollection)}
     *
     * intervalsSinceLastMeasure and probesSent are manipulated via {@link DynamicEndpointSnitch#latencyProbeNeeded(Map, List, int)} ()}
     */
    protected static class AnnotatedMeasurement
    {
        // Used to prohibit probes against non requested nodes, for example when token aware clients are used
        // most of the cluster will never be ranked at all and we shouldn't probe them either.
        public volatile boolean recentlyRequested = false;
        // Used for exponential backoff of probes against a single host
        public final AtomicLong intervalsSinceLastMeasure = new AtomicLong(0);
        // The underlying measurement technique. E.g. a median filter (histogram) or ema low pass filter (EMA)
        public final ISnitchMeasurement measurement;

        public AnnotatedMeasurement(ISnitchMeasurement measurement)
        {
            this.measurement = measurement;
        }
    }

    /**
     * Allows the subclasses to inject ISnitchMeasurement instances back into this common class so that common
     * functionality can be handled here
     * @param initialValue The initial value of the measurement, some implementations may use this, others may not
     * @return a constructed instance of an ISnitchMeasurement interface
     */
    abstract protected ISnitchMeasurement measurementImpl(long initialValue);

    /**
     * Records a latency. This MUST be cheap as it is called in the fast path
     */
    public void receiveTiming(InetAddressAndPort address, long latency, LatencyMeasurementType measurementType)
    {
        if (measurementType == LatencyMeasurementType.IGNORE)
           return;

        AnnotatedMeasurement sample = samples.get(address);

        if (sample == null)
        {
            AnnotatedMeasurement maybeNewSample = new AnnotatedMeasurement(measurementImpl(latency));
            sample = samples.putIfAbsent(address, maybeNewSample);
            if (sample == null)
                sample = maybeNewSample;
        }

        if (measurementType == LatencyMeasurementType.READ && sample.intervalsSinceLastMeasure.get() > 0)
            sample.intervalsSinceLastMeasure.lazySet(0);

        sample.measurement.sample(latency);
    }

    @VisibleForTesting
    protected void reset()
    {
        currentProbePosition = 0;
        latencyProbeSequence.clear();
        scores.clear();
        samples.clear();
    }

    @VisibleForTesting
    void updateScores()
    {
        if (!StorageService.instance.isGossipActive())
            return;

        if (!registered)
        {
            if (MessagingService.instance() != null)
            {
                MessagingService.instance().register(this);
                registered = true;
            }
        }

        this.scores = calculateScores();
    }

    /**
     * This is generally expensive and is called periodically not on the fast path.
     * @return a freshly constructed scores map.
     */
    public Map<InetAddressAndPort, Double> calculateScores()
    {
        double maxLatency = 1;

        Map<InetAddressAndPort, Double> measurements = new HashMap<>(samples.size());

        // We're going to weight the latency for each host against the worst one we see, to
        // arrive at sort of a 'badness percentage' for them. First, find the worst for each:
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry : samples.entrySet())
        {
            // This is expensive for e.g. the Histogram, so do it once and cache the result
            double measure = entry.getValue().measurement.measure();
            if (measure > maxLatency)
                maxLatency = measure;
            measurements.put(entry.getKey(), measure);
        }

        HashMap<InetAddressAndPort, Double> newScores = new HashMap<>(measurements.size());

        // now make another pass to do the weighting based on the maximums we found before
        for (Map.Entry<InetAddressAndPort, Double> entry : measurements.entrySet())
        {
            double score = entry.getValue() / maxLatency;
            // finally, add the severity without any weighting, since hosts scale this relative to their own load and the size of the task causing the severity.
            // "Severity" is basically a measure of compaction activity (CASSANDRA-3722).
            if (USE_SEVERITY)
                score += getSeverity(entry.getKey());
            // lowest score (least amount of badness) wins.
            newScores.put(entry.getKey(), score);
        }

        return newScores;
    }

    /**
     * Update configuration from {@link DatabaseDescriptor} and restart the various scheduler tasks
     * if the configured rates for these tasks have changed.
     */
    public void applyConfigChanges(int newDynamicUpdateInternal, int newDynamicLatencyProbeInterval, double newDynamicBadnessThreshold)
    {
        if (dynamicUpdateInterval != newDynamicUpdateInternal)
        {
            dynamicUpdateInterval = newDynamicUpdateInternal;
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                if (updateScheduler != null)
                    updateScheduler.cancel(false);
                updateScheduler = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(update, dynamicUpdateInterval, dynamicUpdateInterval, TimeUnit.MILLISECONDS);
            }
        }

        if (this.dynamicLatencyProbeInterval != newDynamicLatencyProbeInterval)
        {
            this.dynamicLatencyProbeInterval = newDynamicLatencyProbeInterval;
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                if (latencyProbeScheduler != null)
                    latencyProbeScheduler.cancel(false);

                probeTimes.clear();
                if (newDynamicLatencyProbeInterval > 0)
                {
                    // Calculate the exponential backoff once so we don't have to take logarithms at runtime
                    for (long i = 1; (i * dynamicLatencyProbeInterval) < MAX_PROBE_INTERVAL_MS; i *= 2)
                    {
                        probeTimes.add(i * dynamicLatencyProbeInterval);
                    }
                    latencyProbeScheduler = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(latencyProbe, dynamicLatencyProbeInterval, dynamicLatencyProbeInterval, TimeUnit.MILLISECONDS);
                }
            }
        }

        dynamicBadnessThreshold = newDynamicBadnessThreshold;
    }

    private void registerMBean()
    {
        MBeanWrapper.instance.registerMBean(this, mbeanName);
    }

    public void close()
    {
        if (updateScheduler != null)
            updateScheduler.cancel(false);
        if (latencyProbeScheduler != null)
            latencyProbeScheduler.cancel(false);

        MBeanWrapper.instance.unregisterMBean(mbeanName);
    }

    /**
     * Determines if latency probes need to be sent, and potentially sends a single latency probe per invocation
     */
    protected void maybeSendLatencyProbe()
    {
        if (!StorageService.instance.isGossipActive())
            return;

        currentProbePosition = latencyProbeNeeded(samples, latencyProbeSequence, currentProbePosition);

        if (currentProbePosition < latencyProbeSequence.size())
        {
            try
            {
                InetAddressAndPort peer = latencyProbeSequence.get(currentProbePosition);
                sendPingMessageToPeer(peer);
            }
            catch (IndexOutOfBoundsException ignored) {}
        }
    }

    /**
     * This method (unfortunately) mutates a lot of state so that it doesn't create any garbage and only iterates the
     * sample map a single time . In particular on every call we:
     *  - increment every sample's intervalsSinceLastMeasure
     *
     * When probes should be generated we also potentially:
     *  - reset sample's recentlyRequested that have reached the "CONSTANT" phase of probing (10 minutes by default)
     *  - add any InetAddressAndPort's that need probing to the provided endpointsToProbe
     *  - shuffle the endpointsToProbe
     *
     * If there are probes to be sent, this method short circuits all generation of probes and just returns the
     * passed probePosition plus one.
     * @return The position of the passed endpointsToProbe that should be probed.
     */
    @VisibleForTesting
    int latencyProbeNeeded(Map<InetAddressAndPort, AnnotatedMeasurement> samples,
                                     List<InetAddressAndPort> endpointsToProbe, int probePosition) {
        boolean shouldGenerateProbes = (probePosition >= endpointsToProbe.size());

        if (shouldGenerateProbes)
        {
            endpointsToProbe.clear();
            samples.keySet().retainAll(Gossiper.instance.getLiveMembers());
        }

        // We have to increment intervalsSinceLastMeasure regardless of if we generate probes
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry: samples.entrySet())
        {
            AnnotatedMeasurement measurement = entry.getValue();
            long intervalsSinceLastMeasure = measurement.intervalsSinceLastMeasure.getAndIncrement();

            // We never probe instances that have not been requested for ranking
            if (!measurement.recentlyRequested)
                continue;

            if (shouldGenerateProbes)
            {
                ProbeType type = evaluateEndpointForProbe(intervalsSinceLastMeasure);
                if (type != ProbeType.NO)
                    endpointsToProbe.add(entry.getKey());

                // If we've reached the "constant" phase of probing then we haven't requested this node recently
                if (type == ProbeType.CONSTANT)
                    measurement.recentlyRequested = false;
            }
        }

        if (shouldGenerateProbes)
        {
            Collections.shuffle(endpointsToProbe, ThreadLocalRandom.current());
            return 0;
        }

        return probePosition + 1;
    }

    /**
     * Implemented limited exponential backoff such that we probe exponentially less frequently until
     * MAX_PROBE_INTERVAL_MS (default 10 minutes). We don't even bother probing before MIN_PROBE_INTERVAL_MS (default
     * 10 seconds).
     * @param intervals The number of probe intervals a node has not been probed in. For example if there have been
     *                  four intervals without probing and each interval is 1 second, we know that we haven't probed
     *                  that host in 4 minutes.
     * @return A ProbeType representing if this node should be probed due to the CONSTANT phase (meaning that callers
     * should not consider this node particularly recent), due to the EXPonential phase, or there should be NO probe.
     */
    @VisibleForTesting
    ProbeType evaluateEndpointForProbe(long intervals)
    {
        long msPerInterval = dynamicLatencyProbeInterval;
        long millisecondsWithoutMeasure = intervals * msPerInterval;

        // Hosts that we have not involved in a recent query
        if (millisecondsWithoutMeasure > MIN_PROBE_INTERVAL_MS)
        {
            // Once we pass MAX_PROBE_INTERVAL_MS we do constant probing at that interval
            if (millisecondsWithoutMeasure > MAX_PROBE_INTERVAL_MS &&
                (millisecondsWithoutMeasure % MAX_PROBE_INTERVAL_MS < msPerInterval))
            {
                return ProbeType.CONSTANT;
            }
            // We are exponentially backing off
            else if(probeTimes.contains(millisecondsWithoutMeasure))
            {
                return ProbeType.EXP;
            }
        }
        return ProbeType.NO;
    }

    private void sendPingMessageToPeer(InetAddressAndPort to)
    {
        logger.trace("Sending a small and large PingMessage to {}", to);

        IAsyncCallback latencyProbeHandler = new IAsyncCallback()
        {
            public boolean isLatencyForSnitch() { return true; }
            public LatencyMeasurementType latencyMeasurementType() { return LatencyMeasurementType.PROBE; }
            public void response(MessageIn msg) { }
        };

        MessageOut<PingMessage> smallChannelMessageOut = new MessageOut<>(PING, PingMessage.smallChannelMessage,
                                                                          PingMessage.serializer, SMALL_MESSAGE);
        MessageOut<PingMessage> largeChannelMessageOut = new MessageOut<>(PING, PingMessage.largeChannelMessage,
                                                                          PingMessage.serializer, LARGE_MESSAGE);

        MessagingService.instance().sendRR(smallChannelMessageOut, to, latencyProbeHandler);
        MessagingService.instance().sendRR(largeChannelMessageOut, to, latencyProbeHandler);
    }

    @Override
    public void gossiperStarting()
    {
        subsnitch.gossiperStarting();
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        return subsnitch.getRack(endpoint);
    }

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        return subsnitch.getDatacenter(endpoint);
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C unsortedAddresses)
    {
        assert address.equals(FBUtilities.getBroadcastAddressAndPort()); // we only know about ourself

        // Unlike the rest of this method which looks at scores, here we have to indicate to the
        // sampling side that we actually requested these nodes. This informs the offline latency probing
        for (Replica replica: unsortedAddresses)
        {
            AnnotatedMeasurement measurement = samples.get(replica.endpoint());
            if (measurement != null && !measurement.recentlyRequested)
                measurement.recentlyRequested = true;
        }

        return dynamicBadnessThreshold == 0
                ? sortedByProximityWithScore(address, unsortedAddresses)
                : sortedByProximityWithBadness(address, unsortedAddresses);
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithScore(final InetAddressAndPort address, C unsortedAddresses)
    {
        // Scores can change concurrently from a call to this method. But Collections.sort() expects
        // its comparator to be "stable", that is 2 endpoint should compare the same way for the duration
        // of the sort() call. As we copy the scores map on write, it is thus enough to alias the current
        // version of it during this call.
        Map<InetAddressAndPort, Double> scores = this.scores;
        return unsortedAddresses.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithBadness(final InetAddressAndPort address, C replicas)
    {
        if (replicas.size() < 2)
            return replicas;

        // TODO: avoid copy
        replicas = subsnitch.sortedByProximity(address, replicas);
        // Make sure the score don't change in the middle of the loop below
        // (which wouldn't really matter here but its cleaner that way).
        Map<InetAddressAndPort, Double> scores = this.scores;

        ArrayList<Double> subsnitchOrderedScores = new ArrayList<>(replicas.size());
        for (Replica replica : replicas)
        {
            Double score = scores.get(replica.endpoint());
            if (score == null)
                score = 0.0;
            subsnitchOrderedScores.add(score);
        }

        // Sort the scores and then compare them (positionally) to the scores in the subsnitch order.
        // If any of the subsnitch-ordered scores exceed the optimal/sorted score by dynamicBadnessThreshold, use
        // the score-sorted ordering instead of the subsnitch ordering.
        ArrayList<Double> sortedScores = new ArrayList<>(subsnitchOrderedScores);
        Collections.sort(sortedScores);

        Iterator<Double> sortedScoreIterator = sortedScores.iterator();
        for (Double subsnitchScore : subsnitchOrderedScores)
        {
            if (subsnitchScore > (sortedScoreIterator.next() * (1.0 + dynamicBadnessThreshold)))
            {
                return sortedByProximityWithScore(address, replicas);
            }
        }

        return replicas;
    }

    // Compare endpoints given an immutable snapshot of the scores
    private int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2, Map<InetAddressAndPort, Double> scores)
    {
        Double scored1 = scores.get(a1.endpoint());
        Double scored2 = scores.get(a2.endpoint());

        if (scored1 == null)
        {
            scored1 = 0.0;
        }

        if (scored2 == null)
        {
            scored2 = 0.0;
        }

        if (scored1.equals(scored2))
            return subsnitch.compareEndpoints(target, a1, a2);
        if (scored1 < scored2)
            return -1;
        else
            return 1;
    }

    public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
    {
        // That function is fundamentally unsafe because the scores can change at any time and so the result of that
        // method is not stable for identical arguments. This is why we don't rely on super.sortByProximity() in
        // sortByProximityWithScore().
        throw new UnsupportedOperationException("You shouldn't wrap the DynamicEndpointSnitch (within itself or otherwise)");
    }

    public Map<InetAddress, Double> getScores()
    {
        return scores.entrySet().stream().collect(Collectors.toMap(address -> address.getKey().address, Map.Entry::getValue));
    }

    public Map<InetAddressAndPort, Double> getScoresWithPort()
    {
        return scores;
    }

    @VisibleForTesting
    Map<InetAddressAndPort, AnnotatedMeasurement> getMeasurementsWithPort()
    {
        return samples;
    }

    public int getUpdateInterval()
    {
        return dynamicUpdateInterval;
    }
    public int getResetInterval()
    {
        return 0;
    }
    public int getLatencyProbeInterval()
    {
        return dynamicLatencyProbeInterval;
    }
    public double getBadnessThreshold()
    {
        return dynamicBadnessThreshold;
    }

    public String getSubsnitchClassName()
    {
        return subsnitch.getClass().getName();
    }

    /**
     * Dump the underlying metrics backing the DES's decisions for a given host. Since the subclasses
     * might have different sampling techniques they need to implement this.
     */
    public List<Double> dumpTimings(String hostname) throws UnknownHostException
    {
        InetAddressAndPort host = InetAddressAndPort.getByName(hostname);
        ArrayList<Double> timings = new ArrayList<>();
        AnnotatedMeasurement sample = samples.get(host);
        if (sample != null)
        {
            for (double measurement: sample.measurement.measurements())
                timings.add(measurement);
        }
        return timings;
    }

    public void setSeverity(double severity)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, StorageService.instance.valueFactory.severity(severity));
    }

    protected double getSeverity(InetAddressAndPort endpoint)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null)
            return 0.0;

        VersionedValue event = state.getApplicationState(ApplicationState.SEVERITY);
        if (event == null)
            return 0.0;

        return Double.parseDouble(event.value);
    }

    public double getSeverity()
    {
        return getSeverity(FBUtilities.getBroadcastAddressAndPort());
    }

    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
    {
        if (!subsnitch.isWorthMergingForRangeQuery(merged, l1, l2))
            return false;

        // skip checking scores in the single-node case
        if (l1.size() == 1 && l2.size() == 1 && l1.get(0).equals(l2.get(0)))
            return true;

        // Make sure we return the subsnitch decision (i.e true if we're here) if we lack too much scores
        double maxMerged = maxScore(merged);
        double maxL1 = maxScore(l1);
        double maxL2 = maxScore(l2);
        if (maxMerged < 0 || maxL1 < 0 || maxL2 < 0)
            return true;

        return maxMerged <= (maxL1 + maxL2) * RANGE_MERGING_PREFERENCE;
    }

    // Return the max score for the endpoint in the provided list, or -1.0 if no node have a score.
    private double maxScore(ReplicaCollection<?> endpoints)
    {
        double maxScore = -1.0;
        for (Replica replica : endpoints)
        {
            Double score = scores.get(replica.endpoint());
            if (score == null)
                continue;

            if (score > maxScore)
                maxScore = score;
        }
        return maxScore;
    }

    public boolean validate(Set<String> datacenters, Set<String> racks)
    {
        return subsnitch.validate(datacenters, racks);
    }
}
