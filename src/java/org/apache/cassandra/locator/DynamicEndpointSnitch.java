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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.ExponentiallyDecayingReservoir;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.LatencySubscribers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORE_DYNAMIC_SNITCH_SEVERITY;

/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 */
public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements LatencySubscribers.Subscriber, DynamicEndpointSnitchMBean
{
    private static final boolean USE_SEVERITY = !IGNORE_DYNAMIC_SNITCH_SEVERITY.getBoolean();

    private static final double ALPHA = 0.75; // set to 0.75 to make EDS more biased to towards the newer values
    private static final int WINDOW_SIZE = 100;

    private volatile int dynamicUpdateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
    private volatile int dynamicResetInterval = DatabaseDescriptor.getDynamicResetInterval();
    private volatile double dynamicBadnessThreshold = DatabaseDescriptor.getDynamicBadnessThreshold();

    // the score for a merged set of endpoints must be this much worse than the score for separate endpoints to
    // warrant not merging two ranges into a single range
    private static final double RANGE_MERGING_PREFERENCE = 1.5;

    private String mbeanName;
    private boolean registered = false;

    private volatile HashMap<InetAddressAndPort, Double> scores = new HashMap<>();
    private final ConcurrentHashMap<InetAddressAndPort, ExponentiallyDecayingReservoir> samples = new ConcurrentHashMap<>();

    public final IEndpointSnitch subsnitch;

    private volatile ScheduledFuture<?> updateSchedular;
    private volatile ScheduledFuture<?> resetSchedular;

    private final Runnable update;
    private final Runnable reset;

    public DynamicEndpointSnitch(IEndpointSnitch snitch)
    {
        this(snitch, null);
    }

    public DynamicEndpointSnitch(IEndpointSnitch snitch, String instance)
    {
        mbeanName = "org.apache.cassandra.db:type=DynamicEndpointSnitch";
        if (instance != null)
            mbeanName += ",instance=" + instance;
        subsnitch = snitch;
        update = new Runnable()
        {
            public void run()
            {
                updateScores();
            }
        };
        reset = new Runnable()
        {
            public void run()
            {
                // we do this so that a host considered bad has a chance to recover, otherwise would we never try
                // to read from it, which would cause its score to never change
                reset();
            }
        };

        if (DatabaseDescriptor.isDaemonInitialized())
        {
            updateSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(update, dynamicUpdateInterval, dynamicUpdateInterval, TimeUnit.MILLISECONDS);
            resetSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(reset, dynamicResetInterval, dynamicResetInterval, TimeUnit.MILLISECONDS);
            registerMBean();
        }
    }

    /**
     * Update configuration from {@link DatabaseDescriptor} and estart the update-scheduler and reset-scheduler tasks
     * if the configured rates for these tasks have changed.
     */
    public void applyConfigChanges()
    {
        if (dynamicUpdateInterval != DatabaseDescriptor.getDynamicUpdateInterval())
        {
            dynamicUpdateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                updateSchedular.cancel(false);
                updateSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(update, dynamicUpdateInterval, dynamicUpdateInterval, TimeUnit.MILLISECONDS);
            }
        }

        if (dynamicResetInterval != DatabaseDescriptor.getDynamicResetInterval())
        {
            dynamicResetInterval = DatabaseDescriptor.getDynamicResetInterval();
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                resetSchedular.cancel(false);
                resetSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(reset, dynamicResetInterval, dynamicResetInterval, TimeUnit.MILLISECONDS);
            }
        }

        dynamicBadnessThreshold = DatabaseDescriptor.getDynamicBadnessThreshold();
    }

    private void registerMBean()
    {
        MBeanWrapper.instance.registerMBean(this, mbeanName);
    }

    public void close()
    {
        updateSchedular.cancel(false);
        resetSchedular.cancel(false);

        MBeanWrapper.instance.unregisterMBean(mbeanName);
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
        final HashMap<InetAddressAndPort, Double> scores = this.scores;
        return unsortedAddresses.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithBadness(final InetAddressAndPort address, C replicas)
    {
        if (replicas.size() < 2)
            return replicas;

        // TODO: avoid copy
        replicas = subsnitch.sortedByProximity(address, replicas);
        HashMap<InetAddressAndPort, Double> scores = this.scores; // Make sure the score don't change in the middle of the loop below
                                                           // (which wouldn't really matter here but its cleaner that way).
        ArrayList<Double> subsnitchOrderedScores = new ArrayList<>(replicas.size());
        for (Replica replica : replicas)
        {
            Double score = scores.get(replica.endpoint());
            if (score == null)
                score = defaultStore(replica.endpoint());
            subsnitchOrderedScores.add(score);
        }

        // Sort the scores and then compare them (positionally) to the scores in the subsnitch order.
        // If any of the subsnitch-ordered scores exceed the optimal/sorted score by dynamicBadnessThreshold, use
        // the score-sorted ordering instead of the subsnitch ordering.
        ArrayList<Double> sortedScores = new ArrayList<>(subsnitchOrderedScores);
        Collections.sort(sortedScores);

        // only calculate this once b/c its volatile and shouldn't be modified during the loop either
        double badnessThreshold = 1.0 + dynamicBadnessThreshold;
        Iterator<Double> sortedScoreIterator = sortedScores.iterator();
        for (Double subsnitchScore : subsnitchOrderedScores)
        {
            if (subsnitchScore > (sortedScoreIterator.next() * badnessThreshold))
            {
                return sortedByProximityWithScore(address, replicas);
            }
        }

        return replicas;
    }

    private static double defaultStore(InetAddressAndPort target)
    {
        return USE_SEVERITY ? getSeverity(target) : 0.0;
    }

    // Compare endpoints given an immutable snapshot of the scores
    private int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2, Map<InetAddressAndPort, Double> scores)
    {
        Double scored1 = scores.get(a1.endpoint());
        Double scored2 = scores.get(a2.endpoint());
        
        if (scored1 == null)
        {
            scored1 = defaultStore(a1.endpoint());
        }

        if (scored2 == null)
        {
            scored2 = defaultStore(a2.endpoint());
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

    public void receiveTiming(InetAddressAndPort host, long latency, TimeUnit unit) // this is cheap
    {
        ExponentiallyDecayingReservoir sample = samples.get(host);
        if (sample == null)
        {
            ExponentiallyDecayingReservoir maybeNewSample = new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);
            sample = samples.putIfAbsent(host, maybeNewSample);
            if (sample == null)
                sample = maybeNewSample;
        }
        sample.update(unit.toMillis(latency));
    }

    @VisibleForTesting
    public void updateScores() // this is expensive
    {
        if (!StorageService.instance.isInitialized())
            return;
        if (!registered)
        {
            if (MessagingService.instance() != null)
            {
                MessagingService.instance().latencySubscribers.subscribe(this);
                registered = true;
            }

        }
        double maxLatency = 1;

        Map<InetAddressAndPort, Snapshot> snapshots = new HashMap<>(samples.size());
        for (Map.Entry<InetAddressAndPort, ExponentiallyDecayingReservoir> entry : samples.entrySet())
        {
            snapshots.put(entry.getKey(), entry.getValue().getSnapshot());
        }

        // We're going to weight the latency for each host against the worst one we see, to
        // arrive at sort of a 'badness percentage' for them. First, find the worst for each:
        HashMap<InetAddressAndPort, Double> newScores = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, Snapshot> entry : snapshots.entrySet())
        {
            double mean = entry.getValue().getMedian();
            if (mean > maxLatency)
                maxLatency = mean;
        }
        // now make another pass to do the weighting based on the maximums we found before
        for (Map.Entry<InetAddressAndPort, Snapshot> entry : snapshots.entrySet())
        {
            double score = entry.getValue().getMedian() / maxLatency;
            // finally, add the severity without any weighting, since hosts scale this relative to their own load and the size of the task causing the severity.
            // "Severity" is basically a measure of compaction activity (CASSANDRA-3722).
            if (USE_SEVERITY)
                score += getSeverity(entry.getKey());
            // lowest score (least amount of badness) wins.
            newScores.put(entry.getKey(), score);
        }
        scores = newScores;
    }

    private void reset()
    {
       samples.clear();
    }

    public Map<InetAddress, Double> getScores()
    {
        return scores.entrySet().stream().collect(Collectors.toMap(address -> address.getKey().getAddress(), Map.Entry::getValue));
    }

    public Map<String, Double> getScoresWithPort()
    {
        return scores.entrySet().stream().collect(Collectors.toMap(address -> address.getKey().toString(true), Map.Entry::getValue));
    }

    public int getUpdateInterval()
    {
        return dynamicUpdateInterval;
    }
    public int getResetInterval()
    {
        return dynamicResetInterval;
    }
    public double getBadnessThreshold()
    {
        return dynamicBadnessThreshold;
    }

    public String getSubsnitchClassName()
    {
        return subsnitch.getClass().getName();
    }

    public List<Double> dumpTimings(String hostname) throws UnknownHostException
    {
        InetAddressAndPort host = InetAddressAndPort.getByName(hostname);
        ArrayList<Double> timings = new ArrayList<Double>();
        ExponentiallyDecayingReservoir sample = samples.get(host);
        if (sample != null)
        {
            for (double time: sample.getSnapshot().getValues())
                timings.add(time);
        }
        return timings;
    }

    @Override
    public void setSeverity(double severity)
    {
        addSeverity(severity);
    }

    public static void addSeverity(double severity)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, StorageService.instance.valueFactory.severity(severity));
    }

    @VisibleForTesting
    public static double getSeverity(InetAddressAndPort endpoint)
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
