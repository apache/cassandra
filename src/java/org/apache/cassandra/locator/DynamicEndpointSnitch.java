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

package org.apache.cassandra.locator;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 */
public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static final boolean USE_SEVERITY = !Boolean.getBoolean("cassandra.ignore_dynamic_snitch_severity");

    private static final double ALPHA = 0.75; // set to 0.75 to make EDS more biased to towards the newer values
    private static final int WINDOW_SIZE = 100;

    private final int UPDATE_INTERVAL_IN_MS = DatabaseDescriptor.getDynamicUpdateInterval();
    private final int RESET_INTERVAL_IN_MS = DatabaseDescriptor.getDynamicResetInterval();
    private final double BADNESS_THRESHOLD = DatabaseDescriptor.getDynamicBadnessThreshold();

    // the score for a merged set of endpoints must be this much worse than the score for separate endpoints to
    // warrant not merging two ranges into a single range
    private double RANGE_MERGING_PREFERENCE = 1.5;

    private String mbeanName;
    private boolean registered = false;

    private volatile HashMap<InetAddress, Double> scores = new HashMap<>();
    private final ConcurrentHashMap<InetAddress, ExponentiallyDecayingReservoir> samples = new ConcurrentHashMap<>();

    public final IEndpointSnitch subsnitch;

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
        Runnable update = new Runnable()
        {
            public void run()
            {
                updateScores();
            }
        };
        Runnable reset = new Runnable()
        {
            public void run()
            {
                // we do this so that a host considered bad has a chance to recover, otherwise would we never try
                // to read from it, which would cause its score to never change
                reset();
            }
        };
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(update, UPDATE_INTERVAL_IN_MS, UPDATE_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(reset, RESET_INTERVAL_IN_MS, RESET_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
        registerMBean();
   }

    private void registerMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void unregisterMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.unregisterMBean(new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void gossiperStarting()
    {
        subsnitch.gossiperStarting();
    }

    public String getRack(InetAddress endpoint)
    {
        return subsnitch.getRack(endpoint);
    }

    public String getDatacenter(InetAddress endpoint)
    {
        return subsnitch.getDatacenter(endpoint);
    }

    public List<InetAddress> getSortedListByProximity(final InetAddress address, Collection<InetAddress> addresses)
    {
        List<InetAddress> list = new ArrayList<InetAddress>(addresses);
        sortByProximity(address, list);
        return list;
    }

    @Override
    public void sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        assert address.equals(FBUtilities.getBroadcastAddress()); // we only know about ourself
        if (BADNESS_THRESHOLD == 0)
        {
            sortByProximityWithScore(address, addresses);
        }
        else
        {
            sortByProximityWithBadness(address, addresses);
        }
    }

    private void sortByProximityWithScore(final InetAddress address, List<InetAddress> addresses)
    {
        // Scores can change concurrently from a call to this method. But Collections.sort() expects
        // its comparator to be "stable", that is 2 endpoint should compare the same way for the duration
        // of the sort() call. As we copy the scores map on write, it is thus enough to alias the current
        // version of it during this call.
        final HashMap<InetAddress, Double> scores = this.scores;
        Collections.sort(addresses, new Comparator<InetAddress>()
        {
            public int compare(InetAddress a1, InetAddress a2)
            {
                return compareEndpoints(address, a1, a2, scores);
            }
        });
    }

    private void sortByProximityWithBadness(final InetAddress address, List<InetAddress> addresses)
    {
        if (addresses.size() < 2)
            return;

        subsnitch.sortByProximity(address, addresses);
        HashMap<InetAddress, Double> scores = this.scores; // Make sure the score don't change in the middle of the loop below
                                                           // (which wouldn't really matter here but its cleaner that way).
        ArrayList<Double> subsnitchOrderedScores = new ArrayList<>(addresses.size());
        for (InetAddress inet : addresses)
        {
            Double score = scores.get(inet);
            if (score == null)
                return;
            subsnitchOrderedScores.add(score);
        }

        // Sort the scores and then compare them (positionally) to the scores in the subsnitch order.
        // If any of the subsnitch-ordered scores exceed the optimal/sorted score by BADNESS_THRESHOLD, use
        // the score-sorted ordering instead of the subsnitch ordering.
        ArrayList<Double> sortedScores = new ArrayList<>(subsnitchOrderedScores);
        Collections.sort(sortedScores);

        Iterator<Double> sortedScoreIterator = sortedScores.iterator();
        for (Double subsnitchScore : subsnitchOrderedScores)
        {
            if (subsnitchScore > (sortedScoreIterator.next() * (1.0 + BADNESS_THRESHOLD)))
            {
                sortByProximityWithScore(address, addresses);
                return;
            }
        }
    }

    // Compare endpoints given an immutable snapshot of the scores
    private int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2, Map<InetAddress, Double> scores)
    {
        Double scored1 = scores.get(a1);
        Double scored2 = scores.get(a2);
        
        if (scored1 == null)
        {
            scored1 = 0.0;
            receiveTiming(a1, 0);
        }

        if (scored2 == null)
        {
            scored2 = 0.0;
            receiveTiming(a2, 0);
        }

        if (scored1.equals(scored2))
            return subsnitch.compareEndpoints(target, a1, a2);
        if (scored1 < scored2)
            return -1;
        else
            return 1;
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
    {
        // That function is fundamentally unsafe because the scores can change at any time and so the result of that
        // method is not stable for identical arguments. This is why we don't rely on super.sortByProximity() in
        // sortByProximityWithScore().
        throw new UnsupportedOperationException("You shouldn't wrap the DynamicEndpointSnitch (within itself or otherwise)");
    }

    public void receiveTiming(InetAddress host, long latency) // this is cheap
    {
        ExponentiallyDecayingReservoir sample = samples.get(host);
        if (sample == null)
        {
            ExponentiallyDecayingReservoir maybeNewSample = new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);
            sample = samples.putIfAbsent(host, maybeNewSample);
            if (sample == null)
                sample = maybeNewSample;
        }
        sample.update(latency);
    }

    private void updateScores() // this is expensive
    {
        if (!StorageService.instance.isInitialized()) 
            return;
        if (!registered)
        {
            if (MessagingService.instance() != null)
            {
                MessagingService.instance().register(this);
                registered = true;
            }

        }
        double maxLatency = 1;
        // We're going to weight the latency for each host against the worst one we see, to
        // arrive at sort of a 'badness percentage' for them. First, find the worst for each:
        HashMap<InetAddress, Double> newScores = new HashMap<>();
        for (Map.Entry<InetAddress, ExponentiallyDecayingReservoir> entry : samples.entrySet())
        {
            double mean = entry.getValue().getSnapshot().getMedian();
            if (mean > maxLatency)
                maxLatency = mean;
        }
        // now make another pass to do the weighting based on the maximums we found before
        for (Map.Entry<InetAddress, ExponentiallyDecayingReservoir> entry: samples.entrySet())
        {
            double score = entry.getValue().getSnapshot().getMedian() / maxLatency;
            // finally, add the severity without any weighting, since hosts scale this relative to their own load and the size of the task causing the severity.
            // "Severity" is basically a measure of compaction activity (CASSANDRA-3722).
            if (USE_SEVERITY)
                score += StorageService.instance.getSeverity(entry.getKey());
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
        return scores;
    }

    public int getUpdateInterval()
    {
        return UPDATE_INTERVAL_IN_MS;
    }
    public int getResetInterval()
    {
        return RESET_INTERVAL_IN_MS;
    }
    public double getBadnessThreshold()
    {
        return BADNESS_THRESHOLD;
    }

    public String getSubsnitchClassName()
    {
        return subsnitch.getClass().getName();
    }

    public List<Double> dumpTimings(String hostname) throws UnknownHostException
    {
        InetAddress host = InetAddress.getByName(hostname);
        ArrayList<Double> timings = new ArrayList<Double>();
        ExponentiallyDecayingReservoir sample = samples.get(host);
        if (sample != null)
        {
            for (double time: sample.getSnapshot().getValues())
                timings.add(time);
        }
        return timings;
    }

    public void setSeverity(double severity)
    {
        StorageService.instance.reportManualSeverity(severity);
    }

    public double getSeverity()
    {
        return StorageService.instance.getSeverity(FBUtilities.getBroadcastAddress());
    }

    public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
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
    private double maxScore(List<InetAddress> endpoints)
    {
        double maxScore = -1.0;
        for (InetAddress endpoint : endpoints)
        {
            Double score = scores.get(endpoint);
            if (score == null)
                continue;

            if (score > maxScore)
                maxScore = score;
        }
        return maxScore;
    }
}
