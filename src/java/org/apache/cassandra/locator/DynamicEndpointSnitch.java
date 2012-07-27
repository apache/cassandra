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
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import com.yammer.metrics.stats.ExponentiallyDecayingSample;

/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 */
public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static final double ALPHA = 0.75; // set to 0.75 to make EDS more biased to towards the newer values
    private static final int WINDOW_SIZE = 100;

    private int UPDATE_INTERVAL_IN_MS = DatabaseDescriptor.getDynamicUpdateInterval();
    private int RESET_INTERVAL_IN_MS = DatabaseDescriptor.getDynamicResetInterval();
    private double BADNESS_THRESHOLD = DatabaseDescriptor.getDynamicBadnessThreshold();
    private String mbeanName;
    private boolean registered = false;

    private final ConcurrentHashMap<InetAddress, Double> scores = new ConcurrentHashMap<InetAddress, Double>();
    private final ConcurrentHashMap<InetAddress, Long> lastReceived = new ConcurrentHashMap<InetAddress, Long>();
    private final ConcurrentHashMap<InetAddress, ExponentiallyDecayingSample> samples = new ConcurrentHashMap<InetAddress, ExponentiallyDecayingSample>();

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
        StorageService.scheduledTasks.scheduleWithFixedDelay(update, UPDATE_INTERVAL_IN_MS, UPDATE_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
        StorageService.scheduledTasks.scheduleWithFixedDelay(reset, RESET_INTERVAL_IN_MS, RESET_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
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
        super.sortByProximity(address, addresses);
    }

    private void sortByProximityWithBadness(final InetAddress address, List<InetAddress> addresses)
    {
        if (addresses.size() < 2)
            return;
        subsnitch.sortByProximity(address, addresses);
        Double first = scores.get(addresses.get(0));
        if (first == null)
            return;
        for (InetAddress addr : addresses)
        {
            Double next = scores.get(addr);
            if (next == null)
                return;
            if ((first - next) / first > BADNESS_THRESHOLD)
            {
                sortByProximityWithScore(address, addresses);
                return;
            }
        }
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
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

    public void receiveTiming(InetAddress host, long latency) // this is cheap
    {
        lastReceived.put(host, System.currentTimeMillis());

        ExponentiallyDecayingSample sample = samples.get(host);
        if (sample == null)
        {
            ExponentiallyDecayingSample maybeNewSample = new ExponentiallyDecayingSample(WINDOW_SIZE, ALPHA);
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
        long maxPenalty = 1;
        HashMap<InetAddress, Long> penalties = new HashMap<InetAddress, Long>();
        for (Map.Entry<InetAddress, ExponentiallyDecayingSample> entry : samples.entrySet())
        {
            double mean = entry.getValue().getSnapshot().getMedian();
            if (mean > maxLatency)
                maxLatency = mean;
            long timePenalty = lastReceived.containsKey(entry.getKey()) ? lastReceived.get(entry.getKey()) : System.currentTimeMillis();
            timePenalty = System.currentTimeMillis() - timePenalty;
            timePenalty = timePenalty > UPDATE_INTERVAL_IN_MS ? UPDATE_INTERVAL_IN_MS : timePenalty;
            penalties.put(entry.getKey(), timePenalty);
            if (timePenalty > maxPenalty)
                maxPenalty = timePenalty;
        }
        for (Map.Entry<InetAddress, ExponentiallyDecayingSample> entry: samples.entrySet())
        {
            double score = entry.getValue().getSnapshot().getMedian() / maxLatency;
            if (penalties.containsKey(entry.getKey()))
                score += penalties.get(entry.getKey()) / ((double) maxPenalty);
            else
                score += 1; // maxPenalty / maxPenalty
            score += StorageService.instance.getSeverity(entry.getKey());
            scores.put(entry.getKey(), score);            
        }
    }


    private void reset()
    {
        for (ExponentiallyDecayingSample sample : samples.values())
            sample.clear();
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
        ExponentiallyDecayingSample sample = samples.get(host);
        if (sample != null)
        {
            for (double time: sample.getSnapshot().getValues())
                timings.add(time);
        }
        return timings;
    }

    public void setSeverity(double severity)
    {
        StorageService.instance.reportSeverity(severity);
    }

    public double getSeverity()
    {
        return StorageService.instance.getSeverity(FBUtilities.getBroadcastAddress());
    }

}
