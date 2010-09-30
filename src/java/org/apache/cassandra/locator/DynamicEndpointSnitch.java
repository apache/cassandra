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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AbstractStatsDeque;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 */
public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static int UPDATES_PER_INTERVAL = 10000;
    private static int UPDATE_INTERVAL_IN_MS = 100;
    private static int RESET_INTERVAL_IN_MS = 60000 * 10;
    private static int WINDOW_SIZE = 100;
    private boolean registered = false;

    private ConcurrentHashMap<InetAddress, Double> scores = new ConcurrentHashMap();
    private ConcurrentHashMap<InetAddress, AdaptiveLatencyTracker> windows = new ConcurrentHashMap();
    private AtomicInteger intervalupdates = new AtomicInteger(0);
    public IEndpointSnitch subsnitch;

    public DynamicEndpointSnitch(IEndpointSnitch snitch)
    {
        subsnitch = snitch;
        TimerTask update = new TimerTask()
        {
            public void run()
            {
                updateScores();
            }
        };
        TimerTask reset = new TimerTask()
        {
            public void run()
            {
                // we do this so that a host considered bad has a chance to recover, otherwise would we never try
                // to read from it, which would cause its score to never change
                reset();
            }
        };
        Timer timer = new Timer("DynamicEndpointSnitch");
        timer.schedule(update, UPDATE_INTERVAL_IN_MS, UPDATE_INTERVAL_IN_MS);
        timer.schedule(reset, RESET_INTERVAL_IN_MS, RESET_INTERVAL_IN_MS);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=DynamicEndpointSnitch"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public ArrayList<InetAddress> getCachedEndpoints(Token t)
    {
        return subsnitch.getCachedEndpoints(t);
    }

    public void cacheEndpoint(Token t, ArrayList<InetAddress> addr)
    {
        subsnitch.cacheEndpoint(t, addr);
    }

    public void clearEndpointCache()
    {
        subsnitch.clearEndpointCache();
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

    public List<InetAddress> sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        assert address.equals(FBUtilities.getLocalAddress()); // we only know about ourself
        Collections.sort(addresses, new Comparator<InetAddress>()
        {
            public int compare(InetAddress a1, InetAddress a2)
            {
                return compareEndpoints(address, a1, a2);
            }
        });
        return addresses;
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
    {
        Double scored1 = scores.get(a1);
        Double scored2 = scores.get(a2);

        if (scored1 == null || scored2 == null || scored1.equals(scored2))
            return subsnitch.compareEndpoints(target, a1, a2);
        if (scored1 < scored2)
            return -1;
        else
            return 1;
    }

    public void receiveTiming(InetAddress host, Double latency) // this is cheap
    {
        if (intervalupdates.intValue() >= UPDATES_PER_INTERVAL)
            return;
        AdaptiveLatencyTracker tracker = windows.get(host);
        if (tracker == null)
        {
            AdaptiveLatencyTracker alt = new AdaptiveLatencyTracker(WINDOW_SIZE);
            tracker = windows.putIfAbsent(host, alt);
            if (tracker == null)
                tracker = alt;
        }
        tracker.add(latency);
        intervalupdates.getAndIncrement();
    }

    private void updateScores() // this is expensive
    {
        if (!registered)
        {
       	    ILatencyPublisher handler = (ILatencyPublisher)MessagingService.instance.getVerbHandler(StorageService.Verb.READ_RESPONSE);
            if (handler != null)
            {
                handler.register(this);
                registered = true;
            }

        }
        for (Map.Entry<InetAddress, AdaptiveLatencyTracker> entry: windows.entrySet())
        {
            scores.put(entry.getKey(), entry.getValue().score());
        }
        intervalupdates.set(0);
    }

    private void reset()
    {
        for (AdaptiveLatencyTracker tracker : windows.values())
        {
            tracker.clear();
        }
    }

    public Map<InetAddress, Double> getScores()
    {
        return scores;
    }
}

/** a threadsafe version of BoundedStatsDeque+ArrivalWindow with modification for arbitrary times **/
class AdaptiveLatencyTracker extends AbstractStatsDeque
{
    private LinkedBlockingDeque latencies;
    private final int size;                                   
    private static double SENTINEL_COMPARE = 0.0001; // arbitrary; as long as it is the same across hosts it doesn't matter

    AdaptiveLatencyTracker(int size)
    {
        this.size = size;
        latencies = new LinkedBlockingDeque(size);
    }

    public void add(double i)
    {
        if (!latencies.offer(i))
        {
            latencies.remove();
            latencies.offer(i);
        }
    }

    public void clear()
    {
        latencies.clear();
    }

    public Iterator<Double> iterator()
    {
        return latencies.iterator();
    }

    public int size()
    {
        return latencies.size();
    }

    double p(double t)
    {
        double mean = mean();
        double exponent = (-1) * (t) / mean;
        return 1 - Math.pow( Math.E, exponent);
    }

    double score()
    {
        double log = 0d;
        if ( latencies.size() > 0 )
        {
            double probability = p(SENTINEL_COMPARE);
            log = (-1) * Math.log10( probability );
        }
        return log;
    }

}
