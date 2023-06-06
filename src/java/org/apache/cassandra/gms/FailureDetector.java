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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.FD_INITIAL_VALUE_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.FD_MAX_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.LINE_SEPARATOR;
import static org.apache.cassandra.config.CassandraRelevantProperties.MAX_LOCAL_PAUSE_IN_MS;
import static org.apache.cassandra.config.DatabaseDescriptor.newFailureDetector;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
public class FailureDetector implements IFailureDetector, FailureDetectorMBean
{
    private static final Logger logger = LoggerFactory.getLogger(FailureDetector.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=FailureDetector";
    private static final int SAMPLE_SIZE = 1000;
    protected static final long INITIAL_VALUE_NANOS = TimeUnit.NANOSECONDS.convert(getInitialValue(), TimeUnit.MILLISECONDS);
    private static final int DEBUG_PERCENTAGE = 80; // if the phi is larger than this percentage of the max, log a debug message
    private static final long MAX_LOCAL_PAUSE_IN_NANOS = getMaxLocalPause();
    private long lastInterpret = preciseTime.now();
    private long lastPause = 0L;

    private static long getMaxLocalPause()
    {
        long pause = MAX_LOCAL_PAUSE_IN_MS.getLong();

        if (!String.valueOf(pause).equals(MAX_LOCAL_PAUSE_IN_MS.getDefaultValue()))
            logger.warn("Overriding {} max local pause time from {}ms to {}ms",
                        MAX_LOCAL_PAUSE_IN_MS.getKey(), MAX_LOCAL_PAUSE_IN_MS.getDefaultValue(), pause);

        return pause * 1000000L;
    }

    public static final IFailureDetector instance = newFailureDetector();
    public static final Predicate<InetAddressAndPort> isEndpointAlive = instance::isAlive;
    public static final Predicate<Replica> isReplicaAlive = r -> isEndpointAlive.test(r.endpoint());

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0); // 0.434...

    private final ConcurrentHashMap<InetAddressAndPort, ArrivalWindow> arrivalSamples = new ConcurrentHashMap<>();
    private final List<IFailureDetectionEventListener> fdEvntListeners = new CopyOnWriteArrayList<>();

    public FailureDetector()
    {
        // Register this instance with JMX
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    private static long getInitialValue()
    {
        long newValue = FD_INITIAL_VALUE_MS.getLong(Gossiper.intervalInMillis * 2L);

        if (newValue != Gossiper.intervalInMillis * 2)
            logger.info("Overriding {} from {}ms to {}ms", FD_INITIAL_VALUE_MS.getKey(), Gossiper.intervalInMillis * 2, newValue);

        return newValue;
    }

    public String getAllEndpointStates()
    {
        return getAllEndpointStates(false, false);
    }

    public String getAllEndpointStatesWithResolveIp()
    {
        return getAllEndpointStates(false, true);
    }

    public String getAllEndpointStatesWithPort()
    {
        return getAllEndpointStates(true, false);
    }

    public String getAllEndpointStatesWithPortAndResolveIp()
    {
        return getAllEndpointStates(true, true);
    }

    public String getAllEndpointStates(boolean withPort)
    {
        return getAllEndpointStates(withPort, false);
    }

    public String getAllEndpointStates(boolean withPort, boolean resolveIp)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            sb.append(resolveIp ? entry.getKey().getHostName(withPort) : entry.getKey().toString(withPort)).append("\n");
            appendEndpointState(sb, entry.getValue());
        }
        return sb.toString();
    }

    public Map<String, String> getSimpleStates()
    {
        return getSimpleStates(false);
    }

    public Map<String, String> getSimpleStatesWithPort()
    {
        return getSimpleStates(true);
    }

    private Map<String, String> getSimpleStates(boolean withPort)
    {
        Map<String, String> nodesStatus = new HashMap<String, String>(Gossiper.instance.endpointStateMap.size());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (entry.getValue().isAlive())
                nodesStatus.put(entry.getKey().toString(withPort), "UP");
            else
                nodesStatus.put(entry.getKey().toString(withPort), "DOWN");
        }
        return nodesStatus;
    }

    public int getDownEndpointCount()
    {
        int count = 0;
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (!entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    public int getUpEndpointCount()
    {
        int count = 0;
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    @Override
    public TabularData getPhiValues() throws OpenDataException
    {
        return getPhiValues(false);
    }

    @Override
    public TabularData getPhiValuesWithPort() throws OpenDataException
    {
        return getPhiValues(true);
    }

    private TabularData getPhiValues(boolean withPort) throws OpenDataException
    {
        final CompositeType ct = new CompositeType("Node", "Node",
                new String[]{"Endpoint", "PHI"},
                new String[]{"IP of the endpoint", "PHI value"},
                new OpenType[]{SimpleType.STRING, SimpleType.DOUBLE});
        final TabularDataSupport results = new TabularDataSupport(new TabularType("PhiList", "PhiList", ct, new String[]{"Endpoint"}));

        for (final Map.Entry<InetAddressAndPort, ArrivalWindow> entry : arrivalSamples.entrySet())
        {
            final ArrivalWindow window = entry.getValue();
            if (window.mean() > 0)
            {
                final double phi = window.getLastReportedPhi();
                if (phi != Double.MIN_VALUE)
                {
                    // returned values are scaled by PHI_FACTOR so that the are on the same scale as PhiConvictThreshold
                    final CompositeData data = new CompositeDataSupport(ct,
                            new String[]{"Endpoint", "PHI"},
                            new Object[]{entry.getKey().toString(withPort), phi * PHI_FACTOR});
                    results.put(data);
                }
            }
        }
        return results;
    }

    public String getEndpointState(String address) throws UnknownHostException
    {
        StringBuilder sb = new StringBuilder();
        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(InetAddressAndPort.getByName(address));
        appendEndpointState(sb, endpointState);
        return sb.toString();
    }

    private void appendEndpointState(StringBuilder sb, EndpointState endpointState)
    {
        sb.append("  generation:").append(endpointState.getHeartBeatState().getGeneration()).append("\n");
        sb.append("  heartbeat:").append(endpointState.getHeartBeatState().getHeartBeatVersion()).append("\n");
        for (Map.Entry<ApplicationState, VersionedValue> state : endpointState.states())
        {
            if (state.getKey() == ApplicationState.TOKENS)
                continue;
            sb.append("  ").append(state.getKey()).append(":").append(state.getValue().version).append(":").append(state.getValue().value).append("\n");
        }
        VersionedValue tokens = endpointState.getApplicationState(ApplicationState.TOKENS);
        if (tokens != null)
        {
            sb.append("  TOKENS:").append(tokens.version).append(":<hidden>\n");
        }
        else
        {
            sb.append("  TOKENS: not present\n");
        }
    }

    /**
     * Dump the inter arrival times for examination if necessary.
     */
    public void dumpInterArrivalTimes()
    {
        Path path = null;
        try {
            path = Files.createTempFile("failuredetector-", ".dat");

            try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.APPEND)))
            {
                os.write(toString().getBytes());
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, path);
        }
    }

    public void setPhiConvictThreshold(double phi)
    {
        DatabaseDescriptor.setPhiConvictThreshold(phi);
    }

    public double getPhiConvictThreshold()
    {
        return DatabaseDescriptor.getPhiConvictThreshold();
    }

    public boolean isAlive(InetAddressAndPort ep)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return true;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
        // we could assert not-null, but having isAlive fail screws a node over so badly that
        // it's worth being defensive here so minor bugs don't cause disproportionate
        // badness.  (See CASSANDRA-1463 for an example).
        if (epState == null)
            logger.error("Unknown endpoint: " + ep, new IllegalArgumentException(""));
        return epState != null && epState.isAlive();
    }

    public void report(InetAddressAndPort ep)
    {
        long now = preciseTime.now();
        ArrivalWindow heartbeatWindow = arrivalSamples.get(ep);
        if (heartbeatWindow == null)
        {
            // avoid adding an empty ArrivalWindow to the Map
            heartbeatWindow = new ArrivalWindow(SAMPLE_SIZE);
            heartbeatWindow.add(now, ep);
            heartbeatWindow = arrivalSamples.putIfAbsent(ep, heartbeatWindow);
            if (heartbeatWindow != null)
                heartbeatWindow.add(now, ep);
        }
        else
        {
            heartbeatWindow.add(now, ep);
        }

        if (logger.isTraceEnabled() && heartbeatWindow != null)
            logger.trace("Average for {} is {}ns", ep, heartbeatWindow.mean());
    }

    public void interpret(InetAddressAndPort ep)
    {
        ArrivalWindow hbWnd = arrivalSamples.get(ep);
        if (hbWnd == null)
        {
            return;
        }
        long now = preciseTime.now();
        long diff = now - lastInterpret;
        lastInterpret = now;
        if (diff > MAX_LOCAL_PAUSE_IN_NANOS)
        {
            logger.warn("Not marking nodes down due to local pause of {}ns > {}ns", diff, MAX_LOCAL_PAUSE_IN_NANOS);
            lastPause = now;
            return;
        }
        if (preciseTime.now() - lastPause < MAX_LOCAL_PAUSE_IN_NANOS)
        {
            logger.debug("Still not marking nodes down due to local pause");
            return;
        }
        double phi = hbWnd.phi(now);
        if (logger.isTraceEnabled())
            logger.trace("PHI for {} : {}", ep, phi);

        if (PHI_FACTOR * phi > getPhiConvictThreshold())
        {
            if (logger.isTraceEnabled())
                logger.trace("Node {} phi {} > {}; intervals: {} mean: {}ns", new Object[]{ep, PHI_FACTOR * phi, getPhiConvictThreshold(), hbWnd, hbWnd.mean()});
            for (IFailureDetectionEventListener listener : fdEvntListeners)
            {
                listener.convict(ep, phi);
            }
        }
        else if (logger.isDebugEnabled() && (PHI_FACTOR * phi * DEBUG_PERCENTAGE / 100.0 > getPhiConvictThreshold()))
        {
            logger.debug("PHI for {} : {}", ep, phi);
        }
        else if (logger.isTraceEnabled())
        {
            logger.trace("PHI for {} : {}", ep, phi);
            logger.trace("mean for {} : {}ns", ep, hbWnd.mean());
        }
    }

    public void forceConviction(InetAddressAndPort ep)
    {
        logger.debug("Forcing conviction of {}", ep);
        for (IFailureDetectionEventListener listener : fdEvntListeners)
        {
            listener.convict(ep, getPhiConvictThreshold());
        }
    }

    public void remove(InetAddressAndPort ep)
    {
        arrivalSamples.remove(ep);
    }

    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners.add(listener);
    }

    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners.remove(listener);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Set<InetAddressAndPort> eps = arrivalSamples.keySet();

        sb.append("-----------------------------------------------------------------------");
        for (InetAddressAndPort ep : eps)
        {
            ArrivalWindow hWnd = arrivalSamples.get(ep);
            sb.append(ep).append(" : ");
            sb.append(hWnd);
            sb.append(LINE_SEPARATOR.getString());
        }
        sb.append("-----------------------------------------------------------------------");
        return sb.toString();
    }
}

/*
 This class is not thread safe.
 */
class ArrayBackedBoundedStats
{
    private final long[] arrivalIntervals;
    private long sum = 0;
    private int index = 0;
    private boolean isFilled = false;
    private volatile double mean = 0;

    public ArrayBackedBoundedStats(final int size)
    {
        arrivalIntervals = new long[size];
    }

    public void add(long interval)
    {
        if(index == arrivalIntervals.length)
        {
            isFilled = true;
            index = 0;
        }

        if(isFilled)
            sum = sum - arrivalIntervals[index];

        arrivalIntervals[index++] = interval;
        sum += interval;
        mean = (double)sum / size();
    }

    private int size()
    {
        return isFilled ? arrivalIntervals.length : index;
    }

    public double mean()
    {
        return mean;
    }

    public long[] getArrivalIntervals()
    {
        return arrivalIntervals;
    }

}

class ArrivalWindow
{
    private static final Logger logger = LoggerFactory.getLogger(ArrivalWindow.class);
    private long tLast = 0L;
    private final ArrayBackedBoundedStats arrivalIntervals;
    private double lastReportedPhi = Double.MIN_VALUE;

    // in the event of a long partition, never record an interval longer than the rpc timeout,
    // since if a host is regularly experiencing connectivity problems lasting this long we'd
    // rather mark it down quickly instead of adapting
    // this value defaults to the same initial value the FD is seeded with
    private final long MAX_INTERVAL_IN_NANO = getMaxInterval();

    ArrivalWindow(int size)
    {
        arrivalIntervals = new ArrayBackedBoundedStats(size);
    }

    private static long getMaxInterval()
    {
        long newValue = FD_MAX_INTERVAL_MS.getLong(FailureDetector.INITIAL_VALUE_NANOS);
        if (newValue != FailureDetector.INITIAL_VALUE_NANOS)
            logger.info("Overriding {} from {}ms to {}ms", FD_MAX_INTERVAL_MS.getKey(), FailureDetector.INITIAL_VALUE_NANOS, newValue);
        return TimeUnit.NANOSECONDS.convert(newValue, TimeUnit.MILLISECONDS);
    }

    synchronized void add(long value, InetAddressAndPort ep)
    {
        assert tLast >= 0;
        if (tLast > 0L)
        {
            long interArrivalTime = (value - tLast);
            if (interArrivalTime <= MAX_INTERVAL_IN_NANO)
            {
                arrivalIntervals.add(interArrivalTime);
                logger.trace("Reporting interval time of {}ns for {}", interArrivalTime, ep);
            }
            else
            {
                logger.trace("Ignoring interval time of {}ns for {}", interArrivalTime, ep);
            }
        }
        else
        {
            // We use a very large initial interval since the "right" average depends on the cluster size
            // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
            // than low (false positives, which cause "flapping").
            arrivalIntervals.add(FailureDetector.INITIAL_VALUE_NANOS);
        }
        tLast = value;
    }

    double mean()
    {
        return arrivalIntervals.mean();
    }

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(long tnow)
    {
        assert arrivalIntervals.mean() > 0 && tLast > 0; // should not be called before any samples arrive
        long t = tnow - tLast;
        lastReportedPhi = t / mean();
        return lastReportedPhi;
    }

    double getLastReportedPhi()
    {
        return lastReportedPhi;
    }

    public String toString()
    {
        return Arrays.toString(arrivalIntervals.getArrivalIntervals());
    }
}

