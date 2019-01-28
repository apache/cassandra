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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
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
 * latency and providing an ISnitchMeasurement implementation back to this class.
 */
public abstract class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicEndpointSnitch.class);

    // Latency measurement and ranking. The samples contain latency measurements and the scores are used for ranking
    protected static final boolean USE_SEVERITY = !Boolean.getBoolean("cassandra.ignore_dynamic_snitch_severity");
    protected volatile Map<InetAddressAndPort, Double> scores = new HashMap<>();

    protected final Map<InetAddressAndPort, AnnotatedMeasurement> samples = new ConcurrentHashMap<>();

    // Latency probe functionality for actively probing endpoints that we haven't measured recently but are ranking
    public static final String LATENCY_PROBE_TP_NAME = "LatencyProbes";
    public static final long MAX_PROBE_INTERVAL_MS = Long.getLong("cassandra.dynamic_snitch_max_probe_interval_ms", 60 * 10 * 1000L);
    public static final long MIN_PROBE_INTERVAL_MS = Long.getLong("cassandra.dynamic_snitch_min_probe_interval_ms", 60 * 1000L) ;
    // The probe rate is set later when configuration is read in applyConfigChanges
    protected final RateLimiter probeRateLimiter;
    protected final DebuggableScheduledThreadPoolExecutor latencyProbeExecutor;
    private long lastUpdateSamplesNanos = System.nanoTime();

    // User configuration of the snitch tunables
    protected volatile int dynamicUpdateInterval = -1;
    protected volatile int dynamicSampleUpdateInterval = -1;
    protected volatile double dynamicBadnessThreshold = 0;

    // the score for a merged set of endpoints must be this much worse than the score for separate endpoints to
    // warrant not merging two ranges into a single range
    private static final double RANGE_MERGING_PREFERENCE = 1.5;

    private String mbeanName;
    private boolean mbeanRegistered = false;

    public final IEndpointSnitch subsnitch;

    private volatile ScheduledFuture<?> updateScoresScheduler = null;
    private volatile ScheduledFuture<?> updateSamplesScheduler = null;

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

        probeRateLimiter = RateLimiter.create(1);
        latencyProbeExecutor = ScheduledExecutors.getOrCreateSharedExecutor(LATENCY_PROBE_TP_NAME);
        latencyProbeExecutor.setMaximumPoolSize(1);

        if (DatabaseDescriptor.isDaemonInitialized())
        {
            // Applies configuration and registers with MBeanWrapper. Note that this does _not_ register
            // for latency updates, that happens in the MessagingService constructor
            open(false);
        }
    }

    /**
     * Update configuration of the background tasks and restart the various scheduler tasks
     * if the configured rates for these tasks have changed.
     */
    public void applyConfigChanges(int newDynamicUpdateInternal, int newDynamicSampleUpdateInterval,
                                   double newDynamicBadnessThreshold)
    {
        if (DatabaseDescriptor.isDaemonInitialized())
        {
            if (dynamicUpdateInterval != newDynamicUpdateInternal || updateScoresScheduler == null)
            {
                ensureCancelled(updateScoresScheduler);
                updateScoresScheduler = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, newDynamicUpdateInternal, newDynamicUpdateInternal, TimeUnit.MILLISECONDS);
            }

            if (dynamicSampleUpdateInterval != newDynamicSampleUpdateInterval || updateSamplesScheduler == null)
            {
                ensureCancelled(updateSamplesScheduler);
                if (newDynamicSampleUpdateInterval > 0)
                    updateSamplesScheduler = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateSamples, newDynamicSampleUpdateInterval, newDynamicSampleUpdateInterval, TimeUnit.MILLISECONDS);
            }
        }

        dynamicUpdateInterval = newDynamicUpdateInternal;
        dynamicUpdateInterval = newDynamicUpdateInternal;
        dynamicSampleUpdateInterval = newDynamicSampleUpdateInterval;
        dynamicBadnessThreshold = newDynamicBadnessThreshold;

        if (dynamicSampleUpdateInterval > 0)
            setProbeRateLimiter(dynamicSampleUpdateInterval);
    }

    @VisibleForTesting
    void setProbeRateLimiter(int updateIntervalMillis)
    {
        probeRateLimiter.setRate((double) 1000 / ((double) updateIntervalMillis));
    }

    public synchronized void open(boolean registerForLatency)
    {
        applyConfigChanges(DatabaseDescriptor.getDynamicUpdateInterval(),
                           DatabaseDescriptor.getDynamicSampleUpdateInterval(),
                           DatabaseDescriptor.getDynamicBadnessThreshold());

        if (!mbeanRegistered)
            MBeanWrapper.instance.registerMBean(this, mbeanName);

        mbeanRegistered = true;

        if (registerForLatency)
            MessagingService.instance().registerLatencySubscriber(this);

    }

    public synchronized void close()
    {
        ensureCancelled(updateScoresScheduler);
        ensureCancelled(updateSamplesScheduler);

        updateScoresScheduler = null;
        updateSamplesScheduler = null;

        List<ScheduledFuture> probesToCancel = new ArrayList<>();
        for (AnnotatedMeasurement measurement : samples.values())
        {
            probesToCancel.add(measurement.probeFuture);

            measurement.millisSinceLastMeasure.set(0);
            measurement.millisSinceLastRequest.set(0);
            measurement.nextProbeDelayMillis = 0;
            measurement.probeFuture = null;
        }
        ensureCancelled(probesToCancel);

        if (mbeanRegistered)
            MBeanWrapper.instance.unregisterMBean(mbeanName);

        MessagingService.instance().unregisterLatencySubscriber(this);

        mbeanRegistered = false;
    }

    private static void ensureCancelled(ScheduledFuture future)
    {
        ensureCancelled(Collections.singletonList(future));
    }

    private static void ensureCancelled(List<ScheduledFuture> futures)
    {
        List<ScheduledFuture> nonNullFutures = futures.stream()
                                                      .filter(Objects::nonNull)
                                                      .collect(Collectors.toList());
        nonNullFutures.forEach(f -> f.cancel(false));

        for (ScheduledFuture future : nonNullFutures)
        {
            try
            {
                future.get();
            }
            catch (CancellationException | InterruptedException | ExecutionException ignored)
            {
                // CancellationException is expected since we cancelled the future
                // If the probe was interrupted or threw an exception, we also don't care as it's stopped
                // running either way and we don't need the result of the computation anyways
            }
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
     * implementations. This is used to allow CASSANDRA-14459 latency probes as well as further safe
     * experimentation on new latency measurement techniques in CASSANDRA-14817
     *
     * {@link AnnotatedMeasurement#millisSinceLastRequest} is set to zero through
     * {@link DynamicEndpointSnitch#sortedByProximity(InetAddressAndPort, ReplicaCollection)}.
     * {@link AnnotatedMeasurement#millisSinceLastMeasure} is set to zero from
     * {@link DynamicEndpointSnitch#receiveTiming(InetAddressAndPort, long, LatencyMeasurementType)}.
     *
     * {@link AnnotatedMeasurement#millisSinceLastMeasure and {@link AnnotatedMeasurement#nextProbeDelayMillis }
     * are incremented via {@link DynamicEndpointSnitch#updateSamples()}
     */
    protected static class AnnotatedMeasurement
    {
        // Used to optimally target latency probes only on nodes that are both requested for ranking
        // and are not being measured. For example with token aware clients a large portion of the cluster will never
        // be ranked at all and therefore we won't probe them.
        public AtomicLong millisSinceLastRequest = new AtomicLong(0);
        public AtomicLong millisSinceLastMeasure = new AtomicLong(0);
        public volatile long nextProbeDelayMillis = 0;
        public volatile ScheduledFuture<?> probeFuture = null;

        // The underlying measurement technique. E.g. a median filter (histogram) or an EMA filter, etc ...
        public final ISnitchMeasurement measurement;
        public volatile double cachedMeasurement;

        public AnnotatedMeasurement(ISnitchMeasurement measurement)
        {
            this.measurement = measurement;
            this.cachedMeasurement = measurement.measure();
        }

        @Override
        public String toString()
        {
            return "AnnotatedMeasurement{" +
                   "millisSinceLastRequest=" + millisSinceLastRequest +
                   ", millisSinceLastMeasure=" + millisSinceLastMeasure +
                   ", nextProbeDelayMillis=" + nextProbeDelayMillis +
                   ", probeFuturePending=" + (probeFuture != null && !probeFuture.isDone()) +
                   ", measurementClass=" + measurement.getClass().getSimpleName() +
                   ", cachedMeasurement=" + cachedMeasurement +
                   '}';
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
    public void receiveTiming(InetAddressAndPort address, long latencyMicros, LatencyMeasurementType measurementType)
    {
        if (measurementType == LatencyMeasurementType.IGNORE)
           return;

        AnnotatedMeasurement sample = samples.get(address);

        if (sample == null)
        {
            AnnotatedMeasurement maybeNewSample = new AnnotatedMeasurement(measurementImpl(latencyMicros));
            sample = samples.putIfAbsent(address, maybeNewSample);
            if (sample == null)
                sample = maybeNewSample;
        }

        // The conditional load -> store barrier is probably cheaper than an unconditional store-store
        // Since this is on the fast path we do this, otherwise we could just .set(0)
        if (measurementType == LatencyMeasurementType.READ && sample.millisSinceLastMeasure.get() > 0)
            sample.millisSinceLastMeasure.lazySet(0);

        sample.measurement.sample(latencyMicros);
    }

    @VisibleForTesting
    protected void reset()
    {
        scores.clear();
        samples.clear();
    }

    @VisibleForTesting
    protected void updateScores()
    {
        if (!StorageService.instance.isGossipActive() ||
            !MessagingService.instance().isListening())
        {
            return;
        }

        this.scores = calculateScores(samples);
    }

    /**
     * This is generally expensive and is called periodically (semi-frequently) not on the fast path.
     * The main concern here is generating garbage from the measurements (e.g. histograms in particular)
     */
    @VisibleForTesting
    protected static Map<InetAddressAndPort, Double> calculateScores(Map<InetAddressAndPort, AnnotatedMeasurement> samples)
    {
        double maxLatency = 1;
        HashMap<InetAddressAndPort, Double> newScores = new HashMap<>(samples.size());

        // We're going to weight the latency for each host against the worst one we see, to
        // arrive at sort of a 'badness percentage' for them. First, find the worst latency for each:
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry : samples.entrySet())
        {
            AnnotatedMeasurement annotatedMeasurement = entry.getValue();

            // only compute the measurement, which typically generates the most garbage (e.g. for this Histogram),
            // for endpoints we have recently requested for ranking
            if (annotatedMeasurement.millisSinceLastRequest.get() < MAX_PROBE_INTERVAL_MS)
                annotatedMeasurement.cachedMeasurement = annotatedMeasurement.measurement.measure();

            newScores.put(entry.getKey(), annotatedMeasurement.cachedMeasurement);
            maxLatency = Math.max(annotatedMeasurement.cachedMeasurement, maxLatency);
        }

        // now make another pass to normalize the latency scores based on the maximums we found before
        for (Map.Entry<InetAddressAndPort, Double> entry : newScores.entrySet())
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
     * Background task running on the samples dictionary. The default implementation sends latency probes (PING)
     * messages to explore nodes that we have not received timings for recently but have ranked in
     * {@link DynamicEndpointSnitch#sortedByProximity(InetAddressAndPort, ReplicaCollection)}.
     */
    protected void updateSamples()
    {
        long updateIntervalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastUpdateSamplesNanos);
        lastUpdateSamplesNanos = System.nanoTime();

        // Split calculation of probe timers from sending probes for testability
        calculateProbes(samples, updateIntervalMillis);

        // We do this after the calculations so that the progression of the logical clocks continues regardless
        // if gossip is enabled or not. However if Gossip is not active we don't _send_ the probes
        if (!StorageService.instance.isGossipActive())
            return;

        scheduleProbes(samples);
    }

    /**
     * This method mutates the passed AnnotatedMeasurements to implement capped exponential backoff per endpoint.
     *
     * The algorithm is as follows:
     * 1. All samples get their millisSinceLastMeasure and millisSinceLastRequest fields
     *    incremented by the passed interval
     * 2. Any recently requested (ranked) endpoints that have not been measured recently (e.g. because the snitch
     *    has sent them no traffic) get probes with exponential backoff.
     *
     * The backoff is capped at MAX_PROBE_INTERVAL_MS. Furthermore the probes are stopped after
     * MAX_PROBE_INTERVAL_MS of no ranking requests as well.
     *
     * At the end of this method, any passed AnnotatedMeasurements that need latency probes will have non zero
     * nextProbeDelayMillis members set.
     */
    @VisibleForTesting
    protected static void calculateProbes(Map<InetAddressAndPort, AnnotatedMeasurement> samples, long intervalMillis) {
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry: samples.entrySet())
        {
            AnnotatedMeasurement measurement = entry.getValue();
            long lastMeasure = measurement.millisSinceLastMeasure.addAndGet(intervalMillis);
            long lastRequest = measurement.millisSinceLastRequest.addAndGet(intervalMillis);

            if (lastMeasure >= MIN_PROBE_INTERVAL_MS && lastRequest < MAX_PROBE_INTERVAL_MS)
            {
                if (measurement.nextProbeDelayMillis == 0)
                {
                    measurement.nextProbeDelayMillis = intervalMillis;
                }
                else if (measurement.probeFuture != null && measurement.probeFuture.isDone())
                {
                    measurement.nextProbeDelayMillis = Math.min(MAX_PROBE_INTERVAL_MS, measurement.nextProbeDelayMillis * 2);
                }
            }
            else
            {
                measurement.nextProbeDelayMillis = 0;
            }
        }
    }

    @VisibleForTesting
    void scheduleProbes(Map<InetAddressAndPort, AnnotatedMeasurement> samples)
    {
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry: samples.entrySet())
        {
            AnnotatedMeasurement measurement = entry.getValue();

            if (measurement.millisSinceLastRequest.get() > MAX_PROBE_INTERVAL_MS)
            {
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(entry.getKey());
                if (epState == null || Gossiper.instance.isDeadState(epState))
                {
                    samples.remove(entry.getKey());
                    continue;
                }
                // Don't send probes to nodes that probably can't respond
                if (!epState.isAlive())
                    continue;
            }

            long delay = measurement.nextProbeDelayMillis;
            if (delay > 0 && (measurement.probeFuture == null || measurement.probeFuture.isDone()))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Scheduled latency probe against {} in {}ms", entry.getKey(), delay);
                measurement.probeFuture = latencyProbeExecutor.schedule(() -> sendLatencyProbeToPeer(entry.getKey()),
                                                                        delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Method that actually sends latency probes as PING messages. This is the only function in this class
     * that operates on the latencyProbeExecutor thread and it records the maximum latency between a small and large
     * message channel ping.
     */
    private void sendLatencyProbeToPeer(InetAddressAndPort to)
    {
        // This method may have been scheduled (a long time) before it executes, so have to do
        // some quick sanity checks before sending a message to this host
        if (!StorageService.instance.isGossipActive() || !Gossiper.instance.isAlive(to))
            return;

        probeRateLimiter.acquire();

        // We don't have a good way of estimating the localhost latency right now, so instead we give it
        // the minimum of any other host's measurement
        if (to.equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            // TODO: incorporate a basic local (disk) latency check instead
            double minimumLatency = Long.MAX_VALUE;
            for (AnnotatedMeasurement measurement : samples.values())
            {
                minimumLatency = Math.min(minimumLatency, measurement.cachedMeasurement);
            }
            // There are no other latency measurements anywhere
            if (minimumLatency == Long.MAX_VALUE)
                minimumLatency = 0;

            if (logger.isTraceEnabled())
                logger.trace("Local latency probe, using global minimum measurement: {}us", (long) minimumLatency);

            receiveTiming(to, (long) minimumLatency, LatencyMeasurementType.PROBE);
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("Latency probe sending a small and large PingMessage to {}", to);

        // Bypass the normal latency measurement path so we can compute maximum latencies of the two probe
        // messages. This way we can measure the "worst case" via these pings. This complexity may not be
        // justified, but leaving it for now.
        long start = System.nanoTime();

        SettableFuture<Long> smallChannelLatencyNanos = SettableFuture.create();
        SettableFuture<Long> largeChannelLatencyNanos = SettableFuture.create();

        IAsyncCallbackWithFailure smallChannelCallback = new IAsyncCallbackWithFailure()
        {
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                smallChannelLatencyNanos.set(System.nanoTime() - start);
            }

            public void response(MessageIn msg)
            {
                smallChannelLatencyNanos.set(System.nanoTime() - start);
            }
        };
        IAsyncCallbackWithFailure largeChannelCallback = new IAsyncCallbackWithFailure()
        {
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                largeChannelLatencyNanos.set(System.nanoTime() - start);
            }
            public void response(MessageIn msg)
            {
                largeChannelLatencyNanos.set(System.nanoTime() - start);
            }
        };

        MessageOut<PingMessage> smallChannelMessageOut = new MessageOut<>(PING, PingMessage.smallChannelMessage,
                                                                          PingMessage.serializer, SMALL_MESSAGE);
        MessageOut<PingMessage> largeChannelMessageOut = new MessageOut<>(PING, PingMessage.largeChannelMessage,
                                                                          PingMessage.serializer, LARGE_MESSAGE);
        MessagingService.instance().sendRRWithFailure(smallChannelMessageOut, to, smallChannelCallback);
        MessagingService.instance().sendRRWithFailure(largeChannelMessageOut, to, largeChannelCallback);

        // This should execute on the RequestResponse stage thread unless both futures are already done
        Futures.allAsList(smallChannelLatencyNanos, largeChannelLatencyNanos).addListener(() -> {
            try
            {
                long latencySmallNanos = smallChannelLatencyNanos.get();
                long latencyLargeNanos = largeChannelLatencyNanos.get();
                long maxLatencyInMicros = TimeUnit.NANOSECONDS.toMicros(Math.max(latencySmallNanos, latencyLargeNanos));
                logger.trace("Latency probe recording latency of {}us for {}", maxLatencyInMicros, to);
                receiveTiming(to, maxLatencyInMicros, LatencyMeasurementType.PROBE);
            }
            catch (InterruptedException | ExecutionException e)
            {
                logger.error("Exception while waiting for latencies", e);
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void gossiperStarting()
    {
        subsnitch.gossiperStarting();
    }

    @Override
    public String getRack(InetAddressAndPort endpoint)
    {
        return subsnitch.getRack(endpoint);
    }

    @Override
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
            // The conditional load -> store barrier is probably cheaper than an unconditional store-store
            // Since this is on the fast path we do this, otherwise we could just .set(0)
            if (measurement != null && measurement.millisSinceLastRequest.get() > 0)
                measurement.millisSinceLastRequest.lazySet(0);
        }

        // Scores can change concurrently from a call to this method. But Collections.sort() expects
        // its comparator to be "stable", that is 2 endpoints should compare the same way for the duration
        // of the sort() call. As we swap the scores map on write, it is thus enough to alias the current
        // version of it during this call.
        Map<InetAddressAndPort, Double> aliasedScores = this.scores;

        return dynamicBadnessThreshold == 0
               ? sortedByProximityWithScore(address, unsortedAddresses, aliasedScores)
               : sortedByProximityWithBadness(address, unsortedAddresses, aliasedScores);
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithScore(final InetAddressAndPort address, C unsortedAddresses,
                                                                                    Map<InetAddressAndPort, Double> aliasedScores)
    {
        return unsortedAddresses.sorted((r1, r2) -> compareEndpoints(address, r1, r2, aliasedScores));
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithBadness(final InetAddressAndPort address, C replicas,
                                                                                      Map<InetAddressAndPort, Double> aliasedScores)
    {
        if (replicas.size() < 2)
            return replicas;

        // TODO: avoid copy
        replicas = subsnitch.sortedByProximity(address, replicas);

        ArrayList<Double> subsnitchOrderedScores = new ArrayList<>(replicas.size());
        for (Replica replica : replicas)
        {
            Double score = aliasedScores.get(replica.endpoint());
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
                return sortedByProximityWithScore(address, replicas, aliasedScores);
            }
        }

        return replicas;
    }

    // Compare endpoints given an immutable snapshot of the scores
    private int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2, Map<InetAddressAndPort, Double> aliasedScores)
    {
        Double scored1 = aliasedScores.get(a1.endpoint());
        Double scored2 = aliasedScores.get(a2.endpoint());

        // If we don't have latency information about one or more of the replicas, trust the subsnitch until we have
        // latency information.
        if (scored1 == null || scored2 == null || scored1.equals(scored2))
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
    protected Map<InetAddressAndPort, AnnotatedMeasurement> getMeasurementsWithPort()
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
    public int getSampleUpdateInterval()
    {
        return dynamicSampleUpdateInterval;
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
     *
     * Note that pre 4.0 this was milliseconds, so we keep backwards compatibility here.
     */
    public List<Double> dumpTimings(String hostname) throws UnknownHostException
    {
        List<Double> micros = dumpTimingsMicros(hostname);
        return micros.stream().map(s -> s / 1000.0).collect(Collectors.toList());
    }

    public List<Double> dumpTimingsMicros(String hostname) throws UnknownHostException
    {
        InetAddressAndPort host = InetAddressAndPort.getByName(hostname);
        ArrayList<Double> timings = new ArrayList<>();
        AnnotatedMeasurement sample = samples.get(host);
        if (sample != null)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} -> {}", host, sample.toString());
            for (double measurement: sample.measurement.measurements())
                timings.add(measurement);
        }
        return timings;
    }

    public void setSeverity(double severity)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, StorageService.instance.valueFactory.severity(severity));
    }

    protected static double getSeverity(InetAddressAndPort endpoint)
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
