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

package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.EpochAwareDebounce;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration.needsReconfiguration;

public class TCMMetrics
{
    public static final String TYPE_NAME = "TCM";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    public static final TCMMetrics instance = new TCMMetrics();

    public final Gauge<Long> currentEpochGauge;
    public final Gauge<Long> currentCMSSize;
    public final Gauge<Long> unreachableCMSMembers;
    public final Gauge<Integer> isCMSMember;
    public final Gauge<Integer> needsCMSReconfiguration;
    public final Histogram fetchedPeerLogEntries;
    public final Histogram fetchedCMSLogEntries;
    public final Timer fetchPeerLogLatency;
    public final Timer fetchCMSLogLatency;
    public final Meter fetchCMSLogConsistencyDowngrade;
    public final Histogram servedPeerLogEntries;
    public final Histogram servedCMSLogEntries;
    public final Meter logEntryFetchRate;
    public final Timer commitRejectionLatency;
    public final Timer commitFailureLatency;
    public final Timer commitSuccessLatency;
    public final Meter commitRetries;
    public final Meter fetchLogRetries;
    public final Meter progressBarrierRetries;
    // TODO: we eventually want to rely on (currently non-existing) metric that tracks paxos topology retries for all Paxos.
    public final Meter repairPaxosTopologyRetries;
    public final Timer progressBarrierLatency;
    public final Meter progressBarrierCLRelax;
    public final Meter coordinatorBehindSchema;
    public final Meter coordinatorBehindPlacements;
    public final Gauge<Long> epochAwareDebounceTrackerSize;

    private TCMMetrics()
    {
       currentEpochGauge = Metrics.register(factory.createMetricName("Epoch"), () -> {
            ClusterMetadata metadata =  ClusterMetadata.currentNullable();
            if (metadata == null)
                return Epoch.EMPTY.getEpoch();
            return metadata.epoch.getEpoch();
        });

        currentCMSSize = Metrics.register(factory.createMetricName("CMSSize"), () -> {
            ClusterMetadata metadata =  ClusterMetadata.currentNullable();
            if (metadata == null)
                return 0L;
            return (long)metadata.fullCMSMembers().size();
        });

        unreachableCMSMembers = Metrics.register(factory.createMetricName("UnreachableCMSMembers"), () -> {
            ClusterMetadata metadata =  ClusterMetadata.currentNullable();
            if (metadata == null)
                return 0L;
            return metadata.fullCMSMembers().stream().filter(FailureDetector.isEndpointAlive.negate()).count();
        });

        isCMSMember = Metrics.register(factory.createMetricName("IsCMSMember"), () -> {
            ClusterMetadata metadata =  ClusterMetadata.currentNullable();
            return metadata != null && metadata.isCMSMember(FBUtilities.getBroadcastAddressAndPort()) ? 1 : 0;
        });

        needsCMSReconfiguration = Metrics.register(factory.createMetricName("NeedsCMSReconfiguration"), () -> {
            ClusterMetadata metadata =  ClusterMetadata.currentNullable();
            return metadata != null && needsReconfiguration(metadata) ? 1 : 0;
        });

        epochAwareDebounceTrackerSize = Metrics.register(factory.createMetricName("EpochAwareDebounceTrackerEntries"), () -> {
            // don't replace with a method reference because tests may access metrics before EAD is initialized
            return EpochAwareDebounce.instance.inflightTrackerSize();
        });

        fetchedPeerLogEntries = Metrics.histogram(factory.createMetricName("FetchedPeerLogEntries"), false);
        fetchPeerLogLatency = Metrics.timer(factory.createMetricName("FetchPeerLogLatency"));
        fetchedCMSLogEntries = Metrics.histogram(factory.createMetricName("FetchedCMSLogEntries"), false);
        fetchCMSLogLatency = Metrics.timer(factory.createMetricName("FetchCMSLogLatency"));
        fetchCMSLogConsistencyDowngrade = Metrics.meter(factory.createMetricName("FetchCMSLogConsistencyDowngradeRelax"));
        servedPeerLogEntries = Metrics.histogram(factory.createMetricName("ServedPeerLogEntries"), false);
        servedCMSLogEntries = Metrics.histogram(factory.createMetricName("ServedCMSLogEntries"), false);
        fetchLogRetries = Metrics.meter(factory.createMetricName("FetchLogRetries"));
        repairPaxosTopologyRetries = Metrics.meter(factory.createMetricName("RepairCMSPaxosTopologyRetries"));
        logEntryFetchRate = Metrics.meter(factory.createMetricName("LogEntryFetchRate"));

        commitRejectionLatency = Metrics.timer(factory.createMetricName("CommitRejectionLatency"));
        commitFailureLatency = Metrics.timer(factory.createMetricName("CommitFailureLatency"));
        commitSuccessLatency = Metrics.timer(factory.createMetricName("CommitSuccessLatency"));
        commitRetries = Metrics.meter(factory.createMetricName("CommitRetries"));

        progressBarrierRetries = Metrics.meter(factory.createMetricName("ProgressBarrierRetries"));
        progressBarrierLatency = Metrics.timer(factory.createMetricName("ProgressBarrierLatency"));
        progressBarrierCLRelax = Metrics.meter(factory.createMetricName("ProgressBarrierCLRelaxed"));

        coordinatorBehindSchema = Metrics.meter(factory.createMetricName("CoordinatorBehindSchema"));
        coordinatorBehindPlacements = Metrics.meter(factory.createMetricName("CoordinatorBehindPlacements"));
    }

    public void recordCommitFailureLatency(long latency, TimeUnit timeUnit, boolean isRejection)
    {
        if (isRejection)
            commitRejectionLatency.update(latency, timeUnit);
        else
            commitFailureLatency.update(latency, timeUnit);
    }

    public void peerLogEntriesFetched(Epoch before, Epoch after)
    {
        logEntryFetchRate.mark();
        updateLogEntryHistogram(fetchedPeerLogEntries, before, after);
    }

    public void cmsLogEntriesFetched(Epoch before, Epoch after)
    {
        logEntryFetchRate.mark();
        updateLogEntryHistogram(fetchedCMSLogEntries, before, after);
    }

    public void peerLogEntriesServed(Epoch before, Epoch after)
    {
        updateLogEntryHistogram(servedPeerLogEntries, before, after);
    }

    public void cmsLogEntriesServed(Epoch before, Epoch after)
    {
        updateLogEntryHistogram(servedCMSLogEntries, before, after);
    }

    private void updateLogEntryHistogram(Histogram histogram, Epoch before, Epoch after)
    {
        if (after.isAfter(before))
            histogram.update(after.getEpoch() - before.getEpoch());
        else
            histogram.update(0L);
    }
}
