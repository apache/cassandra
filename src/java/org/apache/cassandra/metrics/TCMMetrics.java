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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TCMMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("TCM");

    public final Gauge<Long> currentEpochGauge;
    public final Histogram fetchedPeerLogEntries;
    public final Histogram fetchedCMSLogEntries;
    public final Histogram servedPeerLogEntries;
    public final Histogram servedCMSLogEntries;
    public final Meter logEntryFetchRate;

    public TCMMetrics()
    {
        currentEpochGauge = Metrics.register(factory.createMetricName("Epoch"), () -> {
            ClusterMetadata metadata =  ClusterMetadata.currentNullable();
            if (metadata == null)
                return Epoch.EMPTY.getEpoch();
            return metadata.epoch.getEpoch();
        });

        fetchedPeerLogEntries = Metrics.histogram(factory.createMetricName("FetchedPeerLogEntries"), false);
        fetchedCMSLogEntries = Metrics.histogram(factory.createMetricName("FetchedCMSLogEntries"), false);
        servedPeerLogEntries = Metrics.histogram(factory.createMetricName("ServedPeerLogEntries"), false);
        servedCMSLogEntries = Metrics.histogram(factory.createMetricName("ServedCMSLogEntries"), false);

        logEntryFetchRate = Metrics.meter(factory.createMetricName("LogEntryFetchRate"));
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
