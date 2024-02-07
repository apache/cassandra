/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CASClientRequestMetrics extends ClientRequestMetrics
{
    public final Histogram contention;
    public final Counter unfinishedCommit;
    public final Meter unknownResult;

    // latencies for 4 paxos phases
    public final LatencyMetrics prepareLatency;
    public final LatencyMetrics createProposalLatency;
    public final LatencyMetrics proposeLatency;
    public final LatencyMetrics commitLatency;

    // latency for backoff when there is contention
    public final LatencyMetrics contentionBackoffLatency;

    // num of replicas that are missing MRC
    public final Counter missingMostRecentCommit;

    public CASClientRequestMetrics(String scope, String namePrefix)
    {
        super(scope, namePrefix);

        contention = Metrics.histogram(factory.createMetricName(namePrefix + "ContentionHistogram"), false);
        unfinishedCommit = Metrics.counter(factory.createMetricName(namePrefix + "UnfinishedCommit"));
        unknownResult = Metrics.meter(factory.createMetricName(namePrefix + "UnknownResult"));

        prepareLatency = new LatencyMetrics(factory, namePrefix + "Prepare");
        createProposalLatency = new LatencyMetrics(factory, namePrefix + "CreateProposal");
        proposeLatency = new LatencyMetrics(factory, namePrefix + "Propose");
        commitLatency = new LatencyMetrics(factory, namePrefix + "Commit");

        contentionBackoffLatency = new LatencyMetrics(factory, namePrefix + "ContentionBackoff");

        missingMostRecentCommit = Metrics.counter(factory.createMetricName(namePrefix + "MissingMostRecentCommit"));
    }

    public void release()
    {
        super.release();
        Metrics.remove(factory.createMetricName(namePrefix + "ContentionHistogram"));
        Metrics.remove(factory.createMetricName(namePrefix + "UnfinishedCommit"));
        Metrics.remove(factory.createMetricName(namePrefix + "UnknownResult"));

        prepareLatency.release();
        createProposalLatency.release();
        proposeLatency.release();
        commitLatency.release();

        contentionBackoffLatency.release();

        Metrics.remove(factory.createMetricName(namePrefix + "MissingMostRecentCommit"));
    }
}
