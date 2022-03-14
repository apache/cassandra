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

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Histogram;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ReadCoordinationMetricsTest
{
    @BeforeClass()
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testReadCoordinatorMetrics()
    {
        assertEquals(0, ReadCoordinationMetrics.nonreplicaRequests.getCount());
        ReadCoordinationMetrics.nonreplicaRequests.inc();
        assertEquals(1, ReadCoordinationMetrics.nonreplicaRequests.getCount());
        ReadCoordinationMetrics.nonreplicaRequests.dec();
        assertEquals(0, ReadCoordinationMetrics.nonreplicaRequests.getCount());

        assertEquals(0, ReadCoordinationMetrics.preferredOtherReplicas.getCount());
        ReadCoordinationMetrics.preferredOtherReplicas.inc();
        assertEquals(1, ReadCoordinationMetrics.preferredOtherReplicas.getCount());
        ReadCoordinationMetrics.preferredOtherReplicas.dec();
        assertEquals(0, ReadCoordinationMetrics.preferredOtherReplicas.getCount());
    }

    @Test
    public void testReplicaLatencyHistogram() throws UnknownHostException
    {
        InetAddressAndPort host = InetAddressAndPort.getByName("127.0.0.1");
        assertNull(ReadCoordinationMetrics.getReplicaLatencyHistogram(host));

        // Record a replica latency
        ReadCoordinationMetrics.updateReplicaLatency(host, 100, TimeUnit.MILLISECONDS);

        Histogram histogram = ReadCoordinationMetrics.getReplicaLatencyHistogram(host);
        assertNotNull(histogram);
        assertEquals(1, histogram.getCount());

        // ReadRpcTimeout latency should not be tracked
        ReadCoordinationMetrics.updateReplicaLatency(host, DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        assertEquals(1, histogram.getCount());
    }
}
