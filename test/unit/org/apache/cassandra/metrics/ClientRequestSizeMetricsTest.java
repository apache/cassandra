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

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.datastax.driver.core.QueryOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.Range;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Ensures we properly account for metrics tracked in the native protocol
 */
@RunWith(Parameterized.class)
public class ClientRequestSizeMetricsTest extends CQLTester
{

    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return ProtocolVersion.SUPPORTED.stream()
                                        .map(v -> new Object[]{ v })
                                        .collect(Collectors.toList());
    }

    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @Test
    public void testReadAndWriteMetricsAreRecordedDuringNativeRequests() throws Throwable
    {
        // It may happen that the schema refreshment is done in the middle of the test which can pollute the results
        // We explicitly disable scheme fetching to avoid that effect
        try
        {
            reinitializeNetwork(builder -> {}, builder -> builder.withQueryOptions(new QueryOptions().setMetadataEnabled(false)));
            sessionNet(version); // Ensure that the connection is open

            // We want to ignore all the messages sent by the driver upon connection as well as
            // the event sent upon schema updates
            clearMetrics();

            executeNet(version, "SELECT * from system.peers");

            long requestLength = ClientMessageSizeMetrics.bytesReceived.getCount();
            long responseLength = ClientMessageSizeMetrics.bytesSent.getCount();

            assertThat(requestLength).isGreaterThan(0);
            assertThat(responseLength).isGreaterThan(0);

            checkMetrics(1, requestLength, responseLength);

            // Let's fire the same request again and test that the changes are the same that previously
            executeNet(version, "SELECT * from system.peers");

            checkMetrics(2, requestLength, responseLength);
        }
        finally
        {
            reinitializeNetwork();
        }
    }

    private void checkMetrics(int numberOfRequests, long requestLength, long responseLength)
    {
        Snapshot snapshot;
        long expectedTotalBytesRead = numberOfRequests * requestLength;
        assertEquals(expectedTotalBytesRead, ClientMessageSizeMetrics.bytesReceived.getCount());

        long expectedTotalBytesWritten = numberOfRequests * responseLength;
        assertEquals(expectedTotalBytesWritten, ClientMessageSizeMetrics.bytesSent.getCount());

        // The request fit in one single frame so we know the new number of received frames
        assertEquals(numberOfRequests, ClientMessageSizeMetrics.bytesReceivedPerRequest.getCount());

        snapshot = ClientMessageSizeMetrics.bytesReceivedPerRequest.getSnapshot();
        assertMin(snapshot, requestLength);
        assertMax(snapshot, requestLength);

        // The response fit in one single frame so we know the new number of received frames
        assertEquals(numberOfRequests, ClientMessageSizeMetrics.bytesSentPerResponse.getCount());

        snapshot = ClientMessageSizeMetrics.bytesSentPerResponse.getSnapshot();
        assertMin(snapshot, responseLength);
        assertMax(snapshot, responseLength);
    }

    private void assertMin(Snapshot snapshot, long value)
    {
        Range range = ((EstimatedHistogramReservoirSnapshot) snapshot).getBucketingRangeForValue(value);
        assertEquals(snapshot.getMin(), range.min);
    }

    private void assertMax(Snapshot snapshot, long value)
    {
        Range range = ((EstimatedHistogramReservoirSnapshot) snapshot).getBucketingRangeForValue(value);
        assertEquals(snapshot.getMax(), range.max);
    }

    private void clearMetrics()
    {
        clearCounter(ClientMessageSizeMetrics.bytesReceived);
        clearCounter(ClientMessageSizeMetrics.bytesSent);
        clearHistogram(ClientMessageSizeMetrics.bytesReceivedPerRequest);
        clearHistogram(ClientMessageSizeMetrics.bytesSentPerResponse);
    }

    private void clearCounter(Counter counter)
    {
        counter.dec(counter.getCount());
    }

    private void clearHistogram(Histogram histogram)
    {
        ((ClearableHistogram) histogram).clear();
    }
}
