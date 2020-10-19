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

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.Range;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Ensures we properly account for metrics tracked in the native protocol
 */
public class ClientRequestSizeMetricsTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @Test
    public void testReadAndWriteMetricsAreRecordedDuringNativeRequests() throws Throwable
    {
        // We want to ignore all the messages sent by the driver upon connection as well as
        // the event sent upon schema updates
        clearMetrics();

        executeNet("SELECT * from system.peers");

        long requestLength = ClientRequestSizeMetrics.totalBytesRead.getCount();
        long responseLength = ClientRequestSizeMetrics.totalBytesWritten.getCount();

        assertThat(requestLength).isGreaterThan(0);
        assertThat(responseLength).isGreaterThan(0);

        checkMetrics(1, requestLength, responseLength);

        // Let's fire the same request again and test that the changes are the same that previously
        executeNet("SELECT * from system.peers");

        checkMetrics(2, requestLength, responseLength);
    }

    private void checkMetrics(int numberOfRequests, long requestLength, long responseLength)
    {
        Snapshot snapshot;
        long expectedTotalBytesRead = numberOfRequests * requestLength;
        assertEquals(expectedTotalBytesRead, ClientRequestSizeMetrics.totalBytesRead.getCount());

        long expectedTotalBytesWritten = numberOfRequests * responseLength;
        assertEquals(expectedTotalBytesWritten, ClientRequestSizeMetrics.totalBytesWritten.getCount());

        // The request fit in one single frame so we know the new number of received frames
        assertEquals(numberOfRequests, ClientRequestSizeMetrics.bytesReceivedPerFrame.getCount());

        snapshot = ClientRequestSizeMetrics.bytesReceivedPerFrame.getSnapshot();
        assertMin(snapshot, requestLength);
        assertMax(snapshot, requestLength);

        // The response fit in one single frame so we know the new number of received frames
        assertEquals(numberOfRequests, ClientRequestSizeMetrics.bytesTransmittedPerFrame.getCount());

        snapshot = ClientRequestSizeMetrics.bytesTransmittedPerFrame.getSnapshot();
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
        clearCounter(ClientRequestSizeMetrics.totalBytesRead);
        clearCounter(ClientRequestSizeMetrics.totalBytesWritten);
        clearHistogram(ClientRequestSizeMetrics.bytesReceivedPerFrame);
        clearHistogram(ClientRequestSizeMetrics.bytesTransmittedPerFrame);
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
