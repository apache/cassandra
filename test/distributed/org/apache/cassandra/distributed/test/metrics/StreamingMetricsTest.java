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

package org.apache.cassandra.distributed.test.metrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class StreamingMetricsTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = Cluster.build().withNodes(2)
                         .withDataDirCount(1)
                         .withConfig(config -> config.with(NETWORK)
                                                     .set("stream_entire_sstables", false))
                         .start();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };");
    }

    private static InetAddressAndPort getNodeAddress(int num)
    {
        InetSocketAddress broadcastAddress = cluster.get(num).broadcastAddress();
        return InetAddressAndPort.getByAddressOverrideDefaults(broadcastAddress.getAddress(),
                                                               broadcastAddress.getPort());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }


    @Test
    public void testStreamMetrics()
    {
        cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'false'}", KEYSPACE, "LeveledCompactionStrategy"));

        final int rowsPerFile = 500;
        final int files = 5;
        for (int k = 0; k < files; k++)
        {
            for (int i = k * rowsPerFile; i < k * rowsPerFile + rowsPerFile; ++i)
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');"), Integer.toString(i));
            cluster.get(1).nodetool("flush");
        }

        cluster.get(2).executeInternal("TRUNCATE system.available_ranges;");
        Object[][] results = cluster.get(2).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
        assertThat(results.length).isEqualTo(0);

        InetAddressAndPort node1Address = getNodeAddress(1);
        InetAddressAndPort node2Address = getNodeAddress(2);

        // Trigger streaming from node 2
        cluster.get(2).nodetool("rebuild", "--keyspace", KEYSPACE);

        // Assert metrics in node 2
        long transmittedBytes = cluster.get(2).callOnInstance(() -> {
            StreamingMetrics metrics = StreamingMetrics.get(node1Address);
            assertThat(metrics.incomingBytes.getCount())
                .isGreaterThan(0)
                .describedAs("There should be bytes streamed from the peer.");
            assertThat(metrics.outgoingBytes.getCount())
                .isEqualTo(0)
                .describedAs("There should not be sstables streamed to the peer.");
            assertThat(metrics.incomingProcessTime.getCount())
                .isEqualTo(files)
                .describedAs("There should be " + files + " files streamed from the peer.");
            assertThat(metrics.incomingProcessTime.getSnapshot().getMedian())
                .isGreaterThan(0)
                .describedAs("The median processing time should be non-0");
            return metrics.incomingBytes.getCount();
        });

        // Assert metrics in node 1
        cluster.get(1).runOnInstance(() -> {
            StreamingMetrics metrics = StreamingMetrics.get(node2Address);
            assertThat(metrics.incomingBytes.getCount())
                .isEqualTo(0).describedAs("There should not be sstables streamed from the peer.");
            assertThat(metrics.outgoingBytes.getCount())
                .isEqualTo(transmittedBytes)
                .describedAs("The outgoingBytes count in node1 should be equals to incomingBytes count in node2");
            assertThat(metrics.incomingProcessTime.getCount())
                .isEqualTo(0)
                .describedAs("There should be no files streamed from the peer.");
        });
    }
}
