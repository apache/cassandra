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

import java.net.InetSocketAddress;
import java.util.stream.Stream;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;

public class StreamingMetricsTest extends TestBaseImpl
{

    private static InetAddressAndPort getNodeAddress(Cluster cluster, int num)
    {
        InetSocketAddress broadcastAddress = cluster.get(num).broadcastAddress();
        return InetAddressAndPort.getByAddressOverrideDefaults(broadcastAddress.getAddress(),
                                                               broadcastAddress.getPort());
    }

    @Test
    public void testMetricsWithRepairAndStreamingFromTwoNodes() throws Exception
    {
        testMetricsWithStreamingFromTwoNodes(true);
    }

    @Test
    public void testMetricsWithRebuildAndStreamingFromTwoNodes() throws Exception
    {
        testMetricsWithStreamingFromTwoNodes(false);
    }

    public void testMetricsWithStreamingFromTwoNodes(boolean useRepair) throws Exception
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withDataDirCount(1)
                                          .withConfig(config -> config.with(NETWORK)
                                                                      .set("stream_entire_sstables", false)
                                                                      .set("hinted_handoff_enabled", false))
                                          .start(), 2))
        {
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'false'}", KEYSPACE, "LeveledCompactionStrategy"));

            IMessageFilters.Filter drop1to3 = cluster.filters().verbs(MUTATION_REQ.id).from(1).to(3).drop();

            final int rowsPerFile = 500;
            final int files = 5;
            for (int k = 0; k < files; k++)
            {
                for (int i = k * rowsPerFile; i < k * rowsPerFile + rowsPerFile; ++i)
                {
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');"),
                                                   ConsistencyLevel.ONE,
                                                   Integer.toString(i));
                }
                cluster.get(1).flush(KEYSPACE);
                cluster.get(2).flush(KEYSPACE);
            }

            drop1to3.off();

            // Checks that the table is empty on node 3
            Object[][] results = cluster.get(3).executeInternal(withKeyspace("SELECT k, c1, c2 FROM %s.cf;"));
            assertThat(results.length).isEqualTo(0);

            checkThatNoStreamingOccuredBetweenTheThreeNodes(cluster);

            // Trigger streaming from node 3
            if (useRepair)
                cluster.get(3).nodetool("repair", "--full");
            else
                cluster.get(3).nodetool("rebuild", "--keyspace", KEYSPACE);


            // Check streaming metrics on node 1
            checkThatNoStreamingOccured(cluster, 1, 2);
            long bytesFrom1 = checkDataSent(cluster, 1, 3);
            checkDataReceived(cluster, 1, 3, 0, 0);

            if (useRepair)
                checkTotalDataSent(cluster, 1, bytesFrom1, bytesFrom1, files);
            else
                checkTotalDataSent(cluster, 1, bytesFrom1, 0, 0);

            checkTotalDataReceived(cluster, 1, 0);

            // Check streaming metrics on node 2
            checkThatNoStreamingOccured(cluster, 2, 1);
            long bytesFrom2 = checkDataSent(cluster, 2, 3);
            checkDataReceived(cluster, 1, 2, 0, 0);

            if (useRepair)
                checkTotalDataSent(cluster, 2, bytesFrom2, bytesFrom2, files);
            else
                checkTotalDataSent(cluster, 2, bytesFrom2, 0, 0);

            checkTotalDataReceived(cluster, 2, 0);

            // Check streaming metrics on node 3
            checkDataReceived(cluster, 3, 1, bytesFrom1, files);
            checkDataReceived(cluster, 3, 2, bytesFrom2, files);
            checkTotalDataSent(cluster, 3, 0, 0, 0);
            checkTotalDataReceived(cluster, 3, bytesFrom1 + bytesFrom2);
        }
    }

    @Test
    public void testMetricsWithRebuildAndStreamingToTwoNodes() throws Exception
    {
        testMetricsWithStreamingToTwoNodes(false);
    }

    @Test
    public void testMetricsWithRepairAndStreamingToTwoNodes() throws Exception
    {
        testMetricsWithStreamingToTwoNodes(true);
    }

    private int getNumberOfSSTables(Cluster cluster, int node) {
        return cluster.get(node).callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf").getLiveSSTables().size());
    }

    public void testMetricsWithStreamingToTwoNodes(boolean useRepair) throws Exception
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withDataDirCount(1)
                                          .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                      .set("stream_entire_sstables", false)
                                                                      .set("hinted_handoff_enabled", false))
                                          .start(), 2))
        {
            Stream.of(1,2,3).map(cluster::get).forEach(i -> i.runOnInstance(() -> SystemKeyspace.forceBlockingFlush(SystemKeyspace.LOCAL)));
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'false'}", KEYSPACE, "LeveledCompactionStrategy"));

            final int rowsPerFile = 500;
            final int files = 5;

            cluster.get(1).nodetool("disableautocompaction", KEYSPACE);
            cluster.get(2).nodetool("disableautocompaction", KEYSPACE);
            cluster.get(3).nodetool("disableautocompaction", KEYSPACE);

            IMessageFilters.Filter drop1to3 = cluster.filters().verbs(MUTATION_REQ.id).from(1).to(3).drop();

            int sstablesInitiallyOnNode2 = 0;
            int sstablesInitiallyOnNode3 = 0;

            for (int k = 0; k < 3; k++)
            {
                for (int i = k * rowsPerFile; i < k * rowsPerFile + rowsPerFile; ++i)
                {
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');"),
                                                   ConsistencyLevel.ONE,
                                                   Integer.toString(i));
                }
                cluster.get(1).flush(KEYSPACE);
                cluster.get(2).flush(KEYSPACE);
                sstablesInitiallyOnNode2++;
            }

            drop1to3.off();

            IMessageFilters.Filter drop1to2 = cluster.filters().verbs(MUTATION_REQ.id).from(1).to(2).drop();

            for (int k = 3; k < files; k++)
            {
                for (int i = k * rowsPerFile; i < k * rowsPerFile + rowsPerFile; ++i)
                {
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');"),
                                                   ConsistencyLevel.ONE,
                                                   Integer.toString(i));
                }
                cluster.get(1).flush(KEYSPACE);
                cluster.get(3).flush(KEYSPACE);
                sstablesInitiallyOnNode3++;
            }

            drop1to2.off();

            checkThatNoStreamingOccuredBetweenTheThreeNodes(cluster);

            // Trigger streaming from node 3 and node 2

            long bytesFrom2To1;
            int sstablesFrom2To1;

            long bytesFrom3To1;
            int sstablesFrom3To1;

            int sstablesFrom3To2;
            int sstablesFrom2To3;

            assertThat(sstablesInitiallyOnNode2).isEqualTo(getNumberOfSSTables(cluster, 2));
            assertThat(sstablesInitiallyOnNode3).isEqualTo(getNumberOfSSTables(cluster, 3));
            if (useRepair)
            {
                cluster.get(3).nodetool("repair", "--full");
                cluster.get(2).nodetool("repair", "--full");

                bytesFrom2To1 = checkDataSent(cluster, 2, 1);
                sstablesFrom2To1 = sstablesInitiallyOnNode2;

                bytesFrom3To1 = checkDataSent(cluster, 3, 1) ;
                sstablesFrom3To1 = sstablesInitiallyOnNode3;

                sstablesFrom2To3 = sstablesInitiallyOnNode2;
                sstablesFrom3To2 = sstablesInitiallyOnNode3;
            }
            else
            {
                cluster.get(3).nodetool("rebuild", "--keyspace", KEYSPACE);
                cluster.get(2).nodetool("rebuild", "--keyspace", KEYSPACE);

                bytesFrom2To1 = 0;
                sstablesFrom2To1 = 0;

                bytesFrom3To1 = 0;
                sstablesFrom3To1 = 0;

                sstablesFrom2To3 = sstablesInitiallyOnNode2;
                sstablesFrom3To2 = sstablesInitiallyOnNode3 + sstablesInitiallyOnNode2;
            }

            // Check streaming metrics on node 1
            long bytesFrom1To2 = checkDataSent(cluster, 1, 2);
            long bytesFrom1To3 = checkDataSent(cluster, 1, 3);

            long totalBytesSentFrom1 = bytesFrom1To2 + bytesFrom1To3;

            if (useRepair)
                checkTotalDataSent(cluster, 1, totalBytesSentFrom1, totalBytesSentFrom1, 10);
            else
                checkTotalDataSent(cluster, 1, totalBytesSentFrom1, 0, 0);

            checkDataReceived(cluster, 1, 2, bytesFrom2To1, sstablesFrom2To1);
            checkDataReceived(cluster, 1, 3, bytesFrom3To1, sstablesFrom3To1);
            checkTotalDataReceived(cluster, 1, bytesFrom2To1 + bytesFrom3To1);

            // Check streaming metrics on node 2 and 3
            long bytesFrom2To3 = checkDataSent(cluster, 2, 3);
            long bytesFrom3To2 = checkDataSent(cluster, 3, 2);

            long totalBytesReceivedBy2 = bytesFrom1To2 + bytesFrom3To2;

            checkDataReceived(cluster, 2, 1, bytesFrom1To2, files);
            checkDataReceived(cluster, 2, 3, bytesFrom3To2, sstablesFrom3To2);

            if (useRepair)
                checkTotalDataSent(cluster, 2, bytesFrom2To3 + bytesFrom2To1, bytesFrom2To3 + bytesFrom2To1, sstablesFrom2To3 + sstablesFrom2To1);
            else
                checkTotalDataSent(cluster, 2, bytesFrom2To3, 0, 0);

            checkTotalDataReceived(cluster, 2, totalBytesReceivedBy2);

            long totalBytesReceivedBy3 = bytesFrom1To3 + bytesFrom2To3;

            checkDataReceived(cluster, 3, 1, bytesFrom1To3, files);
            checkDataReceived(cluster, 3, 2, bytesFrom2To3, sstablesFrom2To3);

            if (useRepair)
                checkTotalDataSent(cluster, 3, bytesFrom3To2 + bytesFrom3To1, bytesFrom3To2 + bytesFrom3To1, sstablesFrom3To2 + sstablesFrom3To1);
            else
                checkTotalDataSent(cluster, 3, bytesFrom3To2, 0, 0);

            checkTotalDataReceived(cluster, 3, totalBytesReceivedBy3);
        }
    }

    private void checkThatNoStreamingOccuredBetweenTheThreeNodes(Cluster cluster)
    {
        checkThatNoStreamingOccured(cluster, 1, 2);
        checkThatNoStreamingOccured(cluster, 1, 3);
        checkTotalDataSent(cluster, 1, 0, 0, 0);
        checkTotalDataReceived(cluster, 1, 0);

        checkThatNoStreamingOccured(cluster, 2, 1);
        checkThatNoStreamingOccured(cluster, 2, 3);
        checkTotalDataSent(cluster, 2, 0, 0, 0);
        checkTotalDataReceived(cluster, 2, 0);

        checkThatNoStreamingOccured(cluster, 3, 1);
        checkThatNoStreamingOccured(cluster, 3, 2);
        checkTotalDataSent(cluster, 3, 0, 0, 0);
        checkTotalDataReceived(cluster, 3, 0);
    }

    private void checkThatNoStreamingOccured(Cluster cluster, int node, int peer)
    {
        InetAddressAndPort address = getNodeAddress(cluster, peer);
        cluster.get(node).runOnInstance(() -> {

            StreamingMetrics metrics = StreamingMetrics.get(address);

            assertThat(metrics.incomingBytes.getCount())
                .describedAs("No SSTable should have been streamed so far from node" + node + " to node" + peer)
                .isEqualTo(0);

            assertThat(metrics.outgoingBytes.getCount())
                .describedAs("No SSTable should have been streamed so far from node" + node + " to node" + peer)
                .isEqualTo(0);

            assertThat(metrics.incomingProcessTime.getCount())
                .describedAs("No SSTable should have been streamed so far from node" + node + " to node" + peer)
                .isEqualTo(0);
        });
    }

    private long checkDataSent(Cluster cluster, int node, int peer)
    {
        InetAddressAndPort address = getNodeAddress(cluster, peer);
        return cluster.get(node).callOnInstance(() -> {

            StreamingMetrics metrics = StreamingMetrics.get(address);

            long outgoingBytes = metrics.outgoingBytes.getCount();
            assertThat(outgoingBytes)
                .describedAs("There should be data streamed from node" + node + " to node" + peer)
                .isGreaterThan(0);

            return outgoingBytes;
        });
    }

    private void checkDataReceived(Cluster cluster, int node, int peer, long receivedBytes, int files)
    {
        InetAddressAndPort address = getNodeAddress(cluster, peer);
        cluster.get(node).runOnInstance(() -> {

            StreamingMetrics metrics = StreamingMetrics.get(address);

            long actual = metrics.incomingBytes.getCount();
            assertThat(actual)
                .describedAs("The amount of data received by node" + node + " from node" + peer + " is not the expected one. [expected: " + receivedBytes + ", actual: " + actual + "]")
                .isEqualTo(receivedBytes);

            actual = metrics.incomingProcessTime.getCount();
            // The incomingProcessTime timer is updated for each incoming file. By consequence incomingProcessTime.getCount() should be equals to the number of files received by the node.
            assertThat(actual)
                .describedAs("The amount of files received by node" + node + " from node" + peer + " is not the expected one. [expected: " + files + ", actual: " + actual + "]")
                .isEqualTo(files);

            if (metrics.incomingProcessTime.getCount() != 0)
            {
                assertThat(metrics.incomingProcessTime.getSnapshot().getMedian())
                    .describedAs("The median processing time for data streamed from node"+ peer + " to node" + node + " should be non-0")
                    .isGreaterThan(0);
            }
        });
    }

    private void checkTotalDataSent(Cluster cluster,
                                    int node,
                                    long outgoingBytes,
                                    long outgoingRepairBytes,
                                    long outgoingRepairSSTables)
    {
        cluster.get(node).runOnInstance(() -> {

            long actual = StreamingMetrics.totalOutgoingBytes.getCount();
            assertThat(actual)
                .describedAs("The total amount of data sent by the node" + node + " is not the expected one. [expected: " + outgoingBytes + ", actual: " + actual + "]")
                .isEqualTo(outgoingBytes);

            actual = StreamingMetrics.totalOutgoingRepairBytes.getCount();
            assertThat(actual)
                .describedAs("The total amount of data sent by the node" + node + " for repair is not the expected one. [expected: " + outgoingRepairBytes + ", actual: " + actual + "]")
                .isEqualTo(outgoingRepairBytes);

            actual = StreamingMetrics.totalOutgoingRepairSSTables.getCount();
            assertThat(actual)
                .describedAs("The total amount of SSTables sent by the node" + node + " for repair is not the expected one. [expected: " + outgoingRepairSSTables + ", actual: " + actual + "]")
                .isEqualTo(outgoingRepairSSTables);
        });
    }

    private void checkTotalDataReceived(Cluster cluster, int node, long incomingBytes)
    {
        cluster.get(node).runOnInstance(() -> {

            long actual = StreamingMetrics.totalIncomingBytes.getCount();
            assertThat(actual)
                .describedAs("The total amount of data received by the node" + node + " is not the expected one. [expected: " + incomingBytes + ", actual: " + actual + "]")
                .isEqualTo(incomingBytes);
       });
    }
}
