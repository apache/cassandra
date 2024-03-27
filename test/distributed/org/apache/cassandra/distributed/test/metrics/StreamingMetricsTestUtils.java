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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;

import static org.apache.cassandra.distributed.shared.DistributedTestBase.KEYSPACE;
import static org.apache.cassandra.distributed.shared.DistributedTestBase.withKeyspace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;

public class StreamingMetricsTestUtils
{

    private static InetAddressAndPort getNodeAddress(Cluster cluster, int num)
    {
        InetSocketAddress broadcastAddress = cluster.get(num).broadcastAddress();
        return InetAddressAndPort.getByAddressOverrideDefaults(broadcastAddress.getAddress(),
                                                               broadcastAddress.getPort());
    }

    public static void testMetricsWithStreamingFromTwoNodes(boolean useRepair, Cluster cluster) throws Exception
    {
        assertThat(cluster.size())
        .describedAs("The minimum cluster size to check streaming to a node from 2 peers is 3.")
        .isEqualTo(3);
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

        checkThatNoStreamingOccured(cluster, 3);

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

    /**
     * Test to verify that streaming metrics are updated incrementally
     * - Create 2 node cluster with RF=2
     * - Create 1 sstable with 10MB on node1, while node2 is empty due to message drop
     * - Run repair OR rebuild on node2 to transfer sstable from node1
     * - Collect metrics during streaming and check that at least 3 different values are reported [0, partial1, .., final_size]
     * - Check final transferred size is correct (~10MB bytes)
     *      * @param cluster The cassandra cluster on which the streaming operation will run.
     *      * @param streamingOperation The nodetool command to launch a streaming operation (repair or rebuild)
     *      * @param compressionEnabled Dictates if we should use compression when creating the testing table.
     *      * @throws Exception
     */
    public static void runStreamingOperationAndCheckIncrementalMetrics(Cluster cluster, Callable<Integer> streamingOperation, boolean compressionEnabled, int numSSTables) throws Exception
    {
        assertThat(cluster.size())
        .describedAs("The minimum cluster size to check streaming metrics is 2 nodes.")
        .isEqualTo(2);
        String createTableStatement = String.format("CREATE TABLE %s.cf (k text PRIMARY KEY, c1 text) " +
                                                    "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'} ", KEYSPACE);
        if (!compressionEnabled)
        {
            createTableStatement += "AND compression = {'enabled':'false'}";
        }
        createTableStatement += ';';
        // Create table with compression disabled so we can easily compute the expected final sstable size
        cluster.schemaChange(createTableStatement);

        // each row has 1KB payload
        Random random = new Random(0);
        StringBuilder random1kbString = new StringBuilder();
        for (int i = 0; i < 1024; i++)
            random1kbString.append((char)random.nextInt(127));

        // Drop all messages from node1 to node2 so node2 will be empty
        IMessageFilters.Filter drop1to2 = cluster.filters().verbs(MUTATION_REQ.id).from(1).to(2).drop();
        Set<String> existingSSTables = new HashSet<>();
        for (int i = 0; i < numSSTables; ++i)
        {
            final int totalRows = 10000; // total size: 10K x 1KB ~= 10MB
            for (int j = 0; j < totalRows; ++j)
            {
                // write rows with timestamp 1 to have deterministic transfer size
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.cf (k, c1) VALUES (?, ?) USING TIMESTAMP 1;"),
                                               ConsistencyLevel.ONE,
                                               i + ":" + j,
                                               random1kbString.toString());
            }

            // Flush and compact all nodes to generate a single sstable
            cluster.forEach(node -> {
                node.flush(KEYSPACE);
            });
            compactNewSSTables(cluster, existingSSTables);
            existingSSTables = cluster.get(1)
                                             .callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf")
                                                                                    .getLiveSSTables()
                                                                                    .stream()
                                                                                    .map(SSTableReader::getFilename)
                                                                                    .collect(Collectors.toSet())
                                             );
        }
        // Check that node 1 only has 1 sstable after flush + compaction
        assertThat(getNumberOfSSTables(cluster, 1)).isEqualTo(numSSTables);
        // Node 2 should have 0 sstables since messages from node1 were dropped
        assertThat(getNumberOfSSTables(cluster, 2)).isEqualTo(0);

        // Disable dropping of messages from node1 to node2
        drop1to2.off();

        ExecutorService nodetoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        checkThatNoStreamingOccured(cluster, 2);

        Future<Integer> streamingOperationExecution = nodetoolExecutor.submit(streamingOperation);
        boolean entireSstable = (boolean) cluster.get(1).config().get("stream_entire_sstables");
        checkMetricsUpdatedIncrementally(cluster, streamingOperationExecution, 2, 1, entireSstable);
        streamingOperationExecution.get();
        nodetoolExecutor.shutdown();
    }

    private static void compactNewSSTables(Cluster cluster, Set<String> existingSSTables)
    {
        Set<String> nodeSSTables = cluster.get(1)
                                          .callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf")
                                                                                 .getLiveSSTables()
                                                                                 .stream()
                                                                                 .map(SSTableReader::getFilename)
                                                                                 .collect(Collectors.toSet())
                                          );
        nodeSSTables.removeAll(existingSSTables);
        List<String> compactCommand = new ArrayList<>();
        compactCommand.add("compact");
        compactCommand.add("--user-defined");
        compactCommand.addAll(nodeSSTables);
        String[] commandArray = new String[compactCommand.size()];
        compactCommand.toArray(commandArray);
        cluster.get(1).nodetoolResult(commandArray);
    }

    private static void checkMetricsUpdatedIncrementally(Cluster cluster, Future<Integer> streamingOperationExecution, int dst, int src, boolean entireSstable)
    {
        InetAddressAndPort srcAddress = getNodeAddress(cluster, src);
        InetAddressAndPort dstAddress = getNodeAddress(cluster, dst);

        Set<Long> srcOutgoingTotal = new LinkedHashSet<>();
        Set<Long> srcOutgoingPeer = new LinkedHashSet<>();
        Set<Long> dstIncomingTotal = new LinkedHashSet<>();
        Set<Long> dstIncomingPeer = new LinkedHashSet<>();

        long expectedTransferSize = 0L;
        // Collect outgoing and incoming streaming metrics while streaming is running
        while (!streamingOperationExecution.isDone())
        {
            // Get the expected transfer size. If we are streaming an entire SSTable, all components except TOC.txt are expected to be transferred.
            // if we are not streaming an entire SSTable, then only the Data.db file ends up being transferred.
            expectedTransferSize = entireSstable ? getSstableSizeForEntireFileStreaming(cluster, 1) : getSingleSSTableSize(cluster, src);
            dstIncomingTotal.add(getIncomingBytesTotal(cluster.get(dst)));
            dstIncomingPeer.add(getIncomingBytesFromPeer(cluster.get(dst), srcAddress));
            srcOutgoingTotal.add(getOutgoingBytesTotal(cluster.get(src)));
            srcOutgoingPeer.add(getOutgoingBytesToPeer(cluster.get(src), dstAddress));
        }

        // Check that at least 3 different values were reported for outgoing and incoming bytes [0, partial1, ..., total_size]
        assertThat(dstIncomingTotal).hasSizeGreaterThanOrEqualTo(3);
        assertThat(dstIncomingPeer).hasSizeGreaterThanOrEqualTo(3);
        assertThat(srcOutgoingTotal).hasSizeGreaterThanOrEqualTo(3);
        assertThat(srcOutgoingPeer).hasSizeGreaterThanOrEqualTo(3);

        // Check that final streamed size is correct
        assertThat(getIncomingBytesTotal(cluster.get(dst))).isEqualTo(expectedTransferSize);
        assertThat(getIncomingBytesFromPeer(cluster.get(dst), srcAddress)).isEqualTo(expectedTransferSize);
        assertThat(getOutgoingBytesToPeer(cluster.get(src), dstAddress)).isEqualTo(expectedTransferSize);
        assertThat(getOutgoingBytesTotal(cluster.get(src))).isEqualTo(expectedTransferSize);
    }

    private static Long getIncomingBytesFromPeer(IInvokableInstance instance, InetAddressAndPort peerAddress)
    {
        return instance.callOnInstance(() -> StreamingMetrics.get(peerAddress).incomingBytes.getCount());
    }

    private static Long getOutgoingBytesToPeer(IInvokableInstance instance, InetAddressAndPort peerAddress)
    {
        return instance.callOnInstance(() -> StreamingMetrics.get(peerAddress).outgoingBytes.getCount());
    }

    private static Long getIncomingBytesTotal(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> StreamingMetrics.totalIncomingBytes.getCount());
    }

    private static Long getOutgoingBytesTotal(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> StreamingMetrics.totalOutgoingBytes.getCount());
    }

    private static int getNumberOfSSTables(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf").getLiveSSTables().size());
    }

    private static long getSingleSSTableSize(Cluster cluster, int node)
    {
        assert cluster.get(node).callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf").getLiveSSTables().size()) == 1;
        return cluster.get(node).callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf").getLiveSSTables().iterator().next().onDiskLength());
    }

    private static long getSstableSizeForEntireFileStreaming(Cluster cluster, int node)
    {
        return cluster.get(node)
                      .callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf")
                                                             .getLiveSSTables()
                                                             .stream()
                                                             .map(sstableReader -> sstableReader.getComponents()
                                                                                                .stream()
                                                                                                .map(c -> new File(sstableReader.descriptor.filenameFor(c)))
                                                                                                .map(f -> f.name().endsWith("TOC.txt") ? 0L : f.length()) // When streaming an entire SSTable, the TOC.txt file is not transferred.
                                                                                                .mapToLong(Long::longValue)
                                                                                                .sum()
                                                             )
                                                             .mapToLong(Long::longValue)
                                                             .sum()

                      );
    }

    public static void testMetricsWithStreamingToTwoNodes(boolean useRepair, Cluster cluster) throws Exception
    {
        assertThat(cluster.size())
        .describedAs("The minimum cluster size to test metrics streaming from one node to two peers is 3")
        .isEqualTo(3);
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

        checkThatNoStreamingOccured(cluster, 3);

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

    private static void checkThatNoStreamingOccured(Cluster cluster, int nodeCount)
    {
        for (int src = 1; src <= nodeCount; src++)
        {
            for (int dst = src + 1; dst <= nodeCount; dst++)
            {
                checkThatNoStreamingOccured(cluster, src, dst);
            }
            checkTotalDataSent(cluster, src, 0, 0, 0);
            checkTotalDataReceived(cluster, src, 0);
        }
    }

    private static void checkThatNoStreamingOccured(Cluster cluster, int node, int peer)
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

    private static long checkDataSent(Cluster cluster, int node, int peer)
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

    private static void checkDataReceived(Cluster cluster, int node, int peer, long receivedBytes, int files)
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

    private static void checkTotalDataSent(Cluster cluster,
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

    private static void checkTotalDataReceived(Cluster cluster, int node, long incomingBytes)
    {
        cluster.get(node).runOnInstance(() -> {

            long actual = StreamingMetrics.totalIncomingBytes.getCount();
            assertThat(actual)
            .describedAs("The total amount of data received by the node" + node + " is not the expected one. [expected: " + incomingBytes + ", actual: " + actual + "]")
            .isEqualTo(incomingBytes);
        });
    }
}
