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

package org.apache.cassandra.distributed.mock.nodetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.InstanceAwareNodetool;
import org.apache.cassandra.distributed.shared.AbstractBuilder;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class NetstatsTest extends TestBaseImpl
{

    private static final Logger logger = LoggerFactory.getLogger(NetstatsTest.class);

    @Test
    public void testNetstatsStreamingProgress() throws Exception
    {
        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        try (final InstanceAwareNodetool nodetool = new InstanceAwareNodetool(getCluster()))
        {
            final Cluster cluster = nodetool.getCluster();
            final IInvokableInstance node1 = cluster.get(1);
            final IInvokableInstance node2 = cluster.get(2);

            createTable(cluster);

            node1.flush("netstats_test");
            node2.flush("netstats_test");

            disableCompaction(node1);
            disableCompaction(node2);

            populateData();

            // change RF from 1 to 2 so we need to repair it, repairing will causes streaming shown in netstats
            changeReplicationFactor();

            final Future<NetstatResults> resultsFuture1 = executorService.submit(new NetstatsCallable(nodetool, node1));
            final Future<NetstatResults> resultsFuture2 = executorService.submit(new NetstatsCallable(nodetool, node2));

            final IIsolatedExecutor.CallableNoExcept<?> repairCallable = node1.asyncCallsOnInstance(() -> {
                try
                {
                    StorageService.instance.repair("netstats_test", Collections.emptyMap(), Collections.emptyList()).right.get();

                    return null;
                }
                catch (final Exception ex)
                {
                    throw new RuntimeException(ex);
                }
            });

            repairCallable.call();

            logger.info("Waiting for netstats callables to stop ...");

            final NetstatResults results1 = resultsFuture1.get(1, TimeUnit.MINUTES);
            final NetstatResults results2 = resultsFuture2.get(1, TimeUnit.MINUTES);

            results1.assertSuccessful();
            results2.assertSuccessful();

            NetstatsOutputParser.validate(NetstatsOutputParser.parse(results1));
            NetstatsOutputParser.validate(NetstatsOutputParser.parse(results2));
        }
        catch (final Throwable t)
        {
            t.printStackTrace();
            Assert.fail("Test has failed: " + t.getMessage());
        }
        finally
        {
            logger.info("Shutting down executor service");

            executorService.shutdownNow();

            if (!executorService.isShutdown())
            {
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES))
                {
                    throw new IllegalStateException("Unable to shutdown executor for invoking netstat commands.");
                }
            }
        }
    }

    // helpers

    private static class NetstatsOutputParser
    {
        public static List<Pair<ReceivingStastistics, SendingStatistics>> parse(final NetstatResults results)
        {
            final Set<String> outputs = new LinkedHashSet<>();

            results.netstatOutputs.stream()
                                  .map(result -> result[0])
                                  .filter(output -> !output.contains("Not sending any streams"))
                                  .filter(output -> output.contains("Receiving") || output.contains("Sending"))
                                  .forEach(outputs::add);

            final List<Pair<ReceivingStastistics, SendingStatistics>> parsed = new ArrayList<>();

            for (final String output : outputs)
            {
                boolean processingReceiving = false;
                boolean processingSending = false;

                final ReceivingStastistics receivingStastistics = new ReceivingStastistics();
                final SendingStatistics sendingStatistics = new SendingStatistics();

                final List<String> sanitisedOutput = Stream.of(output.split("\n"))
                                                           .map(String::trim)
                                                           .filter(line -> !line.isEmpty())
                                                           // sometimes logs are mangled into output
                                                           .filter(line -> Stream.of("DEBUG", "INFO", "ERROR", "WARN").noneMatch(line::contains))
                                                           .filter(line -> Stream.of("Read", "Attempted", "Mismatch", "Pool", "Large", "Small", "Gossip").noneMatch(line::startsWith))
                                                           .collect(toList());

                for (final String outputLine : sanitisedOutput)
                {
                    if (outputLine.startsWith("Receiving"))
                    {
                        processingReceiving = true;
                        processingSending = false;

                        receivingStastistics.parseHeader(outputLine);
                    }
                    else if (outputLine.startsWith("Sending"))
                    {
                        processingSending = true;
                        processingReceiving = false;

                        sendingStatistics.parseHeader(outputLine);
                    }
                    else if (processingReceiving)
                    {
                        receivingStastistics.parseTable(outputLine);
                    }
                    else if (processingSending)
                    {
                        sendingStatistics.parseTable(outputLine);
                    }
                }

                parsed.add(Pair.create(receivingStastistics, sendingStatistics));
            }

            return parsed;
        }

        public static void validate(List<Pair<ReceivingStastistics, SendingStatistics>> result)
        {
            List<SendingStatistics> sendingStatistics = result.stream().map(pair -> pair.right).collect(toList());

            if (sendingStatistics.size() >= 2)
            {
                for (int i = 0; i < sendingStatistics.size() - 1; i++)
                {
                    Assert.assertTrue(sendingStatistics.get(i).sendingHeader.compareTo(sendingStatistics.get(i + 1).sendingHeader) <= 0);
                }
            }

            for (SendingStatistics sending : sendingStatistics)
            {
                if (sending.sendingHeader != null)
                {
                    Assert.assertEquals(sending.sendingHeader.bytesTotalSoFar, (long) sending.sendingSSTable.stream().map(table -> table.bytesSent).reduce(Long::sum).orElseGet(() -> 0L));
                    Assert.assertTrue(sending.sendingHeader.bytesTotal >= sending.sendingSSTable.stream().map(table -> table.bytesInTotal).reduce(Long::sum).orElseGet(() -> 0L));

                    if (sending.sendingHeader.bytesTotalSoFar != 0)
                    {
                        double progress = (double) sending.sendingSSTable.stream().map(table -> table.bytesSent).reduce(Long::sum).orElseGet(() -> 0L) / (double) sending.sendingHeader.bytesTotal;

                        Assert.assertEquals((int) sending.sendingHeader.progressBytes, (int) (progress * 100));

                        Assert.assertTrue((double) sending.sendingHeader.bytesTotal > (double) sending.sendingSSTable.stream().map(table -> table.bytesInTotal).reduce(Long::sum).orElseGet(() -> 0L));
                    }
                }
            }

            List<ReceivingStastistics> receivingStastistics = result.stream().map(pair -> pair.left).collect(toList());

            for (ReceivingStastistics receiving : receivingStastistics)
            {
                if (receiving.receivingHeader != null)
                {
                    Assert.assertTrue(receiving.receivingHeader.bytesTotal >= receiving.receivingTables.stream().map(table -> table.receivedSoFar).reduce(Long::sum).orElseGet(() -> 0L));
                    Assert.assertEquals(receiving.receivingHeader.bytesTotalSoFar, (long) receiving.receivingTables.stream().map(table -> table.receivedSoFar).reduce(Long::sum).orElseGet(() -> 0L));
                }
            }
        }

        public static class ReceivingStastistics
        {
            public ReceivingHeader receivingHeader;
            public List<ReceivingTable> receivingTables = new ArrayList<>();

            public void parseHeader(String header)
            {
                receivingHeader = ReceivingHeader.parseHeader(header);
            }

            public void parseTable(String table)
            {
                receivingTables.add(ReceivingTable.parseTable(table));
            }

            public String toString()
            {
                return "ReceivingStastistics{" +
                       "receivingHeader=" + receivingHeader +
                       ", receivingTables=" + receivingTables +
                       '}';
            }

            public static class ReceivingHeader
            {
                private static final Pattern receivingHeaderPattern = Pattern.compile(
                "Receiving (.*) files, (.*) bytes total. Already received (.*) files \\((.*)%\\), (.*) bytes total \\((.*)%\\)"
                );

                int totalReceiving = 0;
                long bytesTotal = 0;
                int alreadyReceived = 0;
                double progressFiles = 0.0f;
                long bytesTotalSoFar = 0;
                double progressBytes = 0.0f;

                public static ReceivingHeader parseHeader(String header)
                {
                    final Matcher matcher = receivingHeaderPattern.matcher(header);

                    if (matcher.matches())
                    {
                        final ReceivingHeader receivingHeader = new ReceivingHeader();

                        receivingHeader.totalReceiving = Integer.parseInt(matcher.group(1));
                        receivingHeader.bytesTotal = Long.parseLong(matcher.group(2));
                        receivingHeader.alreadyReceived = Integer.parseInt(matcher.group(3));
                        receivingHeader.progressFiles = Double.parseDouble(matcher.group(4));
                        receivingHeader.bytesTotalSoFar = Long.parseLong(matcher.group(5));
                        receivingHeader.progressBytes = Double.parseDouble(matcher.group(6));

                        return receivingHeader;
                    }

                    throw new IllegalStateException("Header does not match - " + header);
                }

                public String toString()
                {
                    return "ReceivingHeader{" +
                           "totalReceiving=" + totalReceiving +
                           ", bytesTotal=" + bytesTotal +
                           ", alreadyReceived=" + alreadyReceived +
                           ", progressFiles=" + progressFiles +
                           ", bytesTotalSoFar=" + bytesTotalSoFar +
                           ", progressBytes=" + progressBytes +
                           '}';
                }
            }

            public static class ReceivingTable
            {
                long receivedSoFar = 0;
                long toReceive = 0;
                double progress = 0.0;

                private static final Pattern recievingFilePattern = Pattern.compile("(.*) (.*)/(.*) bytes \\((.*)%\\) received from (.*)");

                public static ReceivingTable parseTable(String table)
                {
                    final Matcher matcher = recievingFilePattern.matcher(table);

                    if (matcher.matches())
                    {
                        final ReceivingTable receivingTable = new ReceivingTable();

                        receivingTable.receivedSoFar = Long.parseLong(matcher.group(2));
                        receivingTable.toReceive = Long.parseLong(matcher.group(3));
                        receivingTable.progress = Double.parseDouble(matcher.group(4));

                        return receivingTable;
                    }

                    throw new IllegalStateException("Table line does not match - " + table);
                }

                public String toString()
                {
                    return "ReceivingTable{" +
                           "receivedSoFar=" + receivedSoFar +
                           ", toReceive=" + toReceive +
                           ", progress=" + progress +
                           '}';
                }
            }
        }

        public static class SendingStatistics
        {
            public SendingHeader sendingHeader;
            public List<SendingSSTable> sendingSSTable = new ArrayList<>();

            public void parseHeader(String outputLine)
            {
                this.sendingHeader = SendingHeader.parseHeader(outputLine);
            }

            public void parseTable(String table)
            {
                sendingSSTable.add(SendingSSTable.parseTable(table));
            }

            public String toString()
            {
                return "SendingStatistics{" +
                       "sendingHeader=" + sendingHeader +
                       ", sendingSSTable=" + sendingSSTable +
                       '}';
            }

            public static class SendingHeader implements Comparable<SendingHeader>
            {
                private static final Pattern sendingHeaderPattern = Pattern.compile(
                "Sending (.*) files, (.*) bytes total. Already sent (.*) files \\((.*)%\\), (.*) bytes total \\((.*)%\\)"
                );

                int totalSending = 0;
                long bytesTotal = 0;
                int alreadySent = 0;
                double progressFiles = 0.0f;
                long bytesTotalSoFar = 0;
                double progressBytes = 0.0f;

                public static SendingHeader parseHeader(String header)
                {
                    final Matcher matcher = sendingHeaderPattern.matcher(header);

                    if (matcher.matches())
                    {
                        final SendingHeader sendingHeader = new SendingHeader();

                        sendingHeader.totalSending = Integer.parseInt(matcher.group(1));
                        sendingHeader.bytesTotal = Long.parseLong(matcher.group(2));
                        sendingHeader.alreadySent = Integer.parseInt(matcher.group(3));
                        sendingHeader.progressFiles = Double.parseDouble(matcher.group(4));
                        sendingHeader.bytesTotalSoFar = Long.parseLong(matcher.group(5));
                        sendingHeader.progressBytes = Double.parseDouble(matcher.group(6));

                        return sendingHeader;
                    }

                    throw new IllegalStateException("Header does not match - " + header);
                }

                public String toString()
                {
                    return "SendingHeader{" +
                           "totalSending=" + totalSending +
                           ", bytesTotal=" + bytesTotal +
                           ", alreadySent=" + alreadySent +
                           ", progressFiles=" + progressFiles +
                           ", bytesTotalSoFar=" + bytesTotalSoFar +
                           ", progressBytes=" + progressBytes +
                           '}';
                }


                public int compareTo(SendingHeader o)
                {
                    // progress on bytes has to be strictly lower,
                    // even alreadySent and progressFiles and progressBytes are same,
                    // bytesTotalSoFar has to be lower, bigger or same

                    if (alreadySent <= o.alreadySent
                        && progressFiles <= o.progressFiles
                        && bytesTotalSoFar <= o.bytesTotalSoFar
                        && progressBytes <= o.progressBytes)
                    {
                        return -1;
                    }
                    else if (alreadySent == o.alreadySent
                             && progressFiles == o.progressFiles
                             && bytesTotalSoFar == o.bytesTotalSoFar
                             && progressBytes == o.progressBytes)
                    {
                        return 0;
                    }
                    else if (alreadySent >= o.alreadySent
                             && progressFiles >= o.progressFiles
                             && bytesTotalSoFar > o.bytesTotalSoFar
                             && progressBytes >= o.progressBytes)
                    {
                        return 1;
                    }
                    else
                    {
                        throw new IllegalStateException(String.format("Could not compare arguments %s and %s", this, o));
                    }
                }
            }

            public static class SendingSSTable
            {
                private static final Pattern sendingFilePattern = Pattern.compile("(.*) (.*)/(.*) bytes \\((.*)%\\) sent to (.*)");

                long bytesSent = 0;
                long bytesInTotal = 0;
                double progress = 0.0f;

                public static SendingSSTable parseTable(String table)
                {
                    final Matcher matcher = sendingFilePattern.matcher(table);

                    if (matcher.matches())
                    {
                        final SendingSSTable sendingSSTable = new SendingSSTable();

                        sendingSSTable.bytesSent = Long.parseLong(matcher.group(2));
                        sendingSSTable.bytesInTotal = Long.parseLong(matcher.group(3));
                        sendingSSTable.progress = Double.parseDouble(matcher.group(4));

                        return sendingSSTable;
                    }

                    throw new IllegalStateException("Table does not match - " + table);
                }

                public String toString()
                {
                    return "SendingSSTable{" +
                           "bytesSent=" + bytesSent +
                           ", bytesInTotal=" + bytesInTotal +
                           ", progress=" + progress +
                           '}';
                }
            }
        }
    }

    private static final class NetstatResults
    {
        private final List<String[]> netstatOutputs = new ArrayList<>();

        public void add(String[] result)
        {
            netstatOutputs.add(result);
        }

        public void assertSuccessful()
        {
            for (final String[] result : netstatOutputs)
            {
                InstanceAwareNodetool.assertSuccessful(result);
            }
        }
    }

    private static class NetstatsCallable implements Callable<NetstatResults>
    {
        private final IInvokableInstance node;
        private final InstanceAwareNodetool nodetool;

        public NetstatsCallable(final InstanceAwareNodetool nodetool,
                                final IInvokableInstance node)
        {
            this.nodetool = nodetool;
            this.node = node;
        }

        public NetstatResults call() throws Exception
        {
            final NetstatResults results = new NetstatResults();

            boolean sawAnyStreamingOutput = false;

            while (true)
            {
                String[] result = nodetool.execute(node, "netstats");

                if (!sawAnyStreamingOutput)
                {
                    if (result[0].contains("Receiving") || result[0].contains("Sending"))
                    {
                        sawAnyStreamingOutput = true;
                    }
                }

                if (sawAnyStreamingOutput && (!result[0].contains("Receiving") && !result[0].contains("Sending")))
                {
                    break;
                }

                results.add(result);

                Thread.currentThread().sleep(1000);
            }

            return results;
        }
    }

    private void changeReplicationFactor()
    {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect())
        {
            s.execute("ALTER KEYSPACE netstats_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
        }
    }

    private void createTable(Cluster cluster)
    {
        // replication factor is 1
        cluster.schemaChange("CREATE KEYSPACE netstats_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        cluster.schemaChange("CREATE TABLE netstats_test.test_table (id uuid primary key);");
    }

    private void populateData()
    {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect("netstats_test"))
        {

            for (int i = 0; i < 100000; i++)
            {
                s.execute("INSERT INTO test_table (id) VALUES (" + UUID.randomUUID() + ")");
            }
        }
    }

    private void disableCompaction(IInvokableInstance invokableInstance)
    {
        invokableInstance.runOnInstance(() -> {
            try
            {
                StorageService.instance.disableAutoCompaction("netstats_test");
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        });
    }

    private static AbstractBuilder<IInvokableInstance, Cluster, Cluster.Builder> getCluster()
    {
        return Cluster.build()
                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                      .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("stream_throughput_outbound_megabits_per_sec", 1)
                                                  .set("compaction_throughput_mb_per_sec", 1)
                                                  .set("stream_entire_sstables", false));
    }
}