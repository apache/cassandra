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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.utils.Pair;

import static java.util.stream.Collectors.toList;

public abstract class AbstractNetstatsStreaming extends TestBaseImpl
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractNetstatsStreaming.class);

    protected ExecutorService executorService;

    @Before
    public void setup()
    {
        executorService = Executors.newCachedThreadPool();
    }

    @After
    public void teardown() throws Exception
    {
        try
        {
            executorService.shutdownNow();

            if (!executorService.isShutdown())
            {
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES))
                {
                    throw new IllegalStateException("Unable to shutdown executor for invoking netstat commands.");
                }
            }
        }
        finally
        {
            executorService = null;
        }
    }

    protected void changeReplicationFactor()
    {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect())
        {
            s.execute("ALTER KEYSPACE netstats_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };");
        }
    }

    protected void createTable(Cluster cluster, int replicationFactor, boolean compressionEnabled)
    {
        // replication factor is 1
        cluster.schemaChange("CREATE KEYSPACE netstats_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");

        if (compressionEnabled)
        {
            cluster.schemaChange("CREATE TABLE netstats_test.test_table (id uuid primary key) WITH compression = {'enabled':'true', 'class': 'LZ4Compressor'};");
        }
        else
        {
            cluster.schemaChange("CREATE TABLE netstats_test.test_table (id uuid primary key) WITH compression = {'enabled':'false'};");
        }
    }

    protected void populateData(boolean forCompressedTest)
    {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect("netstats_test"))
        {
            int records = forCompressedTest ? 100_000 : 70_000;

            for (int i = 0; i < records; i++)
            {
                s.execute("INSERT INTO test_table (id) VALUES (" + UUID.randomUUID() + ')');
            }
        }
    }

    protected static class NetstatsOutputParser
    {
        public static List<Pair<ReceivingStastistics, SendingStatistics>> parse(final NetstatResults results)
        {
            final Set<String> outputs = new LinkedHashSet<>();

            results.netstatOutputs.stream()
                                  .map(NodeToolResult::getStdout)
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
                                                           .filter(line -> Stream.of("Mode:", "Read", "Attempted", "Mismatch", "Pool", "Large", "Small", "Gossip").noneMatch(line::startsWith))
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
                    SendingStatistics.SendingHeader header1 = sendingStatistics.get(i).sendingHeader;
                    SendingStatistics.SendingHeader header2 = sendingStatistics.get(i + 1).sendingHeader;

                    if (header1 != null && header2 != null)
                    {
                        Assert.assertTrue(header1.compareTo(header2) <= 0);
                    }
                }
            }

            for (SendingStatistics sending : sendingStatistics)
            {
                if (sending.sendingHeader != null)
                {
                    Assert.assertEquals(sending.sendingHeader.bytesTotalSoFar, (long) sending.sendingSSTable.stream().map(table -> table.bytesSent).reduce(Long::sum).orElse(0L));
                    Assert.assertTrue(sending.sendingHeader.bytesTotal >= sending.sendingSSTable.stream().map(table -> table.bytesInTotal).reduce(Long::sum).orElse(0L));

                    if (sending.sendingHeader.bytesTotalSoFar != 0)
                    {
                        double progress = (double) sending.sendingSSTable.stream().map(table -> table.bytesSent).reduce(Long::sum).orElse(0L) / (double) sending.sendingHeader.bytesTotal;

                        Assert.assertTrue((int) sending.sendingHeader.progressBytes >= (int) (progress * 100));

                        Assert.assertTrue((double) sending.sendingHeader.bytesTotal >= (double) sending.sendingSSTable.stream().map(table -> table.bytesInTotal).reduce(Long::sum).orElse(0L));
                    }
                }
            }

            List<ReceivingStastistics> receivingStastistics = result.stream().map(pair -> pair.left).collect(toList());

            for (ReceivingStastistics receiving : receivingStastistics)
            {
                if (receiving.receivingHeader != null)
                {
                    Assert.assertTrue(receiving.receivingHeader.bytesTotal >= receiving.receivingTables.stream().map(table -> table.receivedSoFar).reduce(Long::sum).orElse(0L));
                    Assert.assertEquals(receiving.receivingHeader.bytesTotalSoFar, (long) receiving.receivingTables.stream().map(table -> table.receivedSoFar).reduce(Long::sum).orElse(0L));
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

    protected static final class NetstatResults
    {
        private final List<NodeToolResult> netstatOutputs = new ArrayList<>();

        public void add(NodeToolResult result)
        {
            netstatOutputs.add(result);
        }

        public void assertSuccessful()
        {
            for (final NodeToolResult result : netstatOutputs)
            {
                Assert.assertEquals(result.getRc(), 0);
                Assert.assertTrue(result.getStderr().isEmpty());
            }
        }
    }

    protected static class NetstatsCallable implements Callable<NetstatResults>
    {
        private final IInvokableInstance node;

        public NetstatsCallable(final IInvokableInstance node)
        {
            this.node = node;
        }

        public NetstatResults call() throws Exception
        {
            final NetstatResults results = new NetstatResults();

            boolean sawAnyStreamingOutput = false;

            long mark = 0;
            while (true)
            {
                try
                {
                    final NodeToolResult result = node.nodetoolResult(false, "netstats");

                    logger.info(node.broadcastAddress().toString() + ' ' + result.getStdout());

                    if (!sawAnyStreamingOutput)
                    {
                        if (result.getStdout().contains("Receiving") || result.getStdout().contains("Sending"))
                        {
                            sawAnyStreamingOutput = true;
                        }
                        else
                        {
                            // there is a race condition that streaming starts/stops between calls to netstats
                            // to detect this, check to see if the node has completed a stream
                            // expected log: [Stream (.*)?] All sessions completed
                            LogResult<List<String>> logs = node.logs().grep(mark, "\\[Stream .*\\] All sessions completed");
                            mark = logs.getMark();
                            if (!logs.getResult().isEmpty())
                            {
                                // race condition detected...
                                logger.info("Test race condition detected where streaming started/stopped between calls to netstats");
                                sawAnyStreamingOutput = true;
                            }
                        }
                    }

                    if (sawAnyStreamingOutput && (!result.getStdout().contains("Receiving") && !result.getStdout().contains("Sending")))
                    {
                        break;
                    }

                    results.add(result);

                    Thread.sleep(500);
                }
                catch (final Exception ex)
                {
                    logger.error(ex.getMessage());
                    Thread.sleep(500);
                }
            }

            return results;
        }
    }
}
