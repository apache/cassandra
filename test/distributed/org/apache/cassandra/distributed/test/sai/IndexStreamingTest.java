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
package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class IndexStreamingTest extends TestBaseImpl
{
    // streaming sends events every 65k, so need to make sure that the files are larger than this to hit
    // all cases of the vtable - hence we add a big enough blob column
    private static final ByteBuffer BLOB = ByteBuffer.wrap(new byte[1 << 16]);

    private static final int NODES = 2;
    private static final int SSTABLE_COUNT = 10;
    private static final int EXPECTED_SCENARIOS = 8;
    private static final AtomicInteger SCENARIOS = new AtomicInteger();
    private static Cluster CLUSTER;

    private String keyspace;

    private static int sstableStreamingComponentsCount()
    {
        return (int) DatabaseDescriptor.getSelectedSSTableFormat()
                                       .allComponents()
                                       .stream()
                                       .filter(c -> c.type.streamable)
                                       .count() - 1;  // -1 because we don't include the compression component
    }

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public boolean isLiteral;
    @Parameterized.Parameter(1)
    public boolean isZeroCopyStreaming;
    @Parameterized.Parameter(2)
    public boolean isWide;

    @Parameterized.Parameters(name = "isLiteral={0}, isZeroCopyStreaming={1}, isWide={2}")
    public static List<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (boolean isLiteral : BOOLEANS)
            for (boolean isZeroCopyStreaming : BOOLEANS)
                for (boolean isWide : BOOLEANS)
                    result.add(new Object[]{ isLiteral, isZeroCopyStreaming, isWide });
        assertThat(result).hasSize(EXPECTED_SCENARIOS);
        return result;
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        DatabaseDescriptor.clientInitialization();

        CLUSTER = Cluster.build(NODES)
                         .withConfig(c -> c.with(Feature.values())
                                           .set("streaming_slow_events_log_timeout", "0s"))
                         .start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void setupScenario()
    {
        keyspace = String.format("index_streaming_%s_%s_%s",
                                 isLiteral ? "literal" : "numeric",
                                 isZeroCopyStreaming ? "zerocopy" : "partial",
                                 isWide ? "wide" : "skinny");
        CLUSTER.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}",
                                           keyspace, NODES));
        CLUSTER.schemaChange(String.format(isWide
                                           ? "CREATE TABLE %s.test (pk int, ck int , literal text, numeric int, b blob, PRIMARY KEY(pk, ck)) WITH compression = { 'enabled' : false };"
                                           : "CREATE TABLE %s.test (pk int PRIMARY KEY , literal text, numeric int, b blob) WITH compression = { 'enabled' : false };",
                                           keyspace));
        CLUSTER.schemaChange(String.format("CREATE INDEX ON %s.test(literal) USING 'sai';", keyspace));
        CLUSTER.schemaChange(String.format("CREATE INDEX ON %s.test(numeric) USING 'sai';", keyspace));
        CLUSTER.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace).asserts().success());

        boolean zeroCopy = isZeroCopyStreaming;
        CLUSTER.forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setStreamEntireSSTables(zeroCopy)));

        long skewNanos = TimeUnit.MINUTES.toNanos(2L * SCENARIOS.getAndIncrement());
        CLUSTER.forEach(i -> i.runOnInstance(() -> NoSpamLogger.unsafeSetClock(() -> Clock.Global.nanoTime() + skewNanos)));
    }

    @Test
    public void testIndexComponentStreaming()
    {
        int numSSTableComponents = isWide ? V1OnDiskFormat.WIDE_PER_SSTABLE_COMPONENTS.size() : V1OnDiskFormat.SKINNY_PER_SSTABLE_COMPONENTS.size();
        int numIndexComponents = isLiteral ? V1OnDiskFormat.LITERAL_COMPONENTS.size() : V1OnDiskFormat.NUMERIC_COMPONENTS.size();
        int numComponents = sstableStreamingComponentsCount() + numSSTableComponents + numIndexComponents + 1;

        IInvokableInstance first = CLUSTER.get(1);
        IInvokableInstance second = CLUSTER.get(2);
        long expectedFiles = isZeroCopyStreaming ? (long) SSTABLE_COUNT * numComponents : SSTABLE_COUNT;

        for (int i = 0; i < SSTABLE_COUNT; i++)
        {
            if (isWide)
            {
                String insertTemplate = "INSERT INTO " + keyspace + ".test(pk, ck, " + (isLiteral ? "literal" : "numeric") + ", b) VALUES (?, ?, ?, ?)";
                first.executeInternal(insertTemplate, i, i, isLiteral ? "v" + i : Integer.valueOf(i), BLOB);
            }
            else
            {
                String insertTemplate = "INSERT INTO " + keyspace + ".test(pk, " + (isLiteral ? "literal" : "numeric") + ", b) VALUES (?, ?, ?)";
                first.executeInternal(insertTemplate, i, isLiteral ? "v" + i : Integer.valueOf(i), BLOB);
            }
            first.flush(keyspace);
        }

        // the previous scenarios have left their sessions in the vtable, so only the new session can be asserted on
        Set<String> previousSessionsOnFirst = streamingSessionIds(first);
        Set<String> previousSessionsOnSecond = streamingSessionIds(second);
        long firstMark = first.logs().mark();
        long secondMark = second.logs().mark();

        second.nodetoolResult("rebuild", "--keyspace", keyspace).asserts().success();

        Row row = newStreamingSession(first, previousSessionsOnFirst);
        QueryResultUtil.assertThat(row)
                       .isEqualTo("peers", Collections.singletonList(second.broadcastAddress().toString()))
                       .isEqualTo("follower", true)
                       .isEqualTo("operation", "Rebuild")
                       .isEqualTo("status", "success")
                       .isEqualTo("progress_percentage", 100.0F)
                       .isEqualTo("success_message", null).isEqualTo("failure_cause", null)
                       .isEqualTo("files_sent", expectedFiles)
                       .columnsEqualTo("files_sent", "files_to_send")
                       .columnsEqualTo("bytes_sent", "bytes_to_send")
                       .isEqualTo("files_received", 0L)
                       .columnsEqualTo("files_received", "files_to_receive", "bytes_received", "bytes_to_receive");
        long totalBytes = row.getLong("bytes_sent");
        assertThat(totalBytes).isGreaterThan(0);

        QueryResultUtil.assertThat(newStreamingSession(second, previousSessionsOnSecond))
                       .isEqualTo("peers", Collections.singletonList(first.broadcastAddress().toString()))
                       .isEqualTo("follower", false)
                       .isEqualTo("operation", "Rebuild")
                       .isEqualTo("status", "success")
                       .isEqualTo("progress_percentage", 100.0F)
                       .isEqualTo("success_message", null).isEqualTo("failure_cause", null)
                       .columnsEqualTo("files_to_receive", "files_received").isEqualTo("files_received", expectedFiles)
                       .columnsEqualTo("bytes_to_receive", "bytes_received").isEqualTo("bytes_received", totalBytes)
                       .columnsEqualTo("files_sent", "files_to_send", "bytes_sent", "bytes_to_send").isEqualTo("files_sent", 0L);

        assertSlowEventsLogged(first, firstMark);
        assertSlowEventsLogged(second, secondMark);

        for (int i = 0; i < SSTABLE_COUNT; i++)
        {
            Object[][] rs = isLiteral ? second.executeInternal("select pk from " + keyspace + ".test where literal = ?", "v" + i)
                                      : second.executeInternal("select pk from " + keyspace + ".test where numeric = ?", i);
            assertThat(rs.length).isEqualTo(1);
            assertThat(rs[0][0]).isEqualTo(i);
        }
    }

    private static void assertSlowEventsLogged(IInvokableInstance instance, long mark)
    {
        Assertions.assertThat(instance.logs().grep(mark, "Handling streaming events took longer than").getResult())
                  .describedAs("Unable to find slow log for node%d", instance.config().num())
                  .isNotEmpty();
    }

    private static Set<String> streamingSessionIds(IInvokableInstance instance)
    {
        Set<String> ids = new HashSet<>();
        SimpleQueryResult qr = instance.executeInternalWithResult("SELECT id FROM system_views.streaming");
        while (qr.hasNext())
            ids.add(sessionId(qr.next()));
        return ids;
    }

    private static Row newStreamingSession(IInvokableInstance instance, Set<String> previousIds)
    {
        SimpleQueryResult qr = instance.executeInternalWithResult("SELECT * FROM system_views.streaming");
        String txt = QueryResultUtil.expand(qr);
        qr.reset();

        List<Row> newRows = new ArrayList<>();
        while (qr.hasNext())
        {
            Row row = qr.next();
            if (!previousIds.contains(sessionId(row)))
                newRows.add(row.copy());
        }

        assertThat(newRows).describedAs("Expected a single new streaming session on node%d, found rows\n%s",
                                        instance.config().num(), txt)
                           .hasSize(1);
        return newRows.get(0);
    }

    private static String sessionId(Row row)
    {
        Object id = row.get("id");
        return String.valueOf(id);
    }
}
