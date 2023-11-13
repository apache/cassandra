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
import java.util.List;

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
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class IndexStreamingTest extends TestBaseImpl
{
    // streaming sends events every 65k, so need to make sure that the files are larger than this to hit
    // all cases of the vtable - hence we add a big enough blob column
    private static final ByteBuffer BLOB = ByteBuffer.wrap(new byte[1 << 16]);

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

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

    @Parameterized.Parameters(name = "isLiteral={0}, isZeroCopyStreaming={1}")
    public static List<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (boolean isLiteral : BOOLEANS)
            for (boolean isZeroCopyStreaming : BOOLEANS)
                for (boolean isWide : BOOLEANS)
                    result.add(new Object[]{ isLiteral, isZeroCopyStreaming, isWide });
        return result;
    }

    @Test
    public void testIndexComponentStreaming() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(Feature.values())
                                                             .set("stream_entire_sstables", isZeroCopyStreaming)
                                                             .set("streaming_slow_events_log_timeout", "0s"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace(
                isWide
                ? "CREATE TABLE %s.test (pk int, ck int , literal text, numeric int, b blob, PRIMARY KEY(pk, ck)) WITH compression = { 'enabled' : false };"
                : "CREATE TABLE %s.test (pk int PRIMARY KEY , literal text, numeric int, b blob) WITH compression = { 'enabled' : false };"
            ));

            int numSSTableComponents = isWide ? V1OnDiskFormat.WIDE_PER_SSTABLE_COMPONENTS.size() : V1OnDiskFormat.SKINNY_PER_SSTABLE_COMPONENTS.size();
            int numIndexComponents = isLiteral ? V1OnDiskFormat.LITERAL_COMPONENTS.size() : V1OnDiskFormat.NUMERIC_COMPONENTS.size();
            int numComponents = sstableStreamingComponentsCount() + numSSTableComponents + numIndexComponents + 1;

            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.test(literal) USING 'sai';"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.test(numeric) USING 'sai';"));

            cluster.stream().forEach(i ->
                i.nodetoolResult("disableautocompaction", KEYSPACE).asserts().success()
            );
            IInvokableInstance first = cluster.get(1);
            IInvokableInstance second = cluster.get(2);
            long sstableCount = 10;
            long expectedFiles = isZeroCopyStreaming ? sstableCount * numComponents : sstableCount;

            for (int i = 0; i < sstableCount; i++)
            {
                if (isWide)
                {
                    String insertTemplate = "INSERT INTO %s.test(pk, ck, " + (isLiteral ? "literal" : "numeric") + ", b) VALUES (?, ?, ?, ?)";
                    first.executeInternal(withKeyspace(insertTemplate), i, i, isLiteral ? "v" + i : Integer.valueOf(i), BLOB);
                }
                else
                {
                    String insertTemplate = "INSERT INTO %s.test(pk, " + (isLiteral ? "literal" : "numeric") + ", b) VALUES (?, ?, ?)";
                    first.executeInternal(withKeyspace(insertTemplate), i, isLiteral ? "v" + i : Integer.valueOf(i), BLOB);
                }
                first.flush(KEYSPACE);
            }

            second.nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();

            SimpleQueryResult qr = first.executeInternalWithResult("SELECT * FROM system_views.streaming");
            String txt = QueryResultUtil.expand(qr);
            qr.reset();
            assertThat(qr.toObjectArrays().length).describedAs("Found rows\n%s", txt).isEqualTo(1);
            assertThat(qr.hasNext()).isTrue();
            Row row = qr.next();
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

            qr = second.executeInternalWithResult("SELECT * FROM system_views.streaming");
            txt = QueryResultUtil.expand(qr);
            qr.reset();
            assertThat(qr.toObjectArrays().length).describedAs("Found rows\n%s", txt).isEqualTo(1);
            assertThat(qr.hasNext()).isTrue();

            QueryResultUtil.assertThat(qr.next())
                           .isEqualTo("peers", Collections.singletonList(first.broadcastAddress().toString()))
                           .isEqualTo("follower", false)
                           .isEqualTo("operation", "Rebuild")
                           .isEqualTo("status", "success")
                           .isEqualTo("progress_percentage", 100.0F)
                           .isEqualTo("success_message", null).isEqualTo("failure_cause", null)
                           .columnsEqualTo("files_to_receive", "files_received").isEqualTo("files_received", expectedFiles)
                           .columnsEqualTo("bytes_to_receive", "bytes_received").isEqualTo("bytes_received", totalBytes)
                           .columnsEqualTo("files_sent", "files_to_send", "bytes_sent", "bytes_to_send").isEqualTo("files_sent", 0L);

            // did we trigger slow event log?
            cluster.forEach(i -> Assertions.assertThat(i.logs().grep("Handling streaming events took longer than").getResult())
                                           .describedAs("Unable to find slow log for node%d", i.config().num())
                                           .isNotEmpty());

            for (int i = 0; i < sstableCount; i++)
            {
                Object[][] rs = isLiteral ? second.executeInternal(withKeyspace("select pk from %s.test where literal = ?"), "v" + i)
                                          : second.executeInternal(withKeyspace("select pk from %s.test where numeric = ?"), i);
                assertThat(rs.length).isEqualTo(1);
                assertThat(rs[0][0]).isEqualTo(i);
            }
        }
    }
}
