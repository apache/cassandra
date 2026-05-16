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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the chunked streaming receiver path (CassandraStreamReader / CassandraCompressedStreamReader →
 * BigTableWriter.openDataWriter → DataComponent.buildWriter with OperationType.STREAM) writes via
 * {@link org.apache.cassandra.io.compress.DirectCompressedSequentialWriter} when direct background writes
 * are enabled. Entire-SSTable (zero-copy) streaming bypasses DataComponent.buildWriter, so it must be
 * disabled here.
 */
public class StreamingDirectWriteTest extends TestBaseImpl
{
    @Test
    public void testChunkedStreamReceiverWithDirectWrites() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)
                                                             .set("stream_entire_sstables", false)
                                                             .set("background_write_disk_access_mode", "direct"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k text PRIMARY KEY, v blob) WITH compression = {'enabled':'true', 'class':'LZ4Compressor'};"));
            cluster.stream().forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE).asserts().success());

            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);

            // ~800 KiB of incompressible data spread across many flushes — multiple sstables to stream chunked.
            int rowCount = 100;
            int valBytes = 8 * 1024;
            byte[] val = new byte[valBytes];
            new Random(42).nextBytes(val);
            ByteBuffer blob = ByteBuffer.wrap(val);
            for (int i = 0; i < rowCount; i++)
            {
                node1.executeInternal(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), "k" + i, blob);
                if ((i % 20) == 19)
                    node1.flush(KEYSPACE);
            }
            node1.flush(KEYSPACE);

            node2.nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();

            Object[][] rows = node2.executeInternal(withKeyspace("SELECT k FROM %s.t"));
            assertThat(rows.length).isEqualTo(rowCount);

            assertThat(node2.logs().grep("Direct I/O writer activated for Stream").getResult())
                .describedAs("expected DIO writer to engage for STREAM op on receiver")
                .isNotEmpty();
        }
    }
}
