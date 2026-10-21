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
import org.apache.cassandra.metrics.StorageMetrics;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamingDirectWriteTest extends TestBaseImpl
{
    @Test
    public void testChunkedStreamReceiverWithDirectWrites() throws IOException
    {
        // ZCS streaming bypasses DataComponent.buildWriter; disable to exercise the chunked receiver path.
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

            // Monotonic meter: a snapshot before/after rebuild proves the DIO writer engaged on the
            // receiver, regardless of timing or GC — unlike the inc/dec byte gauge, which races to 0.
            // NB: explicit lambda, not a method ref — a ref binds the non-serializable Meter and fails
            // to ship across the instance boundary; the lambda reads the static field on the receiver.
            long writersBefore = node2.callOnInstance(() -> StorageMetrics.directWriteBuffersAllocated.getCount());

            node2.nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();

            Object[][] rows = node2.executeInternal(withKeyspace("SELECT k FROM %s.t"));
            assertThat(rows.length).isEqualTo(rowCount);

            long writersAfter = node2.callOnInstance(() -> StorageMetrics.directWriteBuffersAllocated.getCount());
            assertThat(writersAfter)
                .describedAs("DIO writer must engage for STREAM op on the chunked receiver")
                .isGreaterThan(writersBefore);
        }
    }
}
