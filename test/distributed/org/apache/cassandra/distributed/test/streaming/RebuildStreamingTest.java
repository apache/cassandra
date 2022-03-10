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
package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class RebuildStreamingTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(Feature.values()).set("stream_entire_sstables", false))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.users (user_id varchar PRIMARY KEY);"));
            cluster.stream().forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE).asserts().success());
            IInvokableInstance first = cluster.get(1);
            IInvokableInstance second = cluster.get(2);
            long expectedFiles = 10;
            for (int i = 0; i < expectedFiles; i++)
            {
                first.executeInternal(withKeyspace("insert into %s.users(user_id) values (?)"), "dcapwell" + i);
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
                           .isEqualTo("peers", Collections.singletonList("/127.0.0.2:7012"))
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
                           .isEqualTo("peers", Collections.singletonList("/127.0.0.1:7012"))
                           .isEqualTo("follower", false)
                           .isEqualTo("operation", "Rebuild")
                           .isEqualTo("status", "success")
                           .isEqualTo("progress_percentage", 100.0F)
                           .isEqualTo("success_message", null).isEqualTo("failure_cause", null)
                           .columnsEqualTo("files_to_receive", "files_received").isEqualTo("files_received", expectedFiles)
                           .columnsEqualTo("bytes_to_receive", "bytes_received").isEqualTo("bytes_received", totalBytes)
                           .columnsEqualTo("files_sent", "files_to_send", "bytes_sent", "bytes_to_send").isEqualTo("files_sent", 0L);
        }
    }
}
