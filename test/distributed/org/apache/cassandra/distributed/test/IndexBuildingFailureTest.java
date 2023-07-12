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
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.assertj.core.api.Assertions.assertThat;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IndexBuildingFailureTest extends TestBaseImpl
{
    @Test
    public void testIndexComponentStreaming() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.with(NETWORK, GOSSIP)).start()))
        {
            cluster.disableAutoCompaction(KEYSPACE);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.test (pk int PRIMARY KEY, v text)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.test(v)"));

            IInvokableInstance first = cluster.get(1);
            IInvokableInstance second = cluster.get(2);
            
            first.executeInternal(withKeyspace("insert into %s.test(pk, v) values (?, ?)"), 1, "v1");
            first.flush(KEYSPACE);
            
            Object[][] rs = second.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(0);

            // The repair job should fail when the index build fails...
            NodeToolResult result = second.nodetoolResult("repair", KEYSPACE);
            result.asserts().failure();

            // By the way, the test won't see the compaction interruption from this as an uncought exception, because
            // CompactionExecutor catches it in afterExecute().

            // ...but the index build fails after the tracker sees the new SSTables, so we can query them...
            rs = second.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(1);
            assertThat(rs[0][0]).isEqualTo(1);
            
            // ...but trying to query the index shows that the backing table and indexes are now out-of-sync. Data loss!
            rs = second.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(1);
            assertThat(rs[0][0]).isEqualTo(1);
            
            // If you uncomment this last assertion block above, you can try to restart, but the situation won't
            // get any better. The base table will still be out of sync if the async index build at startup takes
            // too long or fails for any reason. This compaction interruption will be seen by the uncaught handler
            // when we try to shut the test cluster down though.
            
            second.shutdown().get();
            second.startup();

            // The base table is still fine...
            rs = second.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(1);
            assertThat(rs[0][0]).isEqualTo(1);

            // ...but now the index isn't even available. Honestly this is better than quietly returning no data!
            rs = second.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(1);
            assertThat(rs[0][0]).isEqualTo(1);
        }
    }
}
