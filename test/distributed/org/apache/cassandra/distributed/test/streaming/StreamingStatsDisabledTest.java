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
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.streaming.StreamManager;

public class StreamingStatsDisabledTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(Feature.values()).set("streaming_stats_enabled", false))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.users (user_id varchar, PRIMARY KEY (user_id));"));
            cluster.stream().forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE).asserts().success());

            long expectedFiles = 10;
            for (int i = 0; i < expectedFiles; i++)
            {
                cluster.get(1).executeInternal(withKeyspace("insert into %s.users(user_id) values (?)"), "dcapwell" + i);
                cluster.get(1).flush(KEYSPACE);
            }

            cluster.get(2).nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();
            for (int nodeId : Arrays.asList(1, 2))
                QueryResultUtil.assertThat(cluster.get(nodeId).executeInternalWithResult("SELECT * FROM system_views.streaming")).isEmpty();

            // trigger streaming again
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.users(user_id) VALUES ('trigger streaming')"));
            // mimic JMX
            cluster.get(2).runOnInstance(() -> StreamManager.instance.setStreamingStatsEnabled(true));
            cluster.get(2).nodetoolResult("repair", KEYSPACE).asserts().success();

            QueryResultUtil.assertThat(cluster.get(1).executeInternalWithResult("SELECT * FROM system_views.streaming")).isEmpty();
            QueryResultUtil.assertThat(cluster.get(2).executeInternalWithResult("SELECT * FROM system_views.streaming")).hasSize(1);
        }
    }
}
