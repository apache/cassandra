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

import java.util.Arrays;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class StreamingTest extends DistributedTestBase
{

    private void testStreaming(int nodes, int replicationFactor, int rowCount, String compactionStrategy) throws Throwable
    {
        try (Cluster cluster = Cluster.create(nodes, config -> config.with(NETWORK)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'true'}", KEYSPACE, compactionStrategy));

            for (int i = 0 ; i < rowCount ; ++i)
            {
                for (int n = 1 ; n < nodes ; ++n)
                    cluster.get(n).executeInternal(String.format("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');", KEYSPACE), Integer.toString(i));
            }

            cluster.get(nodes).executeInternal("TRUNCATE system.available_ranges;");
            {
                Object[][] results = cluster.get(nodes).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(0, results.length);
            }

            cluster.get(nodes).runOnInstance(() -> StorageService.instance.rebuild(null, KEYSPACE, null, null));
            {
                Object[][] results = cluster.get(nodes).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(1000, results.length);
                Arrays.sort(results, Comparator.comparingInt(a -> Integer.parseInt((String) a[0])));
                for (int i = 0 ; i < results.length ; ++i)
                {
                    Assert.assertEquals(Integer.toString(i), results[i][0]);
                    Assert.assertEquals("value1", results[i][1]);
                    Assert.assertEquals("value2", results[i][2]);
                }
            }
        }
    }

    @Test
    public void test() throws Throwable
    {
        testStreaming(2, 2, 1000, "LeveledCompactionStrategy");
    }

}
