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

package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.TopPartitionTracker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ReplicaWarningTest extends TestBaseImpl
{
    @Test
    public void testMultiTableWrite() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("min_tracked_partition_size", "50B")
                                                             .set("write_thresholds_enabled", "true")
                                                             .set("write_size_warn_threshold", "50B")
                                                             .with(Feature.NATIVE_PROTOCOL))
                                           .start()))
        {
            createTables(cluster);
            populateTopPartitions(cluster.get(3));
            assertOneWarning(cluster, ConsistencyLevel.ALL);
        }
    }

    @Test
    public void testMultiDCWrite() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(6)
                                           .withRacks(2, 3)
                                           .withConfig(c -> c.set("min_tracked_partition_size", "50B")
                                                             .set("write_thresholds_enabled", "true")
                                                             .set("write_size_warn_threshold", "50B")
                                                             .with(Feature.NATIVE_PROTOCOL))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'NetworkTopologyStrategy', 'datacenter1' : 3, 'datacenter2': 3}"));
            createTables(cluster);
            for (int node : new int[] {4, 5})
                populateTopPartitions(cluster.get(node));
            assertOneWarning(cluster, ConsistencyLevel.EACH_QUORUM);
        }
    }

    private static void createTables(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("create table %s.tbl1 (id int primary key)"));
        cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key)"));
        cluster.schemaChange(withKeyspace("create table %s.tbl3 (id int primary key)"));
    }

    private static void populateTopPartitions(IInvokableInstance instance)
    {
        instance.runOnInstance(() -> {
            TopPartitionTracker tpt = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl1").topPartitions;
            for (int i = 0; i < 10; i++)
            {
                DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.fromString(String.valueOf(i)));
                tpt.topSizes().track(key, 100 + i);
            }
        });
    }

    private static void assertOneWarning(Cluster cluster, ConsistencyLevel cl)
    {
        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoint((String)cluster.get(1).config().get("rpc_address"));

        try (com.datastax.driver.core.Cluster c = builder.build(); Session session = c.connect())
        {
            BatchStatement bs = new BatchStatement();
            bs.add(new SimpleStatement(withKeyspace("insert into %s.tbl1 (id) values (1)")));
            bs.add(new SimpleStatement(withKeyspace("insert into %s.tbl2 (id) values (1)")));
            bs.add(new SimpleStatement(withKeyspace("insert into %s.tbl3 (id) values (1)")));
            ResultSet res = session.execute(bs.setConsistencyLevel(cl));

            List<String> warnings = res.getExecutionInfo().getWarnings();
            // only `tbl1` has any tracked top partitions, should only warn for that
            assertEquals(1, warnings.size());
            for (String warn : warnings)
            {
                assertFalse(warn.contains("tbl2"));
                assertFalse(warn.contains("tbl3"));
            }
        }
    }
}
