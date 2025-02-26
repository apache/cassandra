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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class ReplicaFilteringWithStaticsTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());
    }

    @Test
    public void testStaticMatchWithPartitionDelete()
    {
        testStaticMatchWithPartitionDelete(false);
    }

    @Test
    public void testStaticMatchWithPartitionDeleteSAI()
    {
        testStaticMatchWithPartitionDelete(true);
    }

    public void testStaticMatchWithPartitionDelete(boolean sai)
    {
        String table = "static_and_delete" + (sai ? "_sai" : "");
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 boolean, ck0 ascii, s1 tinyint static, v0 boolean, PRIMARY KEY (pk0, ck0)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'"));

        if (sai)
        {
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(s1) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        }

        CLUSTER.get(3).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 1 SET v0 = false WHERE pk0 = true AND ck0 = 'D'"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = true"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, s1, v0) VALUES (true, 'G', -114, true) USING TIMESTAMP 3"));
        CLUSTER.get(3).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0) VALUES (true, 'F') USING TIMESTAMP 4"));
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, s1, v0) VALUES (true, 'C', 17, true) USING TIMESTAMP 5"));

        // This update to the static column creates matches across all previously written live rows in the partition.
        // When RFP sees the unresolved static row, it must read enough data from the silent replicas at nodes 1 and 3
        // to find all potential matches. With a page size of 1, reading only 1 row from node 3 will return the row at
        // ck = 'D', as the partition delete never made it to node 3. This means we'll ignore the live result node 3 has
        // at ck = 'F', because node 1 will produce a result at ck = 'G', and that determines the next paging cursor.
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 6 SET s1 = 1, v0 = false WHERE pk0 = true AND ck0 = 'A'"));

        String select = withKeyspace("SELECT ck0 FROM %s." + table + " WHERE s1 = 1" + (sai ? "" : " ALLOW FILTERING" ));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ALL, 1), row("A"), row("C"), row("F"), row("G"));
    }

    @Test
    public void testMissingStaticRowWithNonStaticExpression()
    {
        testMissingStaticRowWithNonStaticExpression(false);
    }

    @Test
    public void testMissingStaticRowWithNonStaticExpressionSAI()
    {
        testMissingStaticRowWithNonStaticExpression(true);
    }
    
    public void testMissingStaticRowWithNonStaticExpression(boolean sai)
    {
        String table = "single_predicate" + (sai ? "_sai" : "");
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, ck0 int, ck1 int, s0 int static, s1 int static, v0 int, PRIMARY KEY (pk0, ck0, ck1)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC, ck1 DESC) AND read_repair = 'NONE'"));
        
        if (sai)
        {
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(ck1) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        }

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, ck1, s0, s1, v0) " +
                                                    "VALUES (0, 1, 2, 3, 4, 5) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s." + table + "  USING TIMESTAMP 2 SET s0 = 6, s1 = 7, v0 = 8 " +
                                                    "WHERE  pk0 = 0 AND ck0 = 9 AND ck1 = 10"));

        // Node 2 will not produce a match for the static row. Make sure that replica filtering protection does not
        // fetch the entire partition, which could let non-matching rows slip through combined with the fact that we 
        // don't post-filter at the coordinator with no regular column predicates in the query.
        String select = withKeyspace("SELECT pk0, ck0, ck1, s0, s1 FROM %s." + table + " WHERE ck1 = 2" + (sai ? "" : " ALLOW FILTERING"));
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 2, 6, 7));
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}
