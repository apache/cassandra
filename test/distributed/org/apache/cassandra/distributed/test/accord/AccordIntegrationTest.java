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

package org.apache.cassandra.distributed.test.accord;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTxnBuilder;
import org.apache.cassandra.service.accord.db.AccordData;

import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.*;

public class AccordIntegrationTest extends TestBaseImpl
{
    private static void assertRow(Cluster cluster, String query, int k, int c, int v)
    {
        Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM);
        Assert.assertArrayEquals(new Object[]{new Object[] {k, c, v}}, result);
    }

    @Test
    public void testQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));

            cluster.get(1).runOnInstance(() -> {
                AccordTxnBuilder txnBuilder = new AccordTxnBuilder();
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1)");
                txnBuilder.withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS);
                try
                {
                    AccordData result = (AccordData) AccordService.instance.node.coordinate(txnBuilder.build()).get();
                    Assert.assertNotNull(result);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new AssertionError(e);
                }
            });

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        }
    }

    @Test
    public void multiKeyMultiQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();

        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));

            cluster.get(1).runOnInstance(() -> {
                AccordTxnBuilder txnBuilder = new AccordTxnBuilder();
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0");
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 0)");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 0)");
                txnBuilder.withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS);
                try
                {
                    AccordData result = (AccordData) AccordService.instance.node.coordinate(txnBuilder.build()).get();
                    Assert.assertNotNull(result);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new AssertionError(e);
                }
            });

            cluster.get(1).runOnInstance(() -> {
                AccordTxnBuilder txnBuilder = new AccordTxnBuilder();
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0");
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 1)");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (2, 0, 1)");
                txnBuilder.withCondition(keyspace, "tbl", 1, 0, "v", EQUAL, 0);
                try
                {
                    AccordData result = (AccordData) AccordService.instance.node.coordinate(txnBuilder.build()).get();
                    Assert.assertNotNull(result);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new AssertionError(e);
                }
            });
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 1);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0", 2, 0, 1);
        }
    }
}
