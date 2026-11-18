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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.waitForIndexQueryable;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the BETWEEN operator respects SQL semantics:
 * - Normal BETWEEN (low <= high) returns rows.
 * - Inverted BETWEEN (low > high) returns no rows (empty result).
 */
public class BetweenInversionTest extends TestBaseImpl
{
    @Test
    public void testBetweenInversion() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.t1 (pk int PRIMARY KEY, val text)");

            cluster.coordinator(1).execute("INSERT INTO ks.t1 (pk,val) VALUES (1,'a')", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t1 (pk,val) VALUES (2,'b')", ConsistencyLevel.ALL);

            Object[][] rows = cluster.coordinator(1).execute("SELECT * FROM ks.t1 WHERE pk BETWEEN 1 AND 2 ALLOW FILTERING", ConsistencyLevel.ALL);
            assertEquals(2, rows.length);

            Object[][] inverted = cluster.coordinator(1).execute("SELECT * FROM ks.t1 WHERE pk BETWEEN 2 AND 1 ALLOW FILTERING", ConsistencyLevel.ALL);
            assertEquals(0, inverted.length);
        }
    }

    @Test
    public void testBetweenInversionWithSaiIndex() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.t2 (pk0 int, pk1 int, ck int, v int, PRIMARY KEY ((pk0, pk1), ck))");
            cluster.schemaChange("CREATE INDEX t2_pk1 ON ks.t2 (pk1) USING 'SAI'");
            cluster.schemaChange("CREATE INDEX t2_v ON ks.t2 (v) USING 'SAI'");
            waitForIndexQueryable(cluster, "ks");

            cluster.coordinator(1).execute("INSERT INTO ks.t2 (pk0, pk1, ck, v) VALUES (0, 1, 0, 1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t2 (pk0, pk1, ck, v) VALUES (0, 2, 0, 2)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t2 (pk0, pk1, ck, v) VALUES (0, 2, 1, 1)", ConsistencyLevel.ALL);

            beforeAndAfterFlush(cluster, "ks", () -> {
                // non-inverted ranges match
                assertEquals(3, select(cluster, "SELECT * FROM ks.t2 WHERE pk1 BETWEEN 1 AND 2"));
                assertEquals(3, select(cluster, "SELECT * FROM ks.t2 WHERE v BETWEEN 1 AND 2"));

                // inverted bounds must match nothing
                assertEquals(0, select(cluster, "SELECT * FROM ks.t2 WHERE pk1 BETWEEN 2 AND 1"));
                assertEquals(0, select(cluster, "SELECT * FROM ks.t2 WHERE v BETWEEN 2 AND 1"));
                assertEquals(0, select(cluster, "SELECT * FROM ks.t2 WHERE pk0 = 0 AND pk1 = 1 AND v BETWEEN 2 AND 1"));
            });

            cluster.schemaChange("CREATE TABLE ks.t3 (pk0 int, pk1 int, ck int, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH CLUSTERING ORDER BY (ck DESC)");
            cluster.schemaChange("CREATE INDEX t3_ck ON ks.t3 (ck) USING 'SAI'");
            waitForIndexQueryable(cluster, "ks");

            cluster.coordinator(1).execute("INSERT INTO ks.t3 (pk0, pk1, ck, v) VALUES (0, 1, 1, 1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t3 (pk0, pk1, ck, v) VALUES (0, 2, 2, 2)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t3 (pk0, pk1, ck, v) VALUES (0, 2, 3, 1)", ConsistencyLevel.ALL);

            // same checks, but on an actual reversed clustering column
            beforeAndAfterFlush(cluster, "ks", () -> {
                assertEquals(3, select(cluster, "SELECT * FROM ks.t3 WHERE ck BETWEEN 1 AND 3"));
                assertEquals(0, select(cluster, "SELECT * FROM ks.t3 WHERE ck BETWEEN 3 AND 1"));
            });
        }
    }

    private static int select(Cluster cluster, String query)
    {
        return cluster.coordinator(1).execute(query, ConsistencyLevel.ALL).length;
    }
}