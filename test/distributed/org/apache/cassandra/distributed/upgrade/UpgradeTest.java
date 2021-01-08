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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.Versions;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;

public class UpgradeTest extends UpgradeTestBase
{
    public static void truncateWriteReadTest(UpgradeableCluster cluster, int instanceNum)
    {
        truncateWriteReadTest(cluster);
    }
    public static void truncateWriteReadTest(UpgradeableCluster cluster)
    {
        cluster.forEach(truncateInstance -> {

            final int truncateInstanceNum = truncateInstance.config().num();
            truncateInstance.coordinator().execute("TRUNCATE " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            cluster.forEach(coordInstance -> {
                final int coordInstanceNum = coordInstance.config().num();
                coordInstance.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                                    ConsistencyLevel.ALL,
                                                    truncateInstanceNum, coordInstanceNum, coordInstanceNum);
            });

            Object[][] expected = new Object[cluster.size()][3];
            for (int rowNum = 0; rowNum < cluster.size(); rowNum++)
                expected[rowNum] = row(truncateInstanceNum, rowNum + 1, rowNum + 1);

            cluster.forEach(coordInstance -> {
                assertRows(coordInstance.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                               ConsistencyLevel.ALL, truncateInstanceNum),
                           expected);

                // Check the truncate removed previous rows
                assertRows(coordInstance.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl",
                                                               ConsistencyLevel.ALL),
                           expected);
            });
        });
    }

    @Test
    public void upgradeTest() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30, Versions.Major.v3X)
        .upgrade(Versions.Major.v30, Versions.Major.v3X, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            truncateWriteReadTest(cluster);
        })
        .runAfterNodeUpgrade(UpgradeTest::truncateWriteReadTest)
        .run();
    }
    
    @Test
    public void simpleUpgradeWithNetworkAndGossipTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            for (int i : new int[]{ 1, 2 })
            {
                assertRows(cluster.coordinator(i).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                          ConsistencyLevel.ALL,
                                                          1),
                           row(1, 1, 1));
            }
        }).run();
    }
}
