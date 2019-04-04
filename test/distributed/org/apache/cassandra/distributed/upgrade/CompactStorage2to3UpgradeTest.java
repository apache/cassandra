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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;

public class CompactStorage2to3UpgradeTest extends UpgradeTestBase
{
    @Test
    public void multiColumn() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            assert cluster.size() == 3;
            int rf = cluster.size() - 1;
            assert rf == 2;
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + (cluster.size() - 1) + "};");
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, v1 int, v2 text, PRIMARY KEY (pk)) WITH COMPACT STORAGE");
            ICoordinator coordinator = cluster.coordinator(1);
            // these shouldn't be replicated by the 3rd node
            coordinator.execute("INSERT INTO ks.tbl (pk, v1, v2) VALUES (3, 3, '3')", ConsistencyLevel.ALL);
            coordinator.execute("INSERT INTO ks.tbl (pk, v1, v2) VALUES (9, 9, '9')", ConsistencyLevel.ALL);
            for (int i=0; i<cluster.size(); i++)
            {
                int nodeNum = i+1;
                System.out.println(String.format("****** node %s: %s", nodeNum, cluster.get(nodeNum).config()));
            }

        })
        .runAfterNodeUpgrade(((cluster, node) -> {
            if (node != 2)
                return;

            Object[][] rows = cluster.coordinator(3).execute("SELECT * FROM ks.tbl LIMIT 2", ConsistencyLevel.ALL);
            Object[][] expected = {
                DistributedTestBase.row(9, 9, "9"),
                DistributedTestBase.row(3, 3, "3")
            };
            DistributedTestBase.assertRows(rows, expected);

        })).run();
    }

    @Test
    public void singleColumn() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            assert cluster.size() == 3;
            int rf = cluster.size() - 1;
            assert rf == 2;
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + (cluster.size() - 1) + "};");
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, v int, PRIMARY KEY (pk)) WITH COMPACT STORAGE");
            ICoordinator coordinator = cluster.coordinator(1);
            // these shouldn't be replicated by the 3rd node
            coordinator.execute("INSERT INTO ks.tbl (pk, v) VALUES (3, 3)", ConsistencyLevel.ALL);
            coordinator.execute("INSERT INTO ks.tbl (pk, v) VALUES (9, 9)", ConsistencyLevel.ALL);
            for (int i=0; i<cluster.size(); i++)
            {
                int nodeNum = i+1;
                System.out.println(String.format("****** node %s: %s", nodeNum, cluster.get(nodeNum).config()));
            }

        })
        .runAfterNodeUpgrade(((cluster, node) -> {

            if (node < 2)
                return;

            Object[][] rows = cluster.coordinator(3).execute("SELECT * FROM ks.tbl LIMIT 2", ConsistencyLevel.ALL);
            Object[][] expected = {
                DistributedTestBase.row(9, 9),
                DistributedTestBase.row(3, 3)
            };
            DistributedTestBase.assertRows(rows, expected);

        })).run();
    }
}
