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
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;

public class UpgradeTest extends UpgradeTestBase
{

    @Test
    public void upgradeTest() throws Throwable
    {
        new TestCase()
            .upgrade(Versions.Major.v22, Versions.Major.v30, Versions.Major.v3X)
            .upgrade(Versions.Major.v30, Versions.Major.v3X, Versions.Major.v4)
            .setup((cluster) -> {
                cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

                cluster.get(1).executeInternal("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
                cluster.get(2).executeInternal("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
                cluster.get(3).executeInternal("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");
            })
            .runAfterClusterUpgrade((cluster) -> {
                DistributedTestBase.assertRows(cluster.coordinator(1).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                              ConsistencyLevel.ALL,
                                                                              1),
                                               DistributedTestBase.row(1, 1, 1),
                                               DistributedTestBase.row(1, 2, 2),
                                               DistributedTestBase.row(1, 3, 3));
            }).run();
    }

}
