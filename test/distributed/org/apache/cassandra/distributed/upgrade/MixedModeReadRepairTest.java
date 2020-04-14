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

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.Versions;


public class MixedModeReadRepairTest extends UpgradeTestBase
{
    @Test
    public void mixedModeReadRepairCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup((cluster) -> cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk ascii, b boolean, v blob, PRIMARY KEY (pk)) WITH COMPACT STORAGE"))
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return;
            // now node1 is 3.0 and node2 is 2.2
            // make sure 2.2 side does not get the mutation
            cluster.get(1).executeInternal("DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                          "something");
            // trigger a read repair
            cluster.coordinator(2).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                           ConsistencyLevel.ALL,
                                           "something");
            cluster.get(2).flush(DistributedTestBase.KEYSPACE);
        })
        .runAfterClusterUpgrade((cluster) -> cluster.get(2).forceCompact(DistributedTestBase.KEYSPACE, "tbl"))
        .run();
    }
}
