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
import org.apache.cassandra.distributed.shared.Versions;

public class ReadRepairCompactStorageUpgradeTest extends UpgradeTestBase
{
    /**
     * Tests {@code COMPACT STORAGE} behaviour with mixed replica versions.
     * <p>
     * See CASSANDRA-15363 for further details.
     */
    @Test
    public void mixedModeReadRepairCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrades(v22, v3X)
        .setup((cluster) -> cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl" +
                                                              " (pk ascii, b boolean, v blob, PRIMARY KEY (pk))" +
                                                              " WITH COMPACT STORAGE")))
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return;
            // now node1 is 3.0/3.x and node2 is 2.2
            // make sure 2.2 side does not get the mutation
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk = ?"), "something");
            // trigger a read repair
            cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"),
                                           ConsistencyLevel.ALL,
                                           "something");
            cluster.get(2).flush(KEYSPACE);
        })
        .runAfterClusterUpgrade((cluster) -> cluster.get(2).forceCompact(KEYSPACE, "tbl"))
        .run();
    }
}
