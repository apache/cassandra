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

import com.vdurmont.semver4j.Semver;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class DropCompactStorageBeforeUpgradeSSTablesTest extends DropCompactStorageTester
{
    @Test
    public void dropCompactStorageBeforeUpgradesstablesTo3X() throws Throwable
    {
        dropCompactStorageBeforeUpgradeSstables(v3X);
    }

    /**
     * Upgrades a node from 2.2 to 3.x and DROP COMPACT just after the upgrade but _before_ upgrading the underlying
     * sstables.
     *
     * <p>This test reproduces the issue from CASSANDRA-15897.
     */
    public void dropCompactStorageBeforeUpgradeSstables(Semver upgradeTo) throws Throwable
    {
        new TestCase()
        .nodes(1)
        .singleUpgrade(v22, upgradeTo)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL).set("enable_drop_compact_storage", true))
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, v int, PRIMARY KEY (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck, v) values (1, ?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).flush(KEYSPACE);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            Throwable thrown = catchThrowable(() -> cluster.schemaChange("ALTER TABLE "+KEYSPACE+".tbl DROP COMPACT STORAGE"));
            assertThat(thrown).hasMessageContainingAll("Cannot DROP COMPACT STORAGE as some nodes in the cluster",
                                                       "has some non-upgraded 2.x sstables");

            assertThat(cluster.get(1).nodetool("upgradesstables")).isEqualTo(0);
            Thread.sleep(1000);
            cluster.schemaChange("ALTER TABLE "+KEYSPACE+".tbl DROP COMPACT STORAGE");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
        })
        .run();
    }
}
