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

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.tcm.Transformation;

import static org.junit.Assert.assertTrue;


public class ClusterMetadataUpgradeCleanupPreInitializeTest extends UpgradeTestBase
{

    @Test
    public void cleanupPreInitializeTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .singleUpgradeToCurrentFrom(v41)
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        })
        .runAfterClusterUpgrade((cluster) -> {
            cluster.get(1).executeInternal("INSERT INTO system.local_metadata_log (epoch, kind) VALUES (1, 0)");
            cluster.get(1).flush("system");
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            cluster.get(1).logs().watchFor("Cleaning up orphaned PreInitialize at epoch 1");
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            boolean seenPreInit = false;
            boolean seenInit = false;
            boolean seenSnapshot = false;
            for (Object [] row : cluster.get(1).executeInternal("SELECT epoch, kind FROM system.local_metadata_log"))
            {
                switch (Transformation.Kind.fromId((Integer)row[1]))
                {
                    case PRE_INITIALIZE_CMS:
                        seenPreInit = true;
                        break;
                    case INITIALIZE_CMS:
                        seenInit = true;
                        break;
                    case TRIGGER_SNAPSHOT:
                        seenSnapshot = true;
                        break;
                }
            }
            assertTrue(seenPreInit);
            assertTrue(seenInit);
            assertTrue(seenSnapshot);
        }).run();
    }
}
