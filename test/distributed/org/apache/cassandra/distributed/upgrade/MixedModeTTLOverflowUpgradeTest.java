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

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.utils.StorageCompatibilityMode.NONE;
import static org.apache.cassandra.utils.StorageCompatibilityMode.UPGRADING;

/**
 * Tests TTL the overflow policy triggers at the correct limit: year 2038 <=nb or 2186 >=oa
 * <p>
 * <=oa overflow policy triggers at year 2038. That could be <=4.1 or 5.0 with 4.x storage compatibility
 * >oa overflow policy triggers at year 2106. That is >=5.0 using >=5.x storage compatibility
 *
 * @see StorageCompatibilityMode
 */
public class MixedModeTTLOverflowUpgradeTest extends UpgradeTestBase
{
    @Test
    public void testTTLOverflowDuringUpgrade() throws Throwable
    {
        testTTLOverflow((cluster, node) -> {
            if (node == 1) // only node1 is upgraded, and the cluster is in mixed versions mode
            {
                assertPolicyTriggersAt2038(cluster.coordinator(1));
                assertPolicyTriggersAt2038(cluster.coordinator(2));

                // We restart the upgraded node out of 4.0 storage compatibility,
                // and we set it to be compatible with 5.0.
                // 2038 should still be the limit because there is still a not upgraded node.
                restartNodeWithCompatibilityMode(cluster, 1, UPGRADING);
                assertPolicyTriggersAt2038(cluster.coordinator(1));
                assertPolicyTriggersAt2038(cluster.coordinator(2));
            }
            else // both nodes have been upgraded, and the cluster isn't in mixed version mode anymore
            {
                // Once we have completed the upgrade, 2038 should still be the limit because there is still one node
                // in 5.0 storage compatibility mode.
                assertPolicyTriggersAt2038(cluster.coordinator(1));
                assertPolicyTriggersAt2038(cluster.coordinator(2));

                // We restart the last upgraded node in 5.0 compatibility mode, so both nodes are now in 5.0
                // compatibility mode, and the limit should be 2106.
                restartNodeWithCompatibilityMode(cluster, 2, UPGRADING);
                assertPolicyTriggersAt2106(cluster.coordinator(1));
                assertPolicyTriggersAt2106(cluster.coordinator(2));

                // We restart get both nodes out of compatibility mode, so the limit should be 2106.
                restartNodeWithCompatibilityMode(cluster, 1, NONE);
                restartNodeWithCompatibilityMode(cluster, 2, NONE);
                assertPolicyTriggersAt2106(cluster.coordinator(1));
                assertPolicyTriggersAt2106(cluster.coordinator(2));
            }
        });
    }

    @Test
    public void testTTLOverflowAfterUpgrade() throws Throwable
    {
        testTTLOverflow((cluster, node) -> {
            if (node == 1) // only node1 is upgraded, and the cluster is in mixed versions mode
            {
                assertPolicyTriggersAt2038(cluster.coordinator(1));
                assertPolicyTriggersAt2038(cluster.coordinator(2));
            }
            else // both nodes have been upgraded, and the cluster isn't in mixed version mode anymore
            {
                assertPolicyTriggersAt2038(cluster.coordinator(1));
                assertPolicyTriggersAt2038(cluster.coordinator(2));

                // We restart one node on 5.0 >oa hence 2038 should still be the limit as the other node is 5.0 <=oa
                // We're on compatibility mode where oa and oa nodes are a possibility
                restartNodeWithCompatibilityMode(cluster, 1, UPGRADING);
                assertPolicyTriggersAt2038(cluster.coordinator(1));
                assertPolicyTriggersAt2038(cluster.coordinator(2));

                // We restart the other node so they're all on 5.0 >oa hence 2106 should be the limit
                restartNodeWithCompatibilityMode(cluster, 2, UPGRADING);
                assertPolicyTriggersAt2106(cluster.coordinator(1));
                assertPolicyTriggersAt2106(cluster.coordinator(2));

                // We restart the cluster out of compatibility mode once everything is 5.0oa TTL 2106
                restartNodeWithCompatibilityMode(cluster, 1, NONE);
                restartNodeWithCompatibilityMode(cluster, 2, NONE);
                assertPolicyTriggersAt2106(cluster.coordinator(1));
                assertPolicyTriggersAt2106(cluster.coordinator(2));
            }
        });
    }

    private static void testTTLOverflow(RunOnClusterAndNode runAfterNodeUpgrade) throws Throwable
    {
        new TestCase()
                .nodes(2)
                .nodesToUpgradeOrdered(1, 2)
                .upgradesToCurrentFrom(v40)
                .setup(cluster -> {
                    cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));

                    assertPolicyTriggersAt2038(cluster.coordinator(1));
                    assertPolicyTriggersAt2038(cluster.coordinator(2));
                })
                .runAfterNodeUpgrade(runAfterNodeUpgrade)
                .run();
    }

    private static void restartNodeWithCompatibilityMode(UpgradeableCluster cluster, int node, StorageCompatibilityMode mode) throws Throwable
    {
        cluster.get(node).shutdown().get();
        cluster.get(node).config().set("storage_compatibility_mode", mode.toString());
        cluster.get(node).startup();
    }

    private static void assertPolicyTriggersAt2038(ICoordinator coordinator)
    {
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (0, 0) USING TTL " + Attributes.MAX_TTL), ALL))
                  .hasMessageContaining("exceeds maximum supported expiration date")
                  .hasMessageContaining("2038");
    }

    private static void assertPolicyTriggersAt2106(ICoordinator coordinator)
    {
        boolean overflowPoliciesApply = (Clock.Global.currentTimeMillis() / 1000) > (Cell.MAX_DELETION_TIME - Attributes.MAX_TTL);

        if (overflowPoliciesApply)
        {
            // This code won't run until 2086
            Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (0, 0) USING TTL " + Attributes.MAX_TTL), ALL))
                      .hasMessageContaining("exceeds maximum supported expiration date")
                      .hasMessageContaining("2106");
        }
        else
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (0, 0) USING TTL " + Attributes.MAX_TTL), ALL);
    }
}
