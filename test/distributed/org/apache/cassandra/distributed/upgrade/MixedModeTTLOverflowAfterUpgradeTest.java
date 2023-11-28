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

import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.distributed.upgrade.MixedModeTTLOverflowUpgradeTest.assertPolicyTriggersAt2038;
import static org.apache.cassandra.distributed.upgrade.MixedModeTTLOverflowUpgradeTest.assertPolicyTriggersAt2106;
import static org.apache.cassandra.distributed.upgrade.MixedModeTTLOverflowUpgradeTest.restartNodeWithCompatibilityMode;
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
public class MixedModeTTLOverflowAfterUpgradeTest extends UpgradeTestBase
{
    @Test
    public void testTTLOverflowAfterUpgrade() throws Throwable
    {
        MixedModeTTLOverflowUpgradeTest.testTTLOverflow((cluster, node) -> {
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
}
