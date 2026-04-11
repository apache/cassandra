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

import static org.apache.cassandra.utils.StorageCompatibilityMode.NONE;
import static org.apache.cassandra.utils.StorageCompatibilityMode.UPGRADING;

public class MixedModeTTLOverflowDuringUpgradeTest extends MixedModeTTLOverflowUpgradeTestBase
{
    @Test
    public void testTTLOverflowDuringUpgrade() throws Throwable
    {
        testTTLOverflow((cluster, node) -> {
            cluster.disableAutoCompaction(KEYSPACE);
            if (node == 1) // only node1 is upgraded, and the cluster is in mixed versions mode
            {
                verify(Step.NODE1_40_NODE2_PREV, cluster, true);

                // We restart the upgraded node 1 with compatibility mode = UPGRADING
                restartNodeWithCompatibilityMode(cluster, 1, UPGRADING);
                // 2038 should still be the limit, because node2 is not upgraded yet
                verify(Step.NODE1_UPGRADING_NODE2_PREV, cluster, true);
            }
            else // both nodes have been upgraded, and the cluster isn't in mixed version mode anymore
            {
                // Once we have completed the upgrade, 2038 should still be the limit because
                // node2 is still in 4.x compatibility mode
                verify(Step.NODE1_UPGRADING_NODE2_40, cluster, true);

                // We restart the last upgraded node in UPGRADING compatibility mode
                restartNodeWithCompatibilityMode(cluster, 2, UPGRADING);
                // Both nodes are in UPGRADING compatibility mode, so the limit should be 2106
                verify(Step.NODE1_UPGRADING_NODE2_UPGRADING, cluster, false);

                // We restart get both nodes out of compatibility mode, so the limit should be 2106.
                restartNodeWithCompatibilityMode(cluster, 1, NONE);
                verify(Step.NODE1_NONE_NODE2_UPGRADING, cluster, false);
                restartNodeWithCompatibilityMode(cluster, 2, NONE);
                verify(Step.NODE1_NONE_NODE2_NONE, cluster, false);
            }
        });
    }
}
