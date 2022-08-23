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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ICoordinator;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests the CQL functions {@code writetime}, {@code maxwritetime} and {@code ttl} on rolling upgrade.
 *
 * {@code writetime} and {@code ttl} on single-cell columns is always supported, even in mixed clusters.
 * {@code writetime} and {@code ttl} on multi-cell columns is not supported in coordinator nodes < 4.2.
 * {@code maxwritetime} is not supported in coordinator nodes < 4.2.
 */
public class MixedModeWritetimeOrTTLTest extends UpgradeTestBase
{
    @Test
    public void testWritetimeOrTTLDuringUpgrade() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgradeOrdered(1, 2)
        .upgradesToCurrentFrom(v30)
        .setup(cluster -> {

            ICoordinator coordinator = cluster.coordinator(1);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int, s set<int>, fs frozen<set<int>>)"));
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v, s, fs) VALUES (0, 0, {0, 1}, {0, 1, 2, 3}) USING TIMESTAMP 1 AND TTL 1000"), ALL);
            coordinator.execute(withKeyspace("UPDATE %s.t USING TIMESTAMP 2 AND TTL 2000 SET v = 1, s = s + {2, 3} WHERE k = 0"), ALL);

            assertPre42Behaviour(cluster.coordinator(1));
            assertPre42Behaviour(cluster.coordinator(2));
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node == 1) // only node1 is upgraded, and the cluster is in mixed mode
            {
                assertPost42Behaviour(cluster.coordinator(1));
                assertPre42Behaviour(cluster.coordinator(2));
            }
            else // both nodes have been upgraded, and the cluster isn't in mixed mode anymore
            {
                assertPost42Behaviour(cluster.coordinator(1));
                assertPost42Behaviour(cluster.coordinator(2));
            }
        })
        .run();
    }

    private void assertPre42Behaviour(ICoordinator coordinator)
    {
        // regular column, supported except for maxwritetime
        assertRows(coordinator.execute(withKeyspace("SELECT writetime(v) FROM %s.t"), ALL), row(2L));
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("SELECT maxwritetime(v) FROM %s.t"), ALL))
                  .hasMessageContaining("Unknown function 'maxwritetime'");
        Assertions.assertThat((Integer) coordinator.execute(withKeyspace("SELECT ttl(v) FROM %s.t"), ALL)[0][0])
                  .isLessThanOrEqualTo(2000).isGreaterThan(2000 - 300); // margin of error of 5 minutes since TTLs decrease

        // frozen collection, supported except for maxwritetime
        assertRows(coordinator.execute(withKeyspace("SELECT writetime(fs) FROM %s.t"), ALL), row(1L));
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("SELECT maxwritetime(fs) FROM %s.t"), ALL))
                  .hasMessageContaining("Unknown function 'maxwritetime'");
        Assertions.assertThat((Integer) coordinator.execute(withKeyspace("SELECT ttl(fs) FROM %s.t"), ALL)[0][0])
                  .isLessThanOrEqualTo(1000).isGreaterThan(1000 - 300); // margin of error of 5 minutes since TTLs decrease

        // not-frozen collection, not supported
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("SELECT writetime(s) FROM %s.t"), ALL))
                  .hasMessageContaining("Cannot use selection function writeTime on non-frozen collection s");
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("SELECT maxwritetime(s) FROM %s.t"), ALL))
                  .hasMessageContaining("Unknown function 'maxwritetime'");
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace("SELECT ttl(s) FROM %s.t"), ALL))
                  .hasMessageContaining("Cannot use selection function ttl on non-frozen collection s");
    }

    private void assertPost42Behaviour(ICoordinator coordinator)
    {
        // regular column, fully supported
        assertRows(coordinator.execute(withKeyspace("SELECT writetime(v) FROM %s.t"), ALL), row(2L));
        assertRows(coordinator.execute(withKeyspace("SELECT maxwritetime(v) FROM %s.t"), ALL), row(2L));
        Assertions.assertThat((Integer) coordinator.execute(withKeyspace("SELECT ttl(v) FROM %s.t"), ALL)[0][0])
                  .isLessThanOrEqualTo(2000).isGreaterThan(2000 - 300); // margin of error of 5 minutes since TTLs decrease

        // frozen collection, fully supported
        assertRows(coordinator.execute(withKeyspace("SELECT writetime(fs) FROM %s.t"), ALL), row(1L));
        assertRows(coordinator.execute(withKeyspace("SELECT maxwritetime(fs) FROM %s.t"), ALL), row(1L));
        Assertions.assertThat((Integer) coordinator.execute(withKeyspace("SELECT ttl(fs) FROM %s.t"), ALL)[0][0])
                  .isLessThanOrEqualTo(1000).isGreaterThan(1000 - 300); // margin of error of 5 minutes since TTLs decrease

        // not-frozen collection, fully supported
        assertRows(coordinator.execute(withKeyspace("SELECT writetime(s) FROM %s.t"), ALL), row(Arrays.asList(1L, 1L, 2L, 2L)));
        assertRows(coordinator.execute(withKeyspace("SELECT maxwritetime(s) FROM %s.t"), ALL), row(2L));
        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT ttl(s) FROM %s.t"), ALL)[0][0])
                  .matches(l -> l instanceof List && ((List<?>) l).size() == 4);
    }
}
