/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.sai;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class SAIShortReadProtectionTest extends TestBaseImpl
{
    /**
     * Verifies that an indexed range query at CL.ALL without merge conflicts does not trigger spurious short-read
     * protection (SRP) requests. See CNDB-20381 for further details.
     */
    @Test
    public void testNoSpuriousShortReadProtectionOnSatisfiedLimit() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY(k, c))"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(v) USING 'sai'"));
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

            ICoordinator coordinator = cluster.coordinator(1);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (0, 0, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (0, 1, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (1, 0, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (1, 1, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (2, 0, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (2, 1, 0)"), ConsistencyLevel.ALL);
            int numRows = 6;

            IInvokableInstance node = cluster.get(1);
            long srpRequestsBefore = srpRequests(node);

            for (int limit = 1; limit <= 10; limit++)
            {
                String select = withKeyspace("SELECT * FROM %s.t WHERE v >= 0 AND v < 10 LIMIT " + limit);
                Object[][] rows = coordinator.execute(select, ConsistencyLevel.ALL);
                Assertions.assertThat(rows.length).isEqualTo(Math.min(limit, numRows));
            }

            // No SRP requests should have been issued.
            Assertions.assertThat(srpRequests(node)).isEqualTo(srpRequestsBefore);

            // Verify that SRP requests are actually triggered when rows are shadowed on some replicas,
            // requiring extra rows to be fetched to satisfy the limit.
            // The main tests for this are in IndexConsistencyTest, this is just a sanity check.
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.t WHERE k = 0 AND c = 0"));
            String select = withKeyspace("SELECT * FROM %s.t WHERE v >= 0 AND v < 10 LIMIT 5");
            Object[][] rows = coordinator.execute(select, ConsistencyLevel.ALL);
            Assertions.assertThat(rows.length).isEqualTo(5);
            Assertions.assertThat(srpRequests(node)).isGreaterThan(srpRequestsBefore);
        }
    }

    private static long srpRequests(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                 .getColumnFamilyStore("t")
                                                 .metric
                                                 .shortReadProtectionRequests
                                                 .getCount());
    }
}
