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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;

public class ClusterMetadataUpgradePreparedStatementsTest extends UpgradeTestBase
{
    @Test
    public void simpleUpgradeTest() throws Throwable
    {
        new UpgradeTestBase.TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                              .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
        })
        .runAfterClusterUpgrade((cluster) -> {
            String node1Address = cluster.get(1).config().broadcastAddress().getHostString();
            com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                                               .addContactPoint(node1Address);
            try (com.datastax.driver.core.Cluster c = builder.build();
                 Session session = c.connect())
            {
                PreparedStatement ps = session.prepare(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?)"));
                PreparedStatement ps2 = session.prepare(withKeyspace("select pk, ck, v from %s.tbl where pk = ?"));
                for (int i = 0; i < 10; i++)
                {
                    session.execute(ps.bind(i, 2, 3));
                    session.execute(ps2.bind(i));
                }
                assertPSCache(cluster.get(1), false, 0);
                cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
                assertPSCache(cluster.get(1), false, 0);
                for (int i = 0; i < 10; i++)
                {
                    session.execute(ps.bind(i, 3, 3));
                    session.execute(ps2.bind(i));
                }
                session.execute(withKeyspace("alter table %s.tbl add x int"));

                assertPSCacheEmpty(cluster.get(1));

                for (int i = 0; i < 10; i++)
                {
                    session.execute(ps.bind(i, 3, 3));
                    session.execute(ps2.bind(i));
                }
                assertPSCache(cluster.get(1), false, 4);
            }
        }).run();
    }

    private static void assertPSCacheEmpty(IUpgradeableInstance inst)
    {
        assertPSCache(inst, true, -1);
    }

    private static void assertPSCache(IUpgradeableInstance inst, boolean shouldBeEmpty, long expectedEpoch)
    {
        ((IInvokableInstance)inst).runOnInstance(() -> {
            if (shouldBeEmpty != QueryProcessor.instance.getPreparedStatements().isEmpty())
                throw new AssertionError("Prepared statements should not be empty after `cms initialize`");

            for (QueryHandler.Prepared p : QueryProcessor.instance.getPreparedStatements().values())
            {
                long statementEpoch = -1;
                if (p.statement instanceof SelectStatement)
                    statementEpoch = ((SelectStatement)p.statement).table.epoch.getEpoch();
                else if (p.statement instanceof UpdateStatement)
                    statementEpoch = ((UpdateStatement)p.statement).metadata.epoch.getEpoch();

                if (statementEpoch != expectedEpoch)
                    throw new AssertionError(String.format("Statement %s has the wrong epoch: %d != %d ", p.statement, statementEpoch, expectedEpoch));
            }
        });
    }

}
