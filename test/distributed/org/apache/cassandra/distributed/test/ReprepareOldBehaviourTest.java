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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ReprepareOldBehaviourTest extends ReprepareTestBase
{
    @Test
    public void testReprepareMixedVersion() throws Throwable
    {
        testReprepare(PrepareBehaviour::oldBehaviour,
                      cfg(true, true),
                      cfg(false, false));
    }

    @Test
    public void testReprepareTwoKeyspacesMixedVersion() throws Throwable
    {
        testReprepareTwoKeyspaces(PrepareBehaviour::oldBehaviour);
    }

    @Test
    public void testReprepareUsingOldBehavior() throws Throwable
    {
        // fork of testReprepareMixedVersionWithoutReset, but makes sure oldBehavior has a clean state
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(PrepareBehaviour::oldBehaviour)
                                                            .start()))
        {
            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                            .addContactPoint("127.0.0.1")
                                                                                            .addContactPoint("127.0.0.2")
                                                                                            .withLoadBalancingPolicy(lbp)
                                                                                            .build();
                 Session session = cluster.connect())
            {
                session.execute(withKeyspace("USE %s"));

                lbp.setPrimary(2);
                final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                session.execute(select.bind());

                lbp.setPrimary(1);
                session.execute(select.bind());
            }
        }
    }

    @Test
    public void testReprepareMixedVersionWithoutReset() throws Throwable
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(PrepareBehaviour::oldBehaviour)
                                                            .start()))
        {
            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // 1 has old behaviour
            for (int firstContact : new int[]{ 1, 2 })
            {
                for (boolean withUse : new boolean[]{ true, false })
                {
                    for (boolean clearBetweenExecutions : new boolean[]{ true, false })
                    {
                        try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                                        .addContactPoint("127.0.0.1")
                                                                                                        .addContactPoint("127.0.0.2")
                                                                                                        .withLoadBalancingPolicy(lbp)
                                                                                                        .build();
                             Session session = cluster.connect())
                        {
                            if (withUse)
                                session.execute(withKeyspace("USE %s"));

                            lbp.setPrimary(firstContact);
                            final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                            session.execute(select.bind());

                            if (clearBetweenExecutions)
                                c.get(2).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                            lbp.setPrimary(firstContact == 1 ? 2 : 1);
                            session.execute(select.bind());

                            if (clearBetweenExecutions)
                                c.get(2).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                            lbp.setPrimary(firstContact);
                            session.execute(select.bind());

                            c.get(2).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                        }
                    }
                }
            }
        }
    }
}