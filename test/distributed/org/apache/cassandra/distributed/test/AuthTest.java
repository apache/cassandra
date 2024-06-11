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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Test;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters.Filter;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.service.StorageService;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthTest extends TestBaseImpl
{
    /**
     * Simply tests that initialisation of a test Instance results in
     * StorageService.instance.doAuthSetup being called as the regular
     * startup does in CassandraDaemon.setup
     */
    @Test
    public void authSetupIsCalledAfterStartup() throws IOException
    {
        try (Cluster cluster = Cluster.build().withNodes(1).start())
        {
            IInvokableInstance instance = cluster.get(1);
            await().pollDelay(1, SECONDS)
                   .pollInterval(1, SECONDS)
                   .atMost(10, SECONDS)
                   .until(() -> instance.callOnInstance(() -> StorageService.instance.authSetupCalled()));
        }
    }

    /**
     * See CASSANDRA-12525 for more information.
     */
    @Test
    public void testZeroTimestampForDefaultRoleCreation() throws Exception
    {
        try (Cluster cluster = builder().withDCs(2)
                                        .withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2, 1))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    .set("authenticator", "PasswordAuthenticator"))
                                        .start())
        {
            Auth.waitForExistingRoles(cluster.get(1));

            long writeTime = getPasswordWritetime(cluster.coordinator(1));
            // TIMESTAMP 0 in action
            assertEquals(0, writeTime);

            changePassword();
            long writeTimeAfterPasswordChange = getPasswordWritetime(cluster.coordinator(1));

            // timestamp was changed after we changed the password
            assertTrue(writeTime < writeTimeAfterPasswordChange);

            IInvokableInstance secondNode = getSecondNode(cluster);

            // drop all communication between nodes
            Filter to = cluster.filters().allVerbs().inbound().drop();
            Filter from = cluster.filters().allVerbs().outbound().drop();

            secondNode.startup();
            Auth.waitForExistingRoles(secondNode);

            long passwordWritetimeOnSecondNode = getPasswordWritetime(cluster.coordinator(2));

            // as new node thinks it is alone in cluster, it created new role with TIMESTAMP 0
            assertEquals(0, passwordWritetimeOnSecondNode);

            // the fact we can still log in with old password shows we dropped all communication
            // and the second node thinks that it is alone in the cluster, so it created new cassandra role
            // with default password
            doWithSession("127.0.0.2",
                          "datacenter2",
                          "cassandra", session -> session.execute("select * from system.local"));

            // turn off filters
            to.off();
            from.off();

            // be sure the first peer is there for the second node
            await()
            .atMost(1, TimeUnit.MINUTES)
            .pollInterval(10, SECONDS)
            .until(() -> {
                List<Row> rows = doWithSession("127.0.0.2",
                                               "datacenter2",
                                               "cassandra",
                                               session -> session.execute("select * from system.peers")).all();
                if (rows.isEmpty())
                    return false;

                return rows.get(0).getInet("peer").getHostAddress().equals("127.0.0.1");
            });

            // change the replication strategy
            doWithSession("127.0.0.2",
                          "datacenter2",
                          "cassandra",
                          session -> session.execute("ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1}"));

            // repair the second node so new password from the first node propagates to it
            assertEquals(0, secondNode.nodetool("repair", "--full"));

            // the second node was repaired, so it is using new password
            doWithSession("127.0.0.2",
                          "datacenter2",
                          "newpassword", session -> session.execute("select * from system.local"));

            // and the first node is still using new password after repair
            doWithSession("127.0.0.1",
                          "datacenter1",
                          "newpassword", session -> session.execute("select * from system.local"));
        }
    }

    private IInvokableInstance getSecondNode(Cluster cluster)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        // set both nodes as seed nodes in the list
        config.set("seed_provider", new IInstanceConfig.ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                           Collections.singletonMap("seeds", "127.0.0.1, 127.0.0.2")));
        return cluster.bootstrap(config);
    }

    private long getPasswordWritetime(ICoordinator coordinator)
    {
        return (Long) coordinator.execute("SELECT WRITETIME (salted_hash) from system_auth.roles where role = 'cassandra'",
                                          ConsistencyLevel.LOCAL_ONE)[0][0];
    }

    private void changePassword()
    {
        doWithSession("127.0.0.1", "datacenter1", "cassandra", (Function<Session, Void>) session -> {
            session.execute("ALTER ROLE cassandra WITH PASSWORD = 'newpassword'");
            return null;
        });
    }

    private <V> V doWithSession(String host, String datacenter, String password, Function<Session, V> fn)
    {
        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                                           .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy.Builder().withLocalDc(datacenter).build())
                                                                                           .withAuthProvider(new PlainTextAuthProvider("cassandra", password))
                                                                                           .addContactPoint(host);

        try (com.datastax.driver.core.Cluster c = builder.build(); Session session = c.connect())
        {
            return fn.apply(session);
        }
    }
}
