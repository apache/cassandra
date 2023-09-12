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
import java.net.InetAddress;
import java.util.function.Consumer;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.distributed.util.SingleHostLoadBalancingPolicy;

import static com.datastax.driver.core.Cluster.Builder;
import static java.lang.String.format;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_NAME;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests for dynamic data masking.
 */
public class ColumnMaskTest extends TestBaseImpl
{
    private static final String SELECT = withKeyspace("SELECT * FROM %s.t");
    private static final String USERNAME = "ddm_user";
    private static final String PASSWORD = "ddm_password";

    /**
     * Tests that column masks are propagated to all nodes in the cluster.
     */
    @Test
    public void testMaskPropagation() throws Throwable
    {
        try (Cluster cluster = createClusterWithAuhentication(3))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text MASKED WITH DEFAULT) WITH read_repair='NONE'"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (1, 'secret1')"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (2, 'secret2')"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (3, 'secret3')"));

            assertRowsInAllCoordinators(cluster, row(1, "****"), row(2, "****"), row(3, "****"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v DROP MASKED"));
            assertRowsInAllCoordinators(cluster, row(1, "secret1"), row(2, "secret2"), row(3, "secret3"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v MASKED WITH mask_inner(null, 1)"));
            assertRowsInAllCoordinators(cluster, row(1, "******1"), row(2, "******2"), row(3, "******3"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v MASKED WITH mask_inner(3, null)"));
            assertRowsInAllCoordinators(cluster, row(1, "sec****"), row(2, "sec****"), row(3, "sec****"));
        }
    }

    /**
     * Tests that column masks are properly loaded at startup.
     */
    @Test
    public void testMaskLoading() throws Throwable
    {
        try (Cluster cluster = createClusterWithAuhentication(1))
        {
            IInvokableInstance node = cluster.get(1);

            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.custom_mask(column text, replacement text) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS text " +
                                              "LANGUAGE java " +
                                              "AS 'return replacement;'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (" +
                                              "a text MASKED WITH DEFAULT, " +
                                              "b text MASKED WITH mask_replace('redacted'), " +
                                              "c text MASKED WITH mask_inner(null, 1), " +
                                              "d text MASKED WITH mask_inner(3, null), " +
                                              "e text MASKED WITH %<s.custom_mask('obscured'), " +
                                              "PRIMARY KEY (a, b))"));
            String insert = withKeyspace("INSERT INTO %s.t(a, b, c, d, e) VALUES (?, ?, ?, ?, ?)");
            node.executeInternal(insert, "secret1", "secret1", "secret1", "secret1", "secret1");
            node.executeInternal(insert, "secret2", "secret2", "secret2", "secret2", "secret2");
            assertRowsWithRestart(node,
                                  row("****", "redacted", "******1", "sec****", "obscured"),
                                  row("****", "redacted", "******2", "sec****", "obscured"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER a DROP MASKED"));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER b MASKED WITH mask_null()"));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER c MASKED WITH mask_inner(null, null, '#')"));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER d MASKED WITH mask_inner(3, 1, '#')"));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER e MASKED WITH %<s.custom_mask('censored')"));
            assertRowsWithRestart(node,
                                  row("secret1", null, "#######", "sec###1", "censored"),
                                  row("secret2", null, "#######", "sec###2", "censored"));
        }
    }

    private static Cluster createClusterWithAuhentication(int nodeCount) throws IOException
    {
        Cluster cluster = init(Cluster.build()
                                      .withNodes(nodeCount)
                                      .withConfig(conf -> conf.with(GOSSIP, NATIVE_PROTOCOL)
                                                              .set("dynamic_data_masking_enabled", "true")
                                                              .set("user_defined_functions_enabled", "true")
                                                              .set("authenticator", "PasswordAuthenticator")
                                                              .set("authorizer", "CassandraAuthorizer"))
                                      .start());

        // create a user without UNMASK permission
        withAuthenticatedSession(cluster.get(1), DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
            session.execute(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USERNAME, PASSWORD));
            session.execute(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, USERNAME));
            session.execute(format("REVOKE UNMASK ON KEYSPACE %s FROM %s", KEYSPACE, USERNAME));
        });

        return cluster;
    }

    private static void assertRowsInAllCoordinators(Cluster cluster, Object[]... expectedRows)
    {
        for (int i = 1; i < cluster.size(); i++)
        {
            assertRowsWithAuthentication(cluster.get(i), expectedRows);
        }
    }

    private static void assertRowsWithRestart(IInvokableInstance node, Object[]... expectedRows) throws Throwable
    {
        // test querying with in-memory column definitions
        assertRowsWithAuthentication(node, expectedRows);

        // restart the nodes to reload the column definitions from disk
        node.shutdown().get();
        node.startup();

        // test querying with the column definitions loaded from disk
        assertRowsWithAuthentication(node, expectedRows);
    }

    private static void assertRowsWithAuthentication(IInvokableInstance node, Object[]... expectedRows)
    {
        withAuthenticatedSession(node, USERNAME, PASSWORD, session -> {
            Statement statement = new SimpleStatement(SELECT).setConsistencyLevel(ConsistencyLevel.ALL);
            ResultSet resultSet = session.execute(statement);
            assertRows(RowUtil.toObjects(resultSet), expectedRows);
        });
    }

    private static void withAuthenticatedSession(IInvokableInstance instance, String username, String password, Consumer<Session> consumer)
    {
        // wait for existing roles
        Auth.waitForExistingRoles(instance);

        // use a load balancing policy that ensures that we actually connect to the desired node
        InetAddress address = instance.broadcastAddress().getAddress();
        LoadBalancingPolicy lbc = new SingleHostLoadBalancingPolicy(address);

        Builder builder = com.datastax.driver.core.Cluster.builder()
                                                          .addContactPoints(address)
                                                          .withLoadBalancingPolicy(lbc)
                                                          .withCredentials(username, password);

        try (com.datastax.driver.core.Cluster cluster = builder.build();
             Session session = cluster.connect())
        {
            consumer.accept(session);
        }
    }
}
