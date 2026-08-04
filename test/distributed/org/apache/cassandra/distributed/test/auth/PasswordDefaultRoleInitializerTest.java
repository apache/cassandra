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

package org.apache.cassandra.distributed.test.auth;

import java.net.InetAddress;
import java.util.function.Consumer;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.distributed.util.SingleHostLoadBalancingPolicy;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.auth.PasswordDefaultRoleInitializer.DEFAULT_SUPERUSER_NAME;
import static org.apache.cassandra.auth.PasswordDefaultRoleInitializer.DEFAULT_SUPERUSER_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the {@code default_role_initializer} refactor (see {@link org.apache.cassandra.auth.IDefaultRoleInitializer}):
 * when the option is left unconfigured, bootstrap must still fall back to {@link org.apache.cassandra.auth.PasswordDefaultRoleInitializer}
 * and create the classic {@code cassandra}/{@code cassandra} superuser exactly as it always has, so deployments and
 * tests that rely on the historical default (e.g. {@code CQLTester}) keep working unchanged.
 */
public class PasswordDefaultRoleInitializerTest extends TestBaseImpl
{
    private static ICluster<IInvokableInstance> CLUSTER;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        CLUSTER = Cluster.build(1)
                        .withConfig(conf -> conf.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                .set("authenticator", "PasswordAuthenticator")
                                                .set("authorizer", "CassandraAuthorizer")
                                                .set("role_manager", "CassandraRoleManager"))
                        .start();
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testClassicSuperuserBootstrappedByDefault()
    {
        Object[] roleRow = CLUSTER.get(1).callOnInstance((SerializableCallable<Object[]>) () -> {
            UntypedResultSet result = QueryProcessor.executeInternal(
                String.format("SELECT is_superuser, can_login, salted_hash FROM %s.%s WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, DEFAULT_SUPERUSER_NAME));
            if (result.isEmpty())
                return null;
            UntypedResultSet.Row row = result.one();
            return new Object[]{ row.getBoolean("is_superuser"), row.getBoolean("can_login"), row.has("salted_hash") };
        });

        assertThat(roleRow).isNotNull();
        assertThat((Boolean) roleRow[0]).as("is_superuser").isTrue();
        assertThat((Boolean) roleRow[1]).as("can_login").isTrue();
        assertThat((Boolean) roleRow[2]).as("has salted_hash").isTrue();

        // and the credential actually works end-to-end, not just the raw row contents
        withAuthenticatedSession(CLUSTER.get(1), DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
            com.datastax.driver.core.ResultSet rows = session.execute("SELECT role FROM system_auth.roles WHERE role = ?", DEFAULT_SUPERUSER_NAME);
            assertThat(rows.one().getString("role")).isEqualTo(DEFAULT_SUPERUSER_NAME);
        });
    }

    // No client_encryption_options are configured for this cluster, so unlike TlsTestUtils#withAuthenticatedSession
    // (which always requires SSLOptions) this connects in plaintext, matching ColumnMaskTest's local helper.
    private static void withAuthenticatedSession(IInvokableInstance instance, String username, String password, Consumer<Session> consumer)
    {
        Auth.waitForExistingRoles(instance);

        InetAddress address = instance.broadcastAddress().getAddress();
        LoadBalancingPolicy lbc = new SingleHostLoadBalancingPolicy(address);

        Builder builder = com.datastax.driver.core.Cluster.builder()
                                                          .addContactPoints(address)
                                                          .withLoadBalancingPolicy(lbc)
                                                          .withCredentials(username, password);

        try (com.datastax.driver.core.Cluster cluster = builder.build(); Session session = cluster.connect())
        {
            consumer.accept(session);
        }
    }
}
