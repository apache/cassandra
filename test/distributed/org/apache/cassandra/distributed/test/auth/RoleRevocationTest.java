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

import java.util.function.Consumer;

import org.junit.Test;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class RoleRevocationTest extends TestBaseImpl
{
    // Ensure both the coordinator of the DDL and a replica both change connection state accordingly
    private static final int CLUSTER_SIZE = 2;
    private static final String USERNAME = "revoke_me";
    private static final String PASSWORD = "i_deserve_disconnection";
    private static final PlainTextAuthProvider CLIENT_AUTH_PROVIDER = new PlainTextAuthProvider(USERNAME, PASSWORD);
    private static final ReconnectionPolicy RECONNECTION_POLICY = new ConstantReconnectionPolicy(100);

    public static ICluster<IInvokableInstance> cluster() throws Exception
    {
        Cluster.Builder builder = Cluster.build(CLUSTER_SIZE);

        builder.withConfig(c -> c.set("authenticator.class_name", "org.apache.cassandra.auth.PasswordAuthenticator")
                                 .set("role_manager", "CassandraRoleManager")
                                 .set("authorizer", "CassandraAuthorizer")
                                 .with(Feature.NETWORK, Feature.NATIVE_PROTOCOL, Feature.GOSSIP));
        ICluster<IInvokableInstance> cluster = builder.start();

        cluster.get(1).runOnInstance(() -> {
            RoleOptions opts = new RoleOptions();
            opts.setOption(IRoleManager.Option.PASSWORD, PASSWORD);
            opts.setOption(IRoleManager.Option.LOGIN, true);
            DatabaseDescriptor.getRoleManager().createRole(AuthenticatedUser.SYSTEM_USER, RoleResource.role(USERNAME), opts);
        });

        return cluster;
    }

    @Test
    public void alterRoleLoginFalse() throws Exception
    {
        test(instance -> {
            instance.runOnInstance(() -> {
                RoleOptions opts = new RoleOptions();
                opts.setOption(IRoleManager.Option.LOGIN, false);
                DatabaseDescriptor.getRoleManager().alterRole(AuthenticatedUser.SYSTEM_USER, RoleResource.role(USERNAME), opts);
            });
        });
    }

    @Test
    public void dropRole() throws Exception
    {
        test(instance -> {
            instance.runOnInstance(() -> {
                DatabaseDescriptor.getRoleManager().dropRole(AuthenticatedUser.SYSTEM_USER, RoleResource.role(USERNAME));
            });
        });
    }

    private void test(Consumer<IInvokableInstance> action) throws Exception
    {
        ICluster<IInvokableInstance> CLUSTER = cluster();
        com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(CLUSTER, null, builder -> builder
                                                                                                   .withAuthProvider(CLIENT_AUTH_PROVIDER)
                                                                                                   .withReconnectionPolicy(RECONNECTION_POLICY));
        Session session = driver.connect();

        // One control, one data connection per host
        Assertions.assertThat(driver.getMetrics().getOpenConnections().getValue()).isEqualTo(CLUSTER_SIZE + 1);
        Assertions.assertThat(driver.getMetrics().getConnectedToHosts().getValue()).isEqualTo(CLUSTER_SIZE);
        Assertions.assertThat(driver.getMetrics().getErrorMetrics().getAuthenticationErrors().getCount()).isEqualTo(0);

        action.accept(CLUSTER.get(1));

        CLUSTER.forEach(instance -> {
            instance.runOnInstance(() -> {
                Roles.cache.invalidate();
                StorageService.instance.disconnectInvalidRoles();
            });
        });

        await().pollDelay(100, MILLISECONDS)
               .pollInterval(100, MILLISECONDS)
               .atMost(10, SECONDS)
               .untilAsserted(() -> {
                   // Should disconnect from both the coordinator of the DDL and the replica that is notified
                   Assertions.assertThat(driver.getMetrics().getOpenConnections().getValue()).isEqualTo(0);
                   Assertions.assertThat(driver.getMetrics().getConnectedToHosts().getValue()).isEqualTo(0);
               });

        await().pollDelay(100, MILLISECONDS)
               .pollInterval(100, MILLISECONDS)
               .atMost(10, SECONDS)
               .untilAsserted(() -> {
                   long authErrors = session.getCluster().getMetrics().getErrorMetrics().getAuthenticationErrors().getCount();
                   Assertions.assertThat(authErrors).isGreaterThan(0);
               });

        session.close();
        driver.close();
        CLUSTER.close();
    }
}