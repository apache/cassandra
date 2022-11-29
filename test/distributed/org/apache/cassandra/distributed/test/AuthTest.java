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
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
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
            boolean setupCalled = cluster.get(1).callOnInstance(() -> {
                long maxWait = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
                long start = System.nanoTime();
                while (!StorageService.instance.authSetupCalled() && System.nanoTime() - start < maxWait)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                return StorageService.instance.authSetupCalled();
            });
            assertTrue(setupCalled);
        }
    }

    /**
     * Sows that in some circumstances CassandraRoleManager will create the cassandra role twice
     */
    @Test
    public void authSetupIsCalledTwice() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP).set("authenticator", "PasswordAuthenticator"))
                                        .start())
        {
            boolean defaultRoleSetup = cluster.get(1).callOnInstance(() -> {
                long maxWait = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
                long start = System.nanoTime();
                while (!CassandraRoleManager.hasExistingRoles() && System.nanoTime() - start < maxWait)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                return CassandraRoleManager.hasExistingRoles();
            });
            assertTrue(defaultRoleSetup);

            //get the time from the the first role setup
            Long time1 = (Long)cluster.coordinator(1).execute("SELECT WRITETIME (salted_hash) from system_auth.roles where role = 'cassandra'",
                                           ConsistencyLevel.ONE)[0][0];



            IInstanceConfig config = cluster.newInstanceConfig();
            // set boostrap to false to simulate a seed node
            config.set("auto_bootstrap", false);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty(BOOTSTRAP_SCHEMA_DELAY_MS.getKey(), Integer.toString(90 * 1000),
                         () -> withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster)));
            newInstance.nodetoolResult("join").asserts().success();
            defaultRoleSetup =  newInstance.callOnInstance(() -> {
                long maxWait = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
                long start = System.nanoTime();
                while (!CassandraRoleManager.hasExistingRoles() && System.nanoTime() - start < maxWait)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                return CassandraRoleManager.hasExistingRoles() ;
            });
            assertTrue(defaultRoleSetup);

            // get write titme frome second role setup
            Long time2 = (Long)cluster.coordinator(1).execute("SELECT WRITETIME (salted_hash) from system_auth.roles where role = 'cassandra'",
                                                               ConsistencyLevel.ONE)[0][0];
            // we don't do this here but if the user changed the Cassandra user password it will be (read) reapired if the second node has a later
            // write timsestamp - check that this is not the case
            assertTrue(time1 >= time2);
        }
    }


}
