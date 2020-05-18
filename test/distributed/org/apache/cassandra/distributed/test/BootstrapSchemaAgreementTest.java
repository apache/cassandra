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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Builder;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.MigrationTask;
import org.apache.cassandra.service.StorageService;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BootstrapSchemaAgreementTest extends TestBaseImpl
{
    private Builder<IInstance, ICluster> getClusterBuilder()
    {
        return builder().withNodes(2)
                 .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                 .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc1", "rack1"))
                 .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP, Feature.NATIVE_PROTOCOL));
    }

    private void createTable(ICluster<IInvokableInstance> cluster)
    {
        cluster.get(1).executeInternal("CREATE KEYSPACE " + KEYSPACE + " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}");
        cluster.get(1).executeInternal(String.format("CREATE TABLE %s.cf (k text, c1 text, PRIMARY KEY (k))", KEYSPACE));
    }

    @Test
    public void test() {

    }

//    @Test
//    public void testGlobalTimeoutReachedOnSchemaAgreement() throws Throwable
//    {
//        try (ICluster<IInvokableInstance> cluster = getClusterBuilder().createWithoutStarting())
//        {
//            cluster.get(1).startup();
//
//            createTable(cluster);
//
//            cluster.get(2).startup();
//
//            boolean reachedGlobalTimeout = cluster.get(2).callsOnInstance(() -> {
//
//                EventuallySucceedingMigrationTaskCallback callback = null;
//
//                try
//                {
//                    // global timeout (10s) is lower than our 5 attempts, 3 seconds each
//                    System.setProperty("cassandra.migration_task_wait_in_seconds", "3");
//                    System.setProperty("cassandra.migration_task_global_wait_in_seconds", "10");
//
//                    // pretend we are going to pull a schema from the first node
//                    // and it fails 5 times in a row, this will reach global timeout
//                    callback = new EventuallySucceedingMigrationTaskCallback(InetAddress.getByName("127.0.0.1"), 5);
//
//                    Set<MigrationTask.MigrationTaskCallback> callbacks = Sets.newHashSet(callback);
//
//                    // this will throw exception
//                    StorageService.instance.waitForSchemaWithCallbacks(0, callbacks);
//
//                    return false;
//                }
//                catch (ConfigurationException ex)
//                {
//                    // this should be reached as we passed global timeout
//                    return true;
//                }
//                catch (UnknownHostException e)
//                {
//                    throw new IllegalStateException("Unable to construct migration callback", e);
//                }
//                finally
//                {
//                    // set defaults back
//                    System.setProperty("cassandra.migration_task_wait_in_seconds", "1");
//                    System.setProperty("cassandra.migration_task_global_wait_in_seconds", "300");
//                }
//            }).call();
//
//            assertThat(reachedGlobalTimeout, is(true));
//        }
//    }
//
//    @Test
//    public void testEventuallySucceedingSchemaMigration() throws Throwable
//    {
//        try (ICluster<IInvokableInstance> cluster = getClusterBuilder().createWithoutStarting())
//        {
//            cluster.get(1).startup();
//            createTable(cluster);
//
//            cluster.get(2).startup();
//
//            Boolean node2SchemaWaitSuccessful = cluster.get(2).callsOnInstance(() -> {
//
//                EventuallySucceedingMigrationTaskCallback callback = null;
//
//                try
//                {
//                    // pretend we are going to pull a schema from the first node and it fails 3 times in a row
//                    callback = new EventuallySucceedingMigrationTaskCallback(InetAddress.getByName("127.0.0.1"), 3);
//                }
//                catch (UnknownHostException e)
//                {
//                    throw new IllegalStateException("Unable to construct migration callback", e);
//                }
//
//                Set<MigrationTask.MigrationTaskCallback> callbacks = Sets.newHashSet(callback);
//
//                StorageService.instance.waitForSchemaWithCallbacks(0, callbacks);
//
//                return !callback.failed && callback.responseCalled && callback.successful;
//            }).call();
//
//            assertThat(node2SchemaWaitSuccessful, is(true));
//        }
//    }
//
//    private static class EventuallySucceedingMigrationTaskCallback extends MigrationTask.MigrationTaskCallback
//    {
//        protected boolean successful;
//        protected boolean failed;
//        protected boolean onFailureCalled;
//        protected boolean responseCalled;
//
//        private int attempts;
//
//        public EventuallySucceedingMigrationTaskCallback(InetAddress endpoint, int attempts)
//        {
//            super(endpoint);
//            this.attempts = attempts;
//        }
//
//        public boolean isRunningForcibly()
//        {
//            return !successful;
//        }
//
//        public void response(MessageIn<Collection<Mutation>> message)
//        {
//            if (attempts == 0)
//            {
//                super.response(message);
//                responseCalled = true;
//                successful = true;
//                return;
//            }
//
//            attempts -= 1;
//
//            // here we pretend that no schema merging was done
//            // super.response(message);
//
//            // simulating of finally block
//            MigrationTask.completedInFlightSchemaRequest(getEndpoint());
//        }
//
//        public void onFailure(InetAddress from, RequestFailureReason failureReason)
//        {
//            super.onFailure(from, failureReason);
//            failed = true;
//            onFailureCalled = true;
//        }
//    }
}