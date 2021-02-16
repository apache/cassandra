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

package org.apache.cassandra.schema;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static com.google.common.util.concurrent.Futures.getUnchecked;

public class MigrationCoordinatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationCoordinatorTest.class);

    private static final InetAddressAndPort EP1;
    private static final InetAddressAndPort EP2;
    private static final InetAddressAndPort EP3;

    private static final UUID LOCAL_VERSION = UUID.randomUUID();
    private static final UUID V1 = UUID.randomUUID();
    private static final UUID V2 = UUID.randomUUID();

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("10.0.0.1");
            EP2 = InetAddressAndPort.getByName("10.0.0.2");
            EP3 = InetAddressAndPort.getByName("10.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }

        DatabaseDescriptor.daemonInitialization();
    }

    private static class InstrumentedCoordinator extends MigrationCoordinator
    {

        Queue<Callback> requests = new LinkedList<>();
        @Override
        protected void sendMigrationMessage(MigrationCoordinator.Callback callback)
        {
            requests.add(callback);
        }

        boolean shouldPullSchema = true;
        @Override
        protected boolean shouldPullSchema(UUID version)
        {
            return shouldPullSchema;
        }

        boolean shouldPullFromEndpoint = true;
        @Override
        protected boolean shouldPullFromEndpoint(InetAddressAndPort endpoint)
        {
            return shouldPullFromEndpoint;
        }

        boolean shouldPullImmediately = true;
        @Override
        protected boolean shouldPullImmediately(InetAddressAndPort endpoint, UUID version)
        {
            return shouldPullImmediately;
        }

        Set<InetAddressAndPort> deadNodes = new HashSet<>();
        protected boolean isAlive(InetAddressAndPort endpoint)
        {
            return !deadNodes.contains(endpoint);
        }

        UUID localVersion = LOCAL_VERSION;
        @Override
        protected boolean isLocalVersion(UUID version)
        {
            return localVersion.equals(version);
        }

        int maxOutstandingRequests = 3;
        @Override
        protected int getMaxOutstandingVersionRequests()
        {
            return maxOutstandingRequests;
        }

        Set<InetAddressAndPort> mergedSchemasFrom = new HashSet<>();
        @Override
        protected void mergeSchemaFrom(InetAddressAndPort endpoint, Collection<Mutation> mutations)
        {
            mergedSchemasFrom.add(endpoint);
        }
    }

    @Test
    public void requestResponseCycle() throws InterruptedException
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();
        coordinator.maxOutstandingRequests = 1;

        Assert.assertTrue(coordinator.requests.isEmpty());

        // first schema report should send a migration request
        getUnchecked(coordinator.reportEndpointVersion(EP1, V1));
        Assert.assertEquals(1, coordinator.requests.size());
        Assert.assertFalse(coordinator.awaitSchemaRequests(1));

        // second should not
        getUnchecked(coordinator.reportEndpointVersion(EP2, V1));
        Assert.assertEquals(1, coordinator.requests.size());
        Assert.assertFalse(coordinator.awaitSchemaRequests(1));

        // until the first request fails, then the second endpoint should be contacted
        MigrationCoordinator.Callback request1 = coordinator.requests.poll();
        Assert.assertEquals(EP1, request1.endpoint);
        getUnchecked(request1.fail());
        Assert.assertTrue(coordinator.mergedSchemasFrom.isEmpty());
        Assert.assertFalse(coordinator.awaitSchemaRequests(1));

        // ... then the second endpoint should be contacted
        Assert.assertEquals(1, coordinator.requests.size());
        MigrationCoordinator.Callback request2 = coordinator.requests.poll();
        Assert.assertEquals(EP2, request2.endpoint);
        Assert.assertFalse(coordinator.awaitSchemaRequests(1));
        getUnchecked(request2.response(Collections.emptyList()));
        Assert.assertEquals(EP2, Iterables.getOnlyElement(coordinator.mergedSchemasFrom));
        Assert.assertTrue(coordinator.awaitSchemaRequests(1));

        // and migration tasks should not be sent out for subsequent version reports
        getUnchecked(coordinator.reportEndpointVersion(EP3, V1));
        Assert.assertTrue(coordinator.requests.isEmpty());

    }

    /**
     * If we don't send a request for a version, and endpoints associated with
     * it all change versions, we should signal anyone waiting on that version
     */
    @Test
    public void versionsAreSignaledWhenDeleted()
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();

        coordinator.reportEndpointVersion(EP1, V1);
        WaitQueue.Signal signal = coordinator.getVersionInfoUnsafe(V1).register();
        Assert.assertFalse(signal.isSignalled());

        coordinator.reportEndpointVersion(EP1, V2);
        Assert.assertNull(coordinator.getVersionInfoUnsafe(V1));

        Assert.assertTrue(signal.isSignalled());
    }

    private static void assertNoContact(InstrumentedCoordinator coordinator, InetAddressAndPort endpoint, UUID version, boolean startupShouldBeUnblocked)
    {
        Assert.assertTrue(coordinator.requests.isEmpty());
        Future<Void> future = coordinator.reportEndpointVersion(EP1, V1);
        if (future != null)
            getUnchecked(future);
        Assert.assertTrue(coordinator.requests.isEmpty());

        Assert.assertEquals(startupShouldBeUnblocked, coordinator.awaitSchemaRequests(1));
    }

    private static void assertNoContact(InstrumentedCoordinator coordinator, boolean startupShouldBeUnblocked)
    {
        assertNoContact(coordinator, EP1, V1, startupShouldBeUnblocked);
    }

    @Test
    public void dontContactNodesWithSameSchema()
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();

        coordinator.localVersion = V1;
        assertNoContact(coordinator, true);
    }

    @Test
    public void dontContactIncompatibleNodes()
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();

        coordinator.shouldPullFromEndpoint = false;
        assertNoContact(coordinator, false);
    }

    @Test
    public void dontContactDeadNodes()
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();

        coordinator.deadNodes.add(EP1);
        assertNoContact(coordinator, EP1, V1, false);
    }

    /**
     * If a node has become incompativle between when the task was scheduled and when it
     * was run, we should detect that and fail the task
     */
    @Test
    public void testGossipRace()
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator() {
            protected boolean shouldPullImmediately(InetAddressAndPort endpoint, UUID version)
            {
                // this is the last thing that gets called before scheduling the pull, so set this flag here
                shouldPullFromEndpoint = false;
                return super.shouldPullImmediately(endpoint, version);
            }
        };

        Assert.assertTrue(coordinator.shouldPullFromEndpoint(EP1));
        assertNoContact(coordinator, EP1, V1, false);
    }

    @Test
    public void testWeKeepSendingRequests() throws Exception
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();

        getUnchecked(coordinator.reportEndpointVersion(EP3, V2));
        coordinator.requests.remove().response(Collections.emptyList());

        getUnchecked(coordinator.reportEndpointVersion(EP1, V1));
        getUnchecked(coordinator.reportEndpointVersion(EP2, V1));

        MigrationCoordinator.Callback prev = null;
        Set<InetAddressAndPort> EPs = Sets.newHashSet(EP1, EP2);
        int ep1requests = 0;
        int ep2requests = 0;

        for (int i=0; i<10; i++)
        {
            Assert.assertEquals(String.format("%s", i), 2, coordinator.requests.size());

            MigrationCoordinator.Callback next = coordinator.requests.remove();

            // we should be contacting endpoints in a round robin fashion
            Assert.assertTrue(EPs.contains(next.endpoint));
            if (prev != null && prev.endpoint.equals(next.endpoint))
                Assert.fail(String.format("Not expecting prev %s to be equal to next %s", prev.endpoint, next.endpoint));

            // should send a new request
            next.fail().get();
            prev = next;
            Assert.assertFalse(coordinator.awaitSchemaRequests(1));

            Assert.assertEquals(2, coordinator.requests.size());
        }
        logger.info("{} -> {}", EP1, ep1requests);
        logger.info("{} -> {}", EP2, ep2requests);

        // a single success should unblock startup though
        coordinator.requests.remove().response(Collections.emptyList());
        Assert.assertTrue(coordinator.awaitSchemaRequests(1));

    }

    /**
     * Pull unreceived schemas should detect and send requests out for any
     * schemas that are marked unreceived and have no outstanding requests
     */
    @Test
    public void pullUnreceived()
    {
        InstrumentedCoordinator coordinator = new InstrumentedCoordinator();

        coordinator.shouldPullFromEndpoint = false;
        assertNoContact(coordinator, false);

        coordinator.shouldPullFromEndpoint = true;
        Assert.assertEquals(0, coordinator.requests.size());
        List<Future<Void>> futures = coordinator.pullUnreceivedSchemaVersions();
        futures.forEach(Futures::getUnchecked);
        Assert.assertEquals(1, coordinator.requests.size());
    }
}
