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

package org.apache.cassandra.service.reads;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicaPlan;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadExecutorTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static EndpointsForToken targets;
    static Token dummy;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.simple(3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        dummy = Murmur3Partitioner.instance.getMinimumToken();
        targets = EndpointsForToken.of(dummy,
                full(InetAddressAndPort.getByName("127.0.0.255")),
                full(InetAddressAndPort.getByName("127.0.0.254")),
                full(InetAddressAndPort.getByName("127.0.0.253"))
        );
        cfs.sampleReadLatencyMicros = 0;
    }

    @Before
    public void resetCounters() throws Throwable
    {
        cfs.metric.speculativeInsufficientReplicas.dec(cfs.metric.speculativeInsufficientReplicas.getCount());
        cfs.metric.speculativeRetries.dec(cfs.metric.speculativeRetries.getCount());
        cfs.metric.speculativeFailedRetries.dec(cfs.metric.speculativeFailedRetries.getCount());
    }

    /**
     * If speculation would have been beneficial but could not be attempted due to lack of replicas
     * count that it occured
     */
    @Test
    public void testUnableToSpeculate() throws Throwable
    {
        assertEquals(0, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(0, ks.metric.speculativeInsufficientReplicas.getCount());
        AbstractReadExecutor executor = new AbstractReadExecutor.NeverSpeculatingReadExecutor(cfs, new MockSinglePartitionReadCommand(), plan(targets, LOCAL_QUORUM), nanoTime(), true);
        executor.maybeTryAdditionalReplicas();
        try
        {
            executor.awaitResponses();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }
        assertEquals(1, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(1, ks.metric.speculativeInsufficientReplicas.getCount());

        //Shouldn't increment
        executor = new AbstractReadExecutor.NeverSpeculatingReadExecutor(cfs, new MockSinglePartitionReadCommand(), plan(targets, LOCAL_QUORUM), nanoTime(), false);
        executor.maybeTryAdditionalReplicas();
        try
        {
            executor.awaitResponses();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }
        assertEquals(1, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(1, ks.metric.speculativeInsufficientReplicas.getCount());
    }

    /**
     *  Test that speculation when it is attempted is countedc, and when it succeed
     *  no failure is counted.
     */
    @Test
    public void testSpeculateSucceeded() throws Throwable
    {
        assertEquals(0, cfs.metric.speculativeRetries.getCount());
        assertEquals(0, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(0, ks.metric.speculativeRetries.getCount());
        assertEquals(0, ks.metric.speculativeFailedRetries.getCount());
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, new MockSinglePartitionReadCommand(DAYS.toMillis(365)), plan(LOCAL_QUORUM, targets, targets.subList(0, 2)), nanoTime());
        executor.maybeTryAdditionalReplicas();
        new Thread()
        {
            @Override
            public void run()
            {
                //Failures end the read promptly but don't require mock data to be suppleid
                executor.handler.onFailure(targets.get(0).endpoint(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                executor.handler.onFailure(targets.get(1).endpoint(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                executor.handler.condition.signalAll();
            }
        }.start();

        try
        {
            executor.awaitResponses();
            fail();
        }
        catch (ReadFailureException e)
        {
            //expected
        }
        assertEquals(1, cfs.metric.speculativeRetries.getCount());
        assertEquals(0, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(1, ks.metric.speculativeRetries.getCount());
        assertEquals(0, ks.metric.speculativeFailedRetries.getCount());

    }

    /**
     * Test that speculation failure statistics are incremented if speculation occurs
     * and the read still times out.
     */
    @Test
    public void testSpeculateFailed() throws Throwable
    {
        assertEquals(0, cfs.metric.speculativeRetries.getCount());
        assertEquals(0, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(0, ks.metric.speculativeRetries.getCount());
        assertEquals(0, ks.metric.speculativeFailedRetries.getCount());
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, new MockSinglePartitionReadCommand(), plan(LOCAL_QUORUM, targets, targets.subList(0, 2)), nanoTime());
        executor.maybeTryAdditionalReplicas();
        try
        {
            executor.awaitResponses();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }
        assertEquals(1, cfs.metric.speculativeRetries.getCount());
        assertEquals(1, cfs.metric.speculativeFailedRetries.getCount());
        assertEquals(1, ks.metric.speculativeRetries.getCount());
        assertEquals(1, ks.metric.speculativeFailedRetries.getCount());
    }

    /**
     * Test that an async speculative execution racing with a local errored request does not violate assertions.
     * CASSANDRA-16097
     */
    @Test
    public void testRaceWithNonSpeculativeFailure()
    {
        MockSinglePartitionReadCommand command = new MockSinglePartitionReadCommand(TimeUnit.DAYS.toMillis(365));
        ReplicaPlan.ForTokenRead plan = plan(ConsistencyLevel.LOCAL_ONE, targets, targets.subList(0, 1));
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command, plan, nanoTime());

        // Issue an initial request against the first endpoint...
        executor.executeAsync();

        // ...and then force a speculative retry against another endpoint.
        cfs.sampleReadLatencyMicros = 0L;
        executor.maybeTryAdditionalReplicas();

        new Thread(() ->
                   {
                       // Fail the first request. When this fails the number of contacts has already been increased
                       // to 2, so the failure won't actally signal. However...
                       executor.handler.onFailure(targets.get(0).endpoint(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);

                       // ...speculative retries are fired after a short wait, and it is possible for the failure to
                       // reach the handler just before one is fired and the number of contacts incremented...
                       executor.handler.condition.signalAll();
                   }).start();

        try
        {
            // ...but by the time we await for results, the number of contacts may already have been incremented.
            // If we rely only on the number of failures and the number of nodes blocked for, compared to the number
            // of contacts, we may not recognize that the query has failed.
            executor.awaitResponses();
            fail("Awaiting responses did not throw a ReadFailureException as expected.");
        }
        catch (Throwable t)
        {
            assertSame(ExceptionUtils.getStackTrace(t), ReadFailureException.class, t.getClass());
            assertTrue(t.getMessage().contains(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.name()));
        }
    }

    public static class MockSinglePartitionReadCommand extends SinglePartitionReadCommand
    {
        private final long timeout;

        MockSinglePartitionReadCommand()
        {
            this(0);
        }

        MockSinglePartitionReadCommand(long timeout)
        {
            super(false, 0, false, cfs.metadata(), 0, null, null, null, Util.dk("ry@n_luvs_teh_y@nk33z"), null, null, false);
            this.timeout = timeout;
        }

        @Override
        public long getTimeout(TimeUnit unit)
        {
            return unit.convert(timeout, MILLISECONDS);
        }

        @Override
        public Message createMessage(boolean trackRepairedData)
        {
            return Message.out(Verb.ECHO_REQ, NoPayload.noPayload);
        }
    }

    private ReplicaPlan.ForTokenRead plan(EndpointsForToken targets, ConsistencyLevel consistencyLevel)
    {
        return plan(consistencyLevel, targets, targets);
    }

    private ReplicaPlan.ForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken natural, EndpointsForToken selected)
    {
        return new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, natural, selected);
    }
}
