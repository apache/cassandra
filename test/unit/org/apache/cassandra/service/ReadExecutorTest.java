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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
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
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReadExecutorTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static List<InetAddress> targets;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.simple(3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        targets = ImmutableList.of(InetAddress.getByName("127.0.0.255"), InetAddress.getByName("127.0.0.254"), InetAddress.getByName("127.0.0.253"));
        cfs.sampleLatencyNanos = 0;
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
        AbstractReadExecutor executor = new AbstractReadExecutor.NeverSpeculatingReadExecutor(ks, cfs, new MockSinglePartitionReadCommand(), ConsistencyLevel.LOCAL_QUORUM, targets, System.nanoTime(), true);
        executor.maybeTryAdditionalReplicas();
        try
        {
            executor.get();
            fail();
        }
        catch (ReadTimeoutException e)
        {
            //expected
        }
        assertEquals(1, cfs.metric.speculativeInsufficientReplicas.getCount());
        assertEquals(1, ks.metric.speculativeInsufficientReplicas.getCount());

        //Shouldn't increment
        executor = new AbstractReadExecutor.NeverSpeculatingReadExecutor(ks, cfs, new MockSinglePartitionReadCommand(), ConsistencyLevel.LOCAL_QUORUM, targets, System.nanoTime(), false);
        executor.maybeTryAdditionalReplicas();
        try
        {
            executor.get();
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
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(ks, cfs, new MockSinglePartitionReadCommand(TimeUnit.DAYS.toMillis(365)), ConsistencyLevel.LOCAL_QUORUM, targets, System.nanoTime());
        executor.maybeTryAdditionalReplicas();
        new Thread()
        {
            @Override
            public void run()
            {
                //Failures end the read promptly but don't require mock data to be suppleid
                executor.handler.onFailure(targets.get(0), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                executor.handler.onFailure(targets.get(1), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                executor.handler.condition.signalAll();
            }
        }.start();

        try
        {
            executor.get();
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
        AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(ks, cfs, new MockSinglePartitionReadCommand(), ConsistencyLevel.LOCAL_QUORUM, targets, System.nanoTime());
        executor.maybeTryAdditionalReplicas();
        try
        {
            executor.get();
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

    public static class MockSinglePartitionReadCommand extends SinglePartitionReadCommand
    {
        private final long timeout;

        MockSinglePartitionReadCommand()
        {
            this(0);
        }

        MockSinglePartitionReadCommand(long timeout)
        {
            super(false, 0, cfs.metadata(), 0, null, null, null, Util.dk("ry@n_luvs_teh_y@nk33z"), null, null);
            this.timeout = timeout;
        }

        @Override
        public long getTimeout()
        {
            return timeout;
        }

        @Override
        public MessageOut createMessage()
        {
            return new MessageOut(MessagingService.Verb.BATCH_REMOVE)
            {
                @Override
                public int serializedSize(int version)
                {
                    return 0;
                }
            };
        }

    }

}
