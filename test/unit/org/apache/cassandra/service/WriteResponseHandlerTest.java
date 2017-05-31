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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WriteResponseHandlerTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static List<InetAddress> targets;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddress.getByName("127.1.0.255"));
        metadata.updateHostId(UUID.randomUUID(), InetAddress.getByName("127.2.0.255"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddress endpoint)
            {
                return null;
            }

            public String getDatacenter(InetAddress endpoint)
            {
                byte[] address = endpoint.getAddress();
                if (address[1] == 1)
                    return "datacenter1";
                else
                    return "datacenter2";
            }

            public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress)
            {
                return null;
            }

            public void sortByProximity(InetAddress address, List<InetAddress> addresses)
            {

            }

            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
            {
                return false;
            }
        });
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        targets = ImmutableList.of(InetAddress.getByName("127.1.0.255"), InetAddress.getByName("127.1.0.254"), InetAddress.getByName("127.1.0.253"),
                                   InetAddress.getByName("127.2.0.255"), InetAddress.getByName("127.2.0.254"), InetAddress.getByName("127.2.0.253"));
    }

    @Before
    public void resetCounters()
    {
        ks.metric.writeFailedIdealCL.dec(ks.metric.writeFailedIdealCL.getCount());
    }

    /**
     * Validate that a successful write at ideal CL logs latency information. Also validates
     * DatacenterSyncWriteResponseHandler
     * @throws Throwable
     */
    @Test
    public void idealCLLatencyTracked() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        //Specify query start time in past to ensure minimum latency measurement
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, System.nanoTime() - TimeUnit.DAYS.toNanos(1));

        //dc1
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        //dc2
        awr.response(createDummyMessage(4));
        awr.response(createDummyMessage(5));

        //Don't need the others
        awr.expired();
        awr.expired();

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertTrue( TimeUnit.DAYS.toMicros(1) < ks.metric.idealCLWriteLatency.totalLatency.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that WriteResponseHandler does the right thing on success.
     * @throws Throwable
     */
    @Test
    public void idealCLWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ALL);

        //dc1
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        awr.response(createDummyMessage(2));
        //dc2
        awr.response(createDummyMessage(3));
        awr.response(createDummyMessage(4));
        awr.response(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that DatacenterWriteResponseHandler does the right thing on success.
     * @throws Throwable
     */
    @Test
    public void idealCLDatacenterWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM);

        //dc1
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        awr.response(createDummyMessage(2));
        //dc2
        awr.response(createDummyMessage(3));
        awr.response(createDummyMessage(4));
        awr.response(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that failing to achieve ideal CL increments the failure counter
     * @throws Throwable
     */
    @Test
    public void failedIdealCLIncrementsStat() throws Throwable
    {

        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        //Succeed in local DC
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        awr.response(createDummyMessage(2));

        //Fail in remote DC
        awr.expired();
        awr.expired();
        awr.expired();
        assertEquals(1, ks.metric.writeFailedIdealCL.getCount());
        assertEquals(0, ks.metric.idealCLWriteLatency.totalLatency.getCount());
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal)
    {
        return createWriteResponseHandler(cl, ideal, System.nanoTime());
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, long queryStartTime)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(targets, ImmutableList.of(), cl, new Runnable() {
            public void run()
            {

            }
        }, WriteType.SIMPLE, queryStartTime, ideal);
    }

    private static MessageIn createDummyMessage(int target)
    {
        return MessageIn.create(targets.get(target), null, null,  null, 0, 0L);
    }
}
