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

package org.apache.cassandra.repair;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RepairSessionTest extends CQLTester
{
    InetAddressAndPort remote;

    RepairSession session;

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void init() throws UnknownHostException
    {
         remote = InetAddressAndPort.getByName("127.0.0.2");
         createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // Set up RepairSession
        TimeUUID parentSessionId = nextTimeUUID();
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> repairRange = new Range<>(p.getToken(ByteBufferUtil.bytes(0)), p.getToken(ByteBufferUtil.bytes(100)));
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        session = new RepairSession(parentSessionId, new Scheduler.NoopScheduler(),
                                                  new CommonRange(endpoints, Collections.emptySet(), Arrays.asList(repairRange)),
                                                  KEYSPACE, RepairParallelism.SEQUENTIAL,
                                                  false, false,
                                                  PreviewKind.NONE, false, false, false, currentTable());
    }

    @Test
    public void testConviction()
    {
        long prevCount = ColumnFamilyStore.getIfExists(KEYSPACE, currentTable()).metric.repairFailuresDueToDownParticipants.getCount();
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        // perform convict
        session.convict(remote, Double.MAX_VALUE);

        // RepairSession should throw ExecutorException with the cause of IOException when getting its value
        try
        {
            session.get();
            fail();
        }
        catch (Exception ex)
        {
            assertEquals(IOException.class, ex.getCause().getClass());
        }
        assertEquals(prevCount + 1, ColumnFamilyStore.getIfExists(KEYSPACE, currentTable()).metric.repairFailuresDueToDownParticipants.getCount());
    }

    @Test
    public void testStartDeadParticipants() throws UnknownHostException
    {
        Gossiper.instance.assassinateEndpoint(remote.getHostAddressAndPort());
        long prevCount = ColumnFamilyStore.getIfExists(KEYSPACE, currentTable()).metric.repairFailuresDueToDownParticipants.getCount();

        session.start(null);

        assertNotNull(session.cause());
        assertEquals(prevCount + 1, ColumnFamilyStore.getIfExists(KEYSPACE, currentTable()).metric.repairFailuresDueToDownParticipants.getCount());
    }
}
