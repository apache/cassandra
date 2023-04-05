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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.*;

public class CASMultiDCTest
{
    private static Cluster CLUSTER;
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String KS_TBL = KEYSPACE + '.' + TABLE;

    private static final AtomicInteger nextKey = new AtomicInteger();

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        Consumer<IInstanceConfig> conf = config -> config
                                                   .with(Feature.NETWORK)
                                                   .set("paxos_variant", "v2")
                                                   .set("write_request_timeout_in_ms", 1000L)
                                                   .set("cas_contention_timeout_in_ms", 1000L)
                                                   .set("request_timeout_in_ms", 1000L);

        Cluster.Builder builder = new Cluster.Builder();
        builder.withNodes(4);
        builder.withDCs(2);
        builder.withConfig(conf);
        CLUSTER = builder.start();
        CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':2};");
        CLUSTER.schemaChange("CREATE TABLE " + KS_TBL + " (k int, c int, v int, primary key (k, v))");
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            Assert.assertTrue(PaxosCommit.getEnableDcLocalCommit()); // should be enabled by default
        }));
    }

    @Before
    public void setUp()
    {
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            PaxosCommit.setEnableDcLocalCommit(true);
        }));
    }

    private static void testLocalSerialCommit(ConsistencyLevel serialCL, ConsistencyLevel commitCL, int key, boolean expectRemoteCommit)
    {
        for (int i=0; i<CLUSTER.size(); i++)
        {
            CLUSTER.get(i + 1).runOnInstance(() -> {
                UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM system.paxos WHERE row_key=?", ByteBufferUtil.bytes(key));
                Assert.assertEquals(0, result.size());
            });
        }

        CLUSTER.coordinator(1).execute("INSERT INTO " + KS_TBL + " (k, c, v) VALUES (?, ?, ?) IF NOT EXISTS", serialCL, commitCL, key, key, key);

        int numCommitted = 0;
        int numWritten = 0;
        for (int i=0; i<CLUSTER.size(); i++)
        {
            boolean expectPaxosRows = expectRemoteCommit || i < 2;
            int flags = CLUSTER.get(i + 1).callOnInstance(() -> {
                int numPaxosRows = QueryProcessor.executeInternal("SELECT * FROM system.paxos WHERE row_key=?", ByteBufferUtil.bytes(key)).size();
                Assert.assertTrue(numPaxosRows == 0 || numPaxosRows == 1);
                if (!expectRemoteCommit)
                    Assert.assertEquals(expectPaxosRows ? 1 : 0, numPaxosRows);
                int numTableRows = QueryProcessor.executeInternal("SELECT * FROM " + KS_TBL + " WHERE k=?", ByteBufferUtil.bytes(key)).size();
                Assert.assertTrue(numTableRows == 0 || numTableRows == 1);
                return (numPaxosRows > 0 ? 1 : 0) | (numTableRows > 0 ? 2 : 0);
            });
            if ((flags & 1) != 0)
                numCommitted++;
            if ((flags & 2) != 0)
                numWritten++;
        }
        Assert.assertTrue(String.format("numWritten: %s < 3", numWritten), numWritten >= 3);
        if (expectRemoteCommit)
            Assert.assertTrue(String.format("numCommitted: %s < 3", numCommitted), numCommitted >= 3);
        else
            Assert.assertEquals(2, numCommitted);
    }

    @Test
    public void testLocalSerialLocalCommit()
    {
        testLocalSerialCommit(LOCAL_SERIAL, LOCAL_QUORUM, nextKey.getAndIncrement(), false);
    }

    @Test
    public void testLocalSerialQuorumCommit()
    {
        testLocalSerialCommit(LOCAL_SERIAL, QUORUM, nextKey.getAndIncrement(), false);
    }

    @Test
    public void testSerialLocalCommit()
    {
        testLocalSerialCommit(SERIAL, LOCAL_QUORUM, nextKey.getAndIncrement(), true);
    }

    @Test
    public void testSerialQuorumCommit()
    {
        testLocalSerialCommit(SERIAL, QUORUM, nextKey.getAndIncrement(), true);
    }

    @Test
    public void testDcLocalCommitDisabled()
    {
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            PaxosCommit.setEnableDcLocalCommit(false);
        }));
        testLocalSerialCommit(LOCAL_SERIAL, QUORUM, nextKey.getAndIncrement(), true);
    }
}
