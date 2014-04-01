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
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class BatchlogManagerTest extends SchemaLoader
{
    @Before
    public void setUp() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        InetAddress localhost = InetAddress.getByName("127.0.0.1");
        metadata.updateNormalToken(Util.token("A"), localhost);
        metadata.updateHostId(UUIDGen.getTimeUUID(), localhost);
    }

    @Test
    public void testReplay() throws Exception
    {
        assertEquals(0, BatchlogManager.instance.countAllBatches());
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed());

        // Generate 1000 mutations and put them all into the batchlog.
        // Half (500) ready to be replayed, half not.
        for (int i = 0; i < 1000; i++)
        {
            RowMutation mutation = new RowMutation("Keyspace1", bytes(i));
            mutation.add("Standard1", bytes(i), bytes(i), System.currentTimeMillis());

            long timestamp = System.currentTimeMillis();
            if (i < 500)
                timestamp -= DatabaseDescriptor.getWriteRpcTimeout() * 2;
            BatchlogManager.getBatchlogMutationFor(Collections.singleton(mutation), UUIDGen.getTimeUUID(), timestamp * 1000).apply();
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.BATCHLOG_CF).forceBlockingFlush();

        assertEquals(1000, BatchlogManager.instance.countAllBatches());
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed());

        // Force batchlog replay.
        BatchlogManager.instance.replayAllFailedBatches();

        // Ensure that the first half, and only the first half, got replayed.
        assertEquals(500, BatchlogManager.instance.countAllBatches());
        assertEquals(500, BatchlogManager.instance.getTotalBatchesReplayed());

        for (int i = 0; i < 1000; i++)
        {
            UntypedResultSet result = QueryProcessor.processInternal(String.format("SELECT * FROM \"Keyspace1\".\"Standard1\" WHERE key = intAsBlob(%d)", i));
            if (i < 500)
            {
                assertEquals(bytes(i), result.one().getBytes("key"));
                assertEquals(bytes(i), result.one().getBytes("column1"));
                assertEquals(bytes(i), result.one().getBytes("value"));
            }
            else
            {
                assertTrue(result.isEmpty());
            }
        }

        // Ensure that no stray mutations got somehow applied.
        UntypedResultSet result = QueryProcessor.processInternal(String.format("SELECT count(*) FROM \"Keyspace1\".\"Standard1\""));
        assertEquals(500, result.one().getLong("count"));
    }
}
