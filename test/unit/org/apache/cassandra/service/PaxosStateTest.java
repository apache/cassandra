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

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class PaxosStateTest
{
    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    @Test
    public void testCommittingAfterTruncation() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Standard1");
        DecoratedKey key = Util.dk("key" + System.nanoTime());
        ByteBuffer name = ByteBufferUtil.bytes("col");
        ByteBuffer value = ByteBufferUtil.bytes(0);
        ColumnFamily update = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        update.addColumn(name, value, FBUtilities.timestampMicros());

        // CFS should be empty initially
        assertNoDataPresent(cfs, key);

        // Commit the proposal & verify the data is present
        Commit beforeTruncate = newProposal(0, key.key, update);
        PaxosState.commit(beforeTruncate);
        assertDataPresent(cfs, key, name, value);

        // Truncate then attempt to commit again, mutation should
        // be ignored as the proposal predates the truncation
        cfs.truncateBlocking();
        PaxosState.commit(beforeTruncate);
        assertNoDataPresent(cfs, key);

        // Now try again with a ballot created after the truncation
        long timestamp = SystemKeyspace.getTruncatedAt(update.metadata().cfId) + 1;
        Commit afterTruncate = newProposal(timestamp, key.key, update);
        PaxosState.commit(afterTruncate);
        assertDataPresent(cfs, key, name, value);
    }

    private Commit newProposal(long ballotMillis, ByteBuffer key, ColumnFamily update)
    {
        return Commit.newProposal(key, UUIDGen.getTimeUUID(ballotMillis), update);
    }

    private void assertDataPresent(ColumnFamilyStore cfs, DecoratedKey key, ByteBuffer name, ByteBuffer value)
    {
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfs.name, System.currentTimeMillis()));
        assertFalse(cf.isEmpty());
        assertEquals(0, ByteBufferUtil.compareUnsigned(value, cf.getColumn(name).value()));
    }

    private void assertNoDataPresent(ColumnFamilyStore cfs, DecoratedKey key)
    {
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfs.name, System.currentTimeMillis()));
        assertNull(cf);
    }
}
