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

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.*;

public class PaxosStateTest
{
    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("PaxosStateTest");
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    @Test
    public void testCommittingAfterTruncation() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open("PaxosStateTestKeyspace1").getColumnFamilyStore("Standard1");
        String key = "key" + System.nanoTime();
        ByteBuffer value = ByteBufferUtil.bytes(0);
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, FBUtilities.timestampMicros(), key);
        builder.clustering("a").add("val", value);
        PartitionUpdate update = Iterables.getOnlyElement(builder.build().getPartitionUpdates());

        // CFS should be empty initially
        assertNoDataPresent(cfs, Util.dk(key));

        // Commit the proposal & verify the data is present
        Commit beforeTruncate = newProposal(0, update);
        PaxosState.commit(beforeTruncate);
        assertDataPresent(cfs, Util.dk(key), "val", value);

        // Truncate then attempt to commit again, mutation should
        // be ignored as the proposal predates the truncation
        cfs.truncateBlocking();
        PaxosState.commit(beforeTruncate);
        assertNoDataPresent(cfs, Util.dk(key));

        // Now try again with a ballot created after the truncation
        long timestamp = SystemKeyspace.getTruncatedAt(update.metadata().cfId) + 1;
        Commit afterTruncate = newProposal(timestamp, update);
        PaxosState.commit(afterTruncate);
        assertDataPresent(cfs, Util.dk(key), "val", value);
    }

    private Commit newProposal(long ballotMillis, PartitionUpdate update)
    {
        return Commit.newProposal(UUIDGen.getTimeUUID(ballotMillis), update);
    }

    private void assertDataPresent(ColumnFamilyStore cfs, DecoratedKey key, String name, ByteBuffer value)
    {
        Row row = Util.getOnlyRowUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(0, ByteBufferUtil.compareUnsigned(value,
                row.getCell(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes(name))).value()));
    }

    private void assertNoDataPresent(ColumnFamilyStore cfs, DecoratedKey key)
    {
        Util.assertEmpty(Util.cmd(cfs, key).build());
    }
}
