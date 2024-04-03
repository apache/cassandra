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
package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;
import java.util.function.Function;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.TableMetadata;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.paxos.PaxosState.Snapshot;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.Config.PaxosStatePurging.gc_grace;
import static org.apache.cassandra.config.Config.PaxosStatePurging.legacy;
import static org.apache.cassandra.config.Config.PaxosStatePurging.repaired;
import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.atUnixMicros;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.junit.Assert.*;

public class PaxosStateTest
{
    static TableMetadata metadata;
    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("PaxosStateTest");
        metadata = Keyspace.open("PaxosStateTestKeyspace1").getColumnFamilyStore("Standard1").metadata.get();
        metadata.withSwapped(metadata.params.unbuild().gcGraceSeconds(3600).build());
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
        RowUpdateBuilder builder = new RowUpdateBuilder(metadata, FBUtilities.timestampMicros(), key);
        builder.clustering("a").add("val", value);
        PartitionUpdate update = Iterables.getOnlyElement(builder.build().getPartitionUpdates());

        // CFS should be empty initially
        assertNoDataPresent(cfs, Util.dk(key));

        // Commit the proposal & verify the data is present
        Commit beforeTruncate = newProposal(0, update);
        PaxosState.commitDirect(beforeTruncate);
        assertDataPresent(cfs, Util.dk(key), "val", value);

        // Truncate then attempt to commit again, mutation should
        // be ignored as the proposal predates the truncation
        cfs.truncateBlocking();
        PaxosState.commitDirect(beforeTruncate);
        assertNoDataPresent(cfs, Util.dk(key));

        // Now try again with a ballot created after the truncation
        long timestamp = SystemKeyspace.getTruncatedAt(update.metadata().id) + 1;
        Commit afterTruncate = newProposal(timestamp, update);
        PaxosState.commitDirect(afterTruncate);
        assertDataPresent(cfs, Util.dk(key), "val", value);
    }

    @Test
    public void testReadExpired()
    {
        DatabaseDescriptor.setPaxosStatePurging(gc_grace);

        String key = "key" + System.nanoTime();
        Accepted accepted = newProposal(1, key).accepted();
        PaxosState.legacyPropose(accepted);

        DatabaseDescriptor.setPaxosStatePurging(repaired);
        // not expired if read in the past
        assertPaxosState(key, accepted, state -> state.current(accepted.ballot).accepted);
        // not expired if read with paxos state purging enabled
        assertPaxosState(key, accepted, state -> state.current(Long.MAX_VALUE).accepted);
        DatabaseDescriptor.setPaxosStatePurging(gc_grace);
        // not expired if read in the past
        assertPaxosState(key, accepted, state -> state.current(accepted.ballot).accepted);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);
        // clear cache to read from disk
        PaxosState.RECENT.clear();

        Committed committed = accepted.committed();
        Committed empty = emptyProposal(key).accepted().committed();
        PaxosState.commitDirect(committed);
        DatabaseDescriptor.setPaxosStatePurging(repaired);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(committed.ballot).committed);
        // not expired if read with paxos state purging enabled
        assertPaxosState(key, committed, state -> state.current(Long.MAX_VALUE).committed);
        DatabaseDescriptor.setPaxosStatePurging(gc_grace);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(committed.ballot).committed);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
        DatabaseDescriptor.setPaxosStatePurging(repaired);
    }

    @Test
    public void testReadTTLd()
    {
        String key = "key" + System.nanoTime();
        String key2 = key + 'A';
        Accepted accepted = new AcceptedWithTTL(newProposal(1, key), 1);
        PaxosState.legacyPropose(accepted);
        PaxosState.legacyPropose(new AcceptedWithTTL(newProposal(1, key2), 10000));

        DatabaseDescriptor.setPaxosStatePurging(repaired);
        // not expired if read in the past
        assertPaxosState(key, accepted, state -> state.current(0).accepted);
        // TTL, so still expired if read with paxos state purging enabled
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);
        DatabaseDescriptor.setPaxosStatePurging(gc_grace);
        // not expired if read in the past
        assertPaxosState(key, accepted, state -> state.current(0).accepted);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);
        DatabaseDescriptor.setPaxosStatePurging(legacy);
        // not expired if read in the past
        assertPaxosState(key, accepted, state -> state.current(0).accepted);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);
        // clear cache to read from disk
        PaxosState.RECENT.clear();

        Committed committed = new CommittedWithTTL(accepted, accepted.update.metadata().params.gcGraceSeconds + 1);
        Committed empty = emptyProposal(key).accepted().committed();
        PaxosState.commitDirect(committed);

        DatabaseDescriptor.setPaxosStatePurging(repaired);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(0).committed);
        // not expired if read with paxos state purging enabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
        DatabaseDescriptor.setPaxosStatePurging(gc_grace);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(0).committed);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
        DatabaseDescriptor.setPaxosStatePurging(legacy);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(0).committed);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
    }

    @Test
    public void testWriteTTLd()
    {
        String key = "key" + System.nanoTime();
        Accepted accepted = newProposal(1, key).accepted();

        DatabaseDescriptor.setPaxosStatePurging(legacy); // write with TTLs
        PaxosState.legacyPropose(accepted);
        accepted = new AcceptedWithTTL(accepted, -1); // for equality test

        DatabaseDescriptor.setPaxosStatePurging(repaired);
        // not expired if read in the past (or now)
        assertPaxosState(key, accepted, state -> state.current(0).accepted);
        // TTL, so still expired if read with paxos state purging enabled
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);

        DatabaseDescriptor.setPaxosStatePurging(gc_grace);
        // not expired if read in the past (or now)
        assertPaxosState(key, accepted, state -> state.current(0).accepted);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);

        DatabaseDescriptor.setPaxosStatePurging(legacy);
        // not expired if read in the past (or now)
        assertPaxosState(key, accepted, state -> state.current(0).accepted);
        // TTL, so expired in the future
        assertPaxosState(key, null, state -> state.current(Long.MAX_VALUE).accepted);

        PaxosState.RECENT.clear();

        Committed committed = new Committed(accepted);
        Committed empty = emptyProposal(key).accepted().committed();
        DatabaseDescriptor.setPaxosStatePurging(legacy); // write with TTLs
        committed = new CommittedWithTTL(committed, committed.update.metadata().params.gcGraceSeconds + 1); // for equality test
        PaxosState.commitDirect(committed);

        DatabaseDescriptor.setPaxosStatePurging(repaired);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(0).committed);
        // not expired if read with paxos state purging enabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
        DatabaseDescriptor.setPaxosStatePurging(gc_grace);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(0).committed);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
        DatabaseDescriptor.setPaxosStatePurging(legacy);
        // not expired if read in the past
        assertPaxosState(key, committed, state -> state.current(0).committed);
        // expired if read with paxos state purging disabled
        assertPaxosState(key, empty, state -> state.current(Long.MAX_VALUE).committed);
    }

    private static void assertPaxosState(String key, Commit expect, Function<PaxosState, Object> test)
    {
        // TODO: test from cache after write
        PaxosState.RECENT.clear();
        // test from disk
        try (PaxosState state = PaxosState.get(Util.dk(key), metadata))
        {
            Assert.assertEquals(expect, test.apply(state));
        }
        // test from cache
        try (PaxosState state = PaxosState.get(Util.dk(key), metadata))
        {
            Assert.assertEquals(expect, test.apply(state));
        }
    }

    private Commit newProposal(long ballotMillis, PartitionUpdate update)
    {
        return Commit.newProposal(atUnixMicros(ballotMillis, NONE), update);
    }

    private Proposal emptyProposal(String k)
    {
        return Proposal.empty(Ballot.none(), Util.dk(k), Keyspace.open("PaxosStateTestKeyspace1").getColumnFamilyStore("Standard1").metadata.get());
    }

    private Proposal newProposal(long ballotMicros, String k)
    {
        return Proposal.empty(atUnixMicros(ballotMicros, NONE), Util.dk(k), Keyspace.open("PaxosStateTestKeyspace1").getColumnFamilyStore("Standard1").metadata.get());
    }

    private void assertDataPresent(ColumnFamilyStore cfs, DecoratedKey key, String name, ByteBuffer value)
    {
        Row row = Util.getOnlyRowUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(0, ByteBufferUtil.compareUnsigned(value,
                row.getCell(cfs.metadata.get().getColumn(ByteBufferUtil.bytes(name))).buffer()));
    }

    private void assertNoDataPresent(ColumnFamilyStore cfs, DecoratedKey key)
    {
        Util.assertEmpty(Util.cmd(cfs, key).build());
    }

    private static void assertAcceptedMerge(Accepted expected, Accepted left, Accepted right)
    {
        Committed empty = Committed.none(expected.update.partitionKey(), expected.update.metadata());
        Snapshot snapshotLeft = new Snapshot(null, null, left, empty);
        Snapshot snapshotRight = new Snapshot(null, null, right, empty);
        Accepted merged = Snapshot.merge(snapshotLeft, snapshotRight).accepted;
        Assert.assertSame(expected, merged);
    }

    @Test
    public void testAcceptMerging()
    {
        Accepted accepted = newProposal(1, "1").accepted();
        AcceptedWithTTL acceptedWithTTL1 = new AcceptedWithTTL(accepted, 100);
        AcceptedWithTTL acceptedWithTTL2 = new AcceptedWithTTL(accepted, 200);

        assertAcceptedMerge(accepted, accepted, acceptedWithTTL1);
        assertAcceptedMerge(accepted, acceptedWithTTL1, accepted);
        assertAcceptedMerge(acceptedWithTTL2, acceptedWithTTL1, acceptedWithTTL2);
        assertAcceptedMerge(acceptedWithTTL2, acceptedWithTTL2, acceptedWithTTL1);
    }

    private static void assertCommittedMerge(Committed expected, Committed left, Committed right)
    {
        Committed merged = Snapshot.merge(new Snapshot(null, null, null, left), new Snapshot(null, null, null, right)).committed;
        Assert.assertSame(expected, merged);
    }

    @Test
    public void testCommitMerging()
    {
        Committed committed = newProposal(1, "1").accepted().committed();
        Committed committedWithTTL1 = new CommittedWithTTL(committed, 100);
        Committed committedWithTTL2 = new CommittedWithTTL(committed, 200);

        assertCommittedMerge(committed, committed, committedWithTTL1);
        assertCommittedMerge(committed, committedWithTTL1, committed);
        assertCommittedMerge(committedWithTTL2, committedWithTTL1, committedWithTTL2);
        assertCommittedMerge(committedWithTTL2, committedWithTTL2, committedWithTTL1);
    }
}
