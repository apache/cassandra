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

package org.apache.cassandra.repair.consistent;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.consistent.LocalSessionTest.InstrumentedLocalSessions;
import org.apache.cassandra.repair.consistent.admin.PendingStats;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.FAILED;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.FINALIZED;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.PREPARING;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class PendingRepairStatTest extends AbstractRepairTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    private static Range<Token> FULL_RANGE;
    private static IPartitioner partitioner;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        partitioner = DatabaseDescriptor.getPartitioner();
        assert partitioner instanceof ByteOrderedPartitioner;
        FULL_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                 DatabaseDescriptor.getPartitioner().getMinimumToken());
    }

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "coordinatorsessiontest").build();
        SchemaLoader.createKeyspace("coordinatorsessiontest", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    @Before
    public void setUp() throws Exception
    {
        cfs.enableAutoCompaction();
    }

    static LocalSession createSession()
    {
        LocalSession.Builder builder = LocalSession.builder(SharedContext.Global.instance);
        builder.withState(PREPARING);
        builder.withSessionID(nextTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(cfm.id.asUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Collections.singleton(FULL_RANGE));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        long now = FBUtilities.nowInSeconds();
        builder.withStartedAt(now);
        builder.withLastUpdate(now);

        return builder.build();
    }

    private static SSTableReader createSSTable(int startKey, int keys)
    {
        Set<SSTableReader> existing = cfs.getLiveSSTables();
        assert keys > 0;
        for (int i=0; i<keys; i++)
        {
            int key = startKey + i;
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", cfm.keyspace, cfm.name), key, key);
        }
        Util.flush(cfs);
        return Iterables.getOnlyElement(Sets.difference(cfs.getLiveSSTables(), existing));
    }

    private static void mutateRepaired(SSTableReader sstable, long repairedAt, TimeUUID pendingRepair)
    {
        try
        {
            cfs.getCompactionStrategyManager().mutateRepaired(Collections.singleton(sstable), repairedAt, pendingRepair, false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void pendingRepairStats()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        cfs.disableAutoCompaction();
        SSTableReader sstable1 = createSSTable(0, 10);
        SSTableReader sstable2 = createSSTable(10, 10);
        SSTableReader sstable3 = createSSTable(10, 20);

        LocalSession session1 = createSession();
        sessions.putSessionUnsafe(session1);
        LocalSession session2 = createSession();
        sessions.putSessionUnsafe(session2);

        PendingStats stats;
        stats = sessions.getPendingStats(cfm.id, Collections.singleton(FULL_RANGE));
        Assert.assertEquals(0, stats.total.numSSTables);

        // set all sstables to pending
        mutateRepaired(sstable1, UNREPAIRED_SSTABLE, session1.sessionID);
        mutateRepaired(sstable2, UNREPAIRED_SSTABLE, session2.sessionID);
        mutateRepaired(sstable3, UNREPAIRED_SSTABLE, session2.sessionID);

        stats = sessions.getPendingStats(cfm.id, Collections.singleton(FULL_RANGE));
        Assert.assertEquals(Sets.newHashSet(session1.sessionID, session2.sessionID), stats.total.sessions);
        Assert.assertEquals(3, stats.total.numSSTables);
        Assert.assertEquals(3, stats.pending.numSSTables);
        Assert.assertEquals(0, stats.failed.numSSTables);
        Assert.assertEquals(0, stats.finalized.numSSTables);

        // set the 2 sessions to failed and finalized
        session1.setState(FAILED);
        sessions.save(session1);
        session2.setState(FINALIZED);
        sessions.save(session2);

        stats = sessions.getPendingStats(cfm.id, Collections.singleton(FULL_RANGE));
        Assert.assertEquals(3, stats.total.numSSTables);
        Assert.assertEquals(0, stats.pending.numSSTables);
        Assert.assertEquals(1, stats.failed.numSSTables);
        Assert.assertEquals(2, stats.finalized.numSSTables);

        // remove sstables from pending sets
        mutateRepaired(sstable1, UNREPAIRED_SSTABLE, NO_PENDING_REPAIR);
        mutateRepaired(sstable2, session2.repairedAt, NO_PENDING_REPAIR);
        mutateRepaired(sstable3, session2.repairedAt, NO_PENDING_REPAIR);

        stats = sessions.getPendingStats(cfm.id, Collections.singleton(FULL_RANGE));
        Assert.assertTrue(stats.total.sessions.isEmpty());
    }
}
