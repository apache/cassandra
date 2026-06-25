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

package org.apache.cassandra.distributed.test.tracking;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage of CASSANDRA-21406: a static journal segment must be retained while any
 * unrepaired sstable references it, and must be droppable once every referencing sstable has
 * been promoted to repaired (e.g. by compaction once mutations are durably reconciled).
 */
public class MutationJournalSegmentRefcountTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE =
    "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} " +
    "AND replication_type = 'tracked'";

    private static final String CREATE_TABLE = "CREATE TABLE %s.tbl (pk int PRIMARY KEY, val text)";

    @Test(timeout = 120_000)
    public void testSegmentRetainedUntilSSTableRepaired() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Disable autocompaction so flushed sstables stay until we explicitly compact.
            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, "tbl").asserts().success());

            // Block offset broadcasts: each node only sees its own witnesses, so isDurablyReconciled is
            // false everywhere and SSTableWriter cannot auto-mark the flushed sstables as repaired.
            cluster.filters().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).drop();

            for (int i = 0; i < 50; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                ConsistencyLevel.QUORUM, i, "v" + i);
            }

            // Flush and force the active journal segment to roll so we have a static segment to inspect.
            cluster.forEach(i -> i.nodetoolResult("flush", KEYSPACE).asserts().success());
            cluster.forEach(i -> i.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty()));

            // Confirm there is a static segment that the new refcount is keeping alive, then try to drop:
            // the dropping pass must be a no-op because every flushed sstable is unrepaired.
            cluster.forEach(i -> i.runOnInstance(() -> {
                int before = MutationJournal.instance().countStaticSegmentsForTesting();
                assertTrue("Expected at least one static segment after flush+segment close, got " + before, before > 0);
                MutationTrackingService.instance().persistLogStateForTesting();
                int after = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Static segments must not be dropped while unrepaired sstables reference them",
                             before, after);
            }));

            // Restore broadcast, exchange witnesses, and persist so isDurablyReconciled is now true everywhere.
            cluster.filters().reset();
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting()));
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));

            // Major-compact the table. Compaction rewrites the sstable through SSTableWriter.finalizeMetadata,
            // which detects that all mutations are durably reconciled and stamps repairedAt on the output.
            // SSTableListChangedNotification then releases refs from the (unrepaired) inputs without acquiring
            // any from the (repaired) output -> refcount drops to zero.
            cluster.forEach(i -> i.nodetoolResult("compact", KEYSPACE, "tbl").asserts().success());

            // Now the persister can drop the static segments.
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));
            cluster.forEach(i -> i.runOnInstance(() -> {
                int remaining = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Static segments must be dropped once their sstables are promoted to repaired",
                             0, remaining);
            }));
        }
    }
}
