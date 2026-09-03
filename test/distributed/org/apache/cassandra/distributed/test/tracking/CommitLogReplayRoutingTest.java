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

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.SSTableProvenance;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Commit log replay must route onto the untracked apply path, not consult schema
 */
public class CommitLogReplayRoutingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";

    /**
     * An untracked write, left unflushed, whose keyspace is then migrated to tracked.
     */
    @Test
    public void replayAppliesUntrackedRecordAfterMigrationToTracked() throws IOException
    {
        assertRecordsReplayAcrossAlter("untracked", "tracked");
    }

    /**
     * A tracked keyspace altered back to untracked with unflushed journal records.
     */
    @Test
    public void replayAppliesTrackedRecordAfterFlipToUntracked() throws IOException
    {
        assertRecordsReplayAcrossAlter("tracked", "untracked");
    }

    /**
     * Writes ten records under one replication type, alters the keyspace to the other, and restarts. The records are in
     * whichever log the first type routes to, and current metadata disagrees with them by the time replay runs.
     */
    private void assertRecordsReplayAcrossAlter(String before, String after) throws IOException
    {
        // NATIVE_PROTOCOL so isNativeTransportRunning() is meaningful; without it the transport never starts.
        try (Cluster cluster = init(builder().withNodes(1)
                                            .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL))
                                            .start()))
        {
            IInvokableInstance node = cluster.get(1);

            cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication_type='" + before + '\'');
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (k int PRIMARY KEY, v int)"));

            for (int k = 0; k < 10; k++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (k, v) VALUES (?, ?)"),
                                               ConsistencyLevel.ALL, k, k);
            assertNoSSTables(node);

            cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication_type='" + after + '\'');
            assertNoSSTables(node);

            restart(node);

            assertTrue("replay must not have stopped the native transport",
                       node.callOnInstance(() -> StorageService.instance.isNativeTransportRunning()));

            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT k, v FROM %s." + TABLE),
                                                             ConsistencyLevel.ALL);
            assertEquals("every record written as " + before + " should have been replayed", 10, rows.length);
        }
    }

    /**
     * A token with unflushed writes in both logs replays from both.
     */
    @Test
    public void replayAppliesBothLogsForSameToken() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            IInvokableInstance node = cluster.get(1);

            cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication_type='untracked'");
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (k int, c int, v int, PRIMARY KEY (k, c))"));

            // Untracked write for k=0 -> commit log.
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (k, c, v) VALUES (0, 0, 0)"),
                                           ConsistencyLevel.ALL);
            assertNoSSTables(node);

            // Migrating the keyspace routes subsequent writes to the journal.
            cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication_type='tracked'");

            // Tracked write for the same partition -> mutation journal.
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (k, c, v) VALUES (0, 1, 1)"),
                                           ConsistencyLevel.ALL);
            assertNoSSTables(node);

            restart(node);

            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT c, v FROM %s." + TABLE + " WHERE k = 0"),
                                                             ConsistencyLevel.ALL);
            assertEquals("both logs should have replayed for the same token", 2, rows.length);

            assertReplayLandsInSeparateSSTables(node);
        }
    }

    private static void assertReplayLandsInSeparateSSTables(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            Set<SSTableReader> sstables = cfs.getLiveSSTables();
            assertEquals("one sstable per domain: " + sstables, 2, sstables.size());

            int withSpan = 0;
            for (SSTableReader sstable : sstables)
            {
                SSTableProvenance provenance = SSTableProvenance.of(sstable);
                assertNotEquals("an sstable claims both logs: " + sstable, SSTableProvenance.BOTH, provenance);
                if (provenance == SSTableProvenance.COMMIT_LOG)
                    withSpan++;
            }
            assertEquals("exactly one sstable claims a commit log span, and the journal-derived one claims none: "
                         + sstables, 1, withSpan);
        });
    }

    private static void restart(IInvokableInstance node)
    {
        ClusterUtils.stopUnchecked(node);
        node.startup();
    }

    private static void assertNoSSTables(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            // The records must still be in the log only, or the restart replays nothing and the test proves nothing.
            assertTrue("already flushed: " + cfs.getLiveSSTables(), cfs.getLiveSSTables().isEmpty());
            assertFalse(cfs.getTracker().getView().getCurrentMemtable().isClean());
        });
    }
}
