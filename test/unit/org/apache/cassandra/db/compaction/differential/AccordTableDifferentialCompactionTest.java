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

package org.apache.cassandra.db.compaction.differential;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * An accord-enabled user table purges and expires relative to gcBefore, not to wall-clock now.
 * CompactionIterator.purger() sets nowInSec to controller.gcBefore when
 * metadata.isAccordEnabled() or metadata.migratingFromAccord() holds. That defers TTL-expiry
 * conversion and liveness purging, so accord can still read the data at earlier timestamps and
 * during migration. CompactionTask.getCompactionController derives gcBefore for an accord table
 * from the node's durability bounds. With no transaction history that derivation yields NO_GC,
 * which expires and purges nothing. The cursor path must apply the same nowInSec override.
 *
 * transactional_mode = 'test_unsafe' sets accordIsEnabled without routing plain CQL reads and
 * writes through accord. The test starts a real local AccordService, because the gcBefore
 * derivation reads the node's durableBefore and redundantBefore state.
 */
public class AccordTableDifferentialCompactionTest extends DifferentialCompactionTester
{
    @BeforeClass
    public static void startAccord()
    {
        DatabaseDescriptor.setAccordTransactionsEnabled(true);
        AccordService.localStartup(ClusterMetadata.current().myNodeId());
        AccordService.distributedStartup();
    }

    @Test
    public void expiredCellsDeferToGcBefore() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000 AND transactional_mode = 'test_unsafe'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // An INSERT with a TTL puts it on the row liveness and on every cell. The pk 1 rows
        // stay plain.
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?) USING TTL 1", ck, ck, "ttl-" + ck);
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, ck);
        flush();

        // A second, overlapping sstable. An UPDATE adds a cell-level TTL to rows that had none.
        for (long ck = 5; ck < 10; ck++)
            execute("UPDATE %s USING TTL 1 SET v2 = ? WHERE pk = 1 AND ck = ?", "cell-ttl-" + ck, ck);
        for (long ck = 10; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, ck + 100);
        flush();

        // Pin "now" two seconds past the last write, so every TTL=1 cell above has expired
        // relative to it.
        long fixedNow = FBUtilities.nowInSeconds() + 2;
        assertSomethingExpiredAt(cfs, fixedNow);

        // NO_GC mirrors what the compaction scheduler passes for accord tables
        // (CompactionTask.getCompactionController asserts gcBefore <= 0 before deriving)
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                         taskWithFixedNow(fixedNow), CompactionManager.NO_GC);

        // These assertions are ABSOLUTE, and they double as the scenario's non-vacuity guard.
        // Every TTL above has lapsed relative to the nowInSec pinned above. A path that used
        // nowInSec would therefore convert these cells to tombstones and drop their values.
        // Under the accord gcBefore they must survive with their TTL intact. A CQL read cannot
        // show that: CQL applies expiry at read time, so a retained but expired cell is
        // invisible either way.
        String json = allJson(out);
        for (long ck = 0; ck < 10; ck++)
            assertTrue("an expiring cell was converted to a tombstone despite the accord gcBefore " +
                       "deferral, at pk 0 ck " + ck, json.contains(cellValue("ttl-" + ck)));
        for (long ck = 5; ck < 10; ck++)
            assertTrue("a cell-level TTL was converted to a tombstone despite the accord gcBefore " +
                       "deferral, at pk 1 ck " + ck, json.contains(cellValue("cell-ttl-" + ck)));
        assertFalse("no cell should have been converted to a tombstone under the accord gcBefore, " +
                    "which is what the deferral exists to prevent",
                    json.contains(CELL_TOMBSTONE));
        // A survivor is still an EXPIRING cell, not a plain one, so the TTL travelled with it.
        // JsonTransformer suppresses a cell's ttl when it equals the row liveness's ttl. The pk 0
        // INSERTs therefore render it on liveness_info, and the pk 1 UPDATEs, which write no row
        // liveness, render it on the cell. Either form satisfies this assertion. The trailing
        // comma keeps it from also matching "ttl":10.
        assertTrue("the retained cells lost their TTL", json.contains("\"ttl\":1,"));
    }
}
