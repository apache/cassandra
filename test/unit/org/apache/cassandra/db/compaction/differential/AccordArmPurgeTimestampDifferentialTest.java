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
 * Checks the {@code CursorCompactor.purgeTimestamp} branch that returns
 * {@code controller.gcBefore} for accord-enabled and accord-migrating tables. The iterator's
 * equivalent is {@code CompactionIterator.purger()} (CompactionIterator.java:281-285).
 *
 * <p>{@link AccordTableDifferentialCompactionTest} runs only the accord-enabled arm and has no
 * non-accord control. A regression that dropped the override, so the accord arm behaved like the
 * non-accord arm, would still pass it. This test runs the same data and the same "now" through all
 * three arms, so the override shows up as a difference between arms, and it asserts cursor ==
 * iterator inside each arm.
 *
 * <p>What the override gates: {@code purgeTimestamp} sets the {@code nowInSec} used for TTL expiry
 * and liveness (Purger.shouldPurge and the isLive/isExpired checks). A plain row, cell, or range
 * tombstone is purged when {@code localDeletionTime < controller.gcBefore}, whatever {@code nowInSec}
 * is, so tombstone purge does not differ between the arms. Only the rewrite of expiring cells into
 * tombstones does. The visible signal is whether an expired TTL cell keeps its value (deferred) or
 * is rewritten as a cell tombstone (expired at nowInSec).
 *
 * <p>transactional_mode = 'test_unsafe' sets accordIsEnabled without routing CQL through accord.
 * transactional_mode = 'off' with transactional_migration_from = 'full' leaves accordIsEnabled
 * false but makes migratingFromAccord() true. AccordService is started because the accord-enabled
 * arm's gcBefore derivation (CompactionTask.getCompactionController) reads the node's durability
 * state; the non-accord and migrating arms do not enter that derivation.
 */
public class AccordArmPurgeTimestampDifferentialTest extends DifferentialCompactionTester
{
    private static final long TTL_SECONDS = 1;

    @BeforeClass
    public static void startAccord()
    {
        DatabaseDescriptor.setAccordTransactionsEnabled(true);
        AccordService.localStartup(ClusterMetadata.current().myNodeId());
        AccordService.distributedStartup();
    }

    /**
     * Non-accord control. purgeTimestamp returns the wall-clock nowInSec, so cells past their TTL
     * relative to the pinned now are expired: their values are dropped and each is rewritten as a
     * cell tombstone. This is the behaviour the accord/migrating arms must NOT show. The cursor and
     * iterator paths must agree.
     */
    @Test
    public void nonAccordArmAppliesExpiry() throws Exception
    {
        ColumnFamilyStore cfs = createExpiringTable("");
        long fixedNow = writeExpiringRowsAndPin(cfs);

        // default gcBefore is ~gc_grace in the past, far below the cells' localExpirationTime
        // (~now), so the expired cells are converted to tombstones rather than fully purged.
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                         taskWithFixedNow(fixedNow));

        String json = allJson(out);
        assertTrue("non-accord: expired TTL cells must be converted to cell tombstones (purge uses " +
                   "wall-clock nowInSec); none were, so the scenario did not exercise expiry",
                   json.contains(CELL_TOMBSTONE));
        for (long ck = 0; ck < 10; ck++)
            assertFalse("non-accord: an expired TTL cell kept its value at ck " + ck +
                        "; the wall-clock nowInSec should have dropped it", json.contains(cellValue("ttl-" + ck)));
    }

    /**
     * Accord-enabled arm. purgeTimestamp returns controller.gcBefore (NO_GC here, mirroring
     * CompactionTask.getCompactionController for an accord table with no transaction history), so
     * the same cells are NOT expired: their values survive with their TTL intact. Cursor and
     * iterator must agree.
     */
    @Test
    public void accordEnabledArmDefersExpiry() throws Exception
    {
        ColumnFamilyStore cfs = createExpiringTable(" AND transactional_mode = 'test_unsafe'");
        long fixedNow = writeExpiringRowsAndPin(cfs);

        // accord tables must be compacted with gcBefore <= 0; getCompactionController asserts it.
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                         taskWithFixedNow(fixedNow), CompactionManager.NO_GC);

        assertExpiryDeferred(out);
    }

    /**
     * Accord-migrating arm. transactional_mode is off, so accordIsEnabled() is false and the table
     * does not enter the accord gcBefore derivation, but migratingFromAccord() is true, so
     * purgeTimestamp still overrides nowInSec to controller.gcBefore. With a gcBefore in the past
     * (below the cells' expiration) the expiry is deferred exactly as in the enabled arm. Cursor
     * and iterator must agree.
     */
    @Test
    public void migratingFromAccordArmDefersExpiry() throws Exception
    {
        // Migration cannot be requested at CREATE ("Cannot set transactional migration on new
        // tables"). Enabling accord and then turning it off leaves the table migrating from full.
        ColumnFamilyStore cfs = createExpiringTable("");
        alterTable("ALTER TABLE %s WITH transactional_mode = 'full'");
        alterTable("ALTER TABLE %s WITH transactional_mode = 'off'");
        assertTrue("fixture did not produce a migrating-from-accord table",
                   cfs.metadata().migratingFromAccord());
        assertFalse("migrating fixture must not be accord-enabled, or it would take the enabled arm's " +
                    "gcBefore derivation path", cfs.metadata().isAccordEnabled());
        long fixedNow = writeExpiringRowsAndPin(cfs);

        // gcBefore in the past becomes the override nowInSec; the cells (expiring ~now) are not yet
        // expired relative to it, so they are deferred. NOT NO_GC: this arm skips the accord
        // derivation and a NO_GC here would leave nowInSec at 0 and defer trivially for both paths.
        long pastGcBefore = FBUtilities.nowInSeconds() - 3600;
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                         taskWithFixedNow(fixedNow), pastGcBefore);

        assertExpiryDeferred(out);
    }

    private ColumnFamilyStore createExpiringTable(String extraOptions)
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000" + extraOptions);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /**
     * Writes two overlapping sstables of TTL cells, then returns a "now" pinned two seconds past
     * the last write so every TTL cell has lapsed relative to it. Asserts the fixture actually put
     * an expired cell on disk, so a change that stopped writing TTLs cannot make the arms pass
     * vacuously.
     */
    private long writeExpiringRowsAndPin(ColumnFamilyStore cfs)
    {
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?) USING TTL " + TTL_SECONDS,
                    ck, ck, "ttl-" + ck);
        flush();
        for (long ck = 5; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?) USING TTL " + TTL_SECONDS,
                    ck, ck, "ttl-" + ck);
        flush();

        long fixedNow = FBUtilities.nowInSeconds() + 2;
        assertSomethingExpiredAt(cfs, fixedNow);
        return fixedNow;
    }

    /** The accord/migrating shared expectation: every TTL value survives and nothing became a tombstone. */
    private void assertExpiryDeferred(CapturedOutput out)
    {
        String json = allJson(out);
        for (long ck = 0; ck < 10; ck++)
            assertTrue("accord arm: an expiring cell was converted to a tombstone despite the gcBefore " +
                       "deferral, at ck " + ck, json.contains(cellValue("ttl-" + ck)));
        assertFalse("accord arm: no cell should have been converted to a tombstone under the gcBefore " +
                    "deferral, which is what the override exists to prevent", json.contains(CELL_TOMBSTONE));
    }
}
