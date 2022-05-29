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
package org.apache.cassandra.cql3.validation.operations;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.assertj.core.api.Condition;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.lang.String.format;
import static org.apache.cassandra.db.ColumnFamilyStore.SNAPSHOT_DROP_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class AutoSnapshotTest extends CQLTester
{
    static int TTL_SECS = 1;

    public static Boolean enabledBefore;
    public static DurationSpec.IntSecondsBound ttlBefore;

    @BeforeClass
    public static void beforeClass()
    {
        enabledBefore = DatabaseDescriptor.isAutoSnapshot();
        ttlBefore = DatabaseDescriptor.getAutoSnapshotTtl();
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setAutoSnapshot(enabledBefore);
        DatabaseDescriptor.setAutoSnapshotTtl(ttlBefore);
    }

    // Dynamic parameters used during tests
    @Parameterized.Parameter(0)
    public Boolean autoSnapshotEnabled;

    @Parameterized.Parameter(1)
    public DurationSpec.IntSecondsBound autoSnapshotTTl;

    @Before
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        // Make sure we're testing the correct parameterized settings
        DatabaseDescriptor.setAutoSnapshot(autoSnapshotEnabled);
        DatabaseDescriptor.setAutoSnapshotTtl(autoSnapshotTTl);
    }

    // Test for all values of [auto_snapshot=[true,false], ttl=[1s, null]
    @Parameterized.Parameters( name = "enabled={0},ttl={1}" )
    public static Collection options() {
        return Arrays.asList(new Object[][] {
        { true, new DurationSpec.IntSecondsBound(TTL_SECS, SECONDS) },
        { false, new DurationSpec.IntSecondsBound(TTL_SECS, SECONDS) },
        { true, null },
        { false, null },
        });
    }

    @Test
    public void testAutoSnapshotOnTrucate() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        // Check there are no snapshots
        ColumnFamilyStore tableDir = getCurrentColumnFamilyStore();
        assertThat(tableDir.listSnapshots()).isEmpty();

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);

        flush();

        execute("DROP TABLE %s");

        verifyAutoSnapshot(SNAPSHOT_DROP_PREFIX, tableDir, currentTable());
    }

    @Test
    public void testAutoSnapshotOnDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        // Check there are no snapshots
        ColumnFamilyStore tableDir = getCurrentColumnFamilyStore();
        assertThat(tableDir.listSnapshots()).isEmpty();

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);

        flush();

        execute("DROP TABLE %s");

        verifyAutoSnapshot(SNAPSHOT_DROP_PREFIX, tableDir, currentTable());
    }

    @Test
    public void testAutoSnapshotOnDropKeyspace() throws Throwable
    {
        // Create tables A and B and flush
        ColumnFamilyStore tableA = createAndPopulateTable();
        ColumnFamilyStore tableB = createAndPopulateTable();
        flush();

        // Check no snapshots
        assertThat(tableA.listSnapshots()).isEmpty();
        assertThat(tableB.listSnapshots()).isEmpty();

        // Drop keyspace, should have snapshot for table A and B
        execute(format("DROP KEYSPACE %s", keyspace()));
        verifyAutoSnapshot(SNAPSHOT_DROP_PREFIX, tableA, tableA.name);
        verifyAutoSnapshot(SNAPSHOT_DROP_PREFIX, tableB, tableB.name);
    }

    private ColumnFamilyStore createAndPopulateTable() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        // Check there are no snapshots
        ColumnFamilyStore tableA = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        return tableA;
    }

    /**
     * Verify that:
     * - A snapshot is created when auto_snapshot = true.
     * - TTL is added to the snapshot when auto_snapshot_ttl != null
     */
    private void verifyAutoSnapshot(String snapshotPrefix, ColumnFamilyStore tableDir, String expectedTableName)
    {
        Map<String, TableSnapshot> snapshots = tableDir.listSnapshots();
        if (autoSnapshotEnabled)
        {
            assertThat(snapshots).hasSize(1);
            assertThat(snapshots).hasKeySatisfying(new Condition<>(k -> k.startsWith(snapshotPrefix), "is dropped snapshot"));
            TableSnapshot snapshot = snapshots.values().iterator().next();
            assertThat(snapshot.getTableName()).isEqualTo(expectedTableName);
            if (autoSnapshotTTl == null)
            {
                // check that the snapshot has NO TTL
                assertThat(snapshot.isExpiring()).isFalse();
            }
            else
            {
                // check that snapshot has TTL and is expired after 1 second
                assertThat(snapshot.isExpiring()).isTrue();
                Uninterruptibles.sleepUninterruptibly(TTL_SECS, SECONDS);
                assertThat(snapshot.isExpired(Instant.now())).isTrue();
            }
        }
        else
        {
            // No snapshot should be created when auto_snapshot = false
            assertThat(snapshots).isEmpty();
        }
    }
}
