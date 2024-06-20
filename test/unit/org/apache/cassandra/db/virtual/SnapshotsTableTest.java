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

package org.apache.cassandra.db.virtual;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.service.snapshot.SnapshotOptions;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.TableSnapshot;

public class SnapshotsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String SNAPSHOT_TTL = "snapshotTtl";
    private static final String SNAPSHOT_NO_TTL = "snapshotNoTtl";
    private static final String TTL = "4h";

    @Before
    public void before() throws Throwable
    {
        SnapshotsTable vtable = new SnapshotsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(vtable)));

        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");

        for (int i = 0; i != 10; ++i)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", i, i);

        flush();
    }

    @After
    public void after()
    {
        SnapshotManager.instance.clearSnapshot(SNAPSHOT_NO_TTL, Collections.emptyMap(), KEYSPACE);
        SnapshotManager.instance.clearSnapshot(SNAPSHOT_TTL, Collections.emptyMap(), KEYSPACE);
        schemaChange(String.format("DROP TABLE %s", KEYSPACE + '.' + currentTable()));
    }

    private static Date toDate(Instant instant)
    {
        return new Date(instant.toEpochMilli());
    }

    private static TableSnapshot createSnapshot(String snapshotTtl, Map<String, String> options, ColumnFamilyStore cfs)
    {
        List<TableSnapshot> snapshots = SnapshotManager.instance.takeSnapshot(SnapshotOptions.userSnapshot(snapshotTtl, options, cfs.getKeyspaceTableName()));
        Assert.assertEquals(1, snapshots.size());
        return snapshots.iterator().next();
    }

    @Test
    public void testSnapshots()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE);
        TableSnapshot snapshotWithTtl = createSnapshot(SNAPSHOT_TTL, Map.of(SnapshotOptions.TTL, TTL), cfs);
        TableSnapshot snapshotWithoutTtl = createSnapshot(SNAPSHOT_NO_TTL, Collections.emptyMap(), cfs);

        // query all from snapshots virtual table
        UntypedResultSet result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots");
        assertRowsIgnoringOrder(result,
                                row(SNAPSHOT_NO_TTL, KEYSPACE, currentTable(), toDate(snapshotWithoutTtl.getCreatedAt()), null, false),
                                row(SNAPSHOT_TTL, KEYSPACE, currentTable(), toDate(snapshotWithTtl.getCreatedAt()), toDate(snapshotWithTtl.getExpiresAt()), false));

        // query with conditions
        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where ephemeral = false");
        assertRows(result,
                   row(SNAPSHOT_NO_TTL, KEYSPACE, currentTable(), toDate(snapshotWithoutTtl.getCreatedAt()), null, false),
                   row(SNAPSHOT_TTL, KEYSPACE, currentTable(), toDate(snapshotWithTtl.getCreatedAt()), toDate(snapshotWithTtl.getExpiresAt()), false));

        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where size_on_disk > 1000");
        assertRows(result,
                   row(SNAPSHOT_NO_TTL, KEYSPACE, currentTable(), toDate(snapshotWithoutTtl.getCreatedAt()), null, false),
                   row(SNAPSHOT_TTL, KEYSPACE, currentTable(), toDate(snapshotWithTtl.getCreatedAt()), toDate(snapshotWithTtl.getExpiresAt()), false));

        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where name = ?", SNAPSHOT_TTL);
        assertRows(result,
                   row(SNAPSHOT_TTL, KEYSPACE, currentTable(), toDate(snapshotWithTtl.getCreatedAt()), toDate(snapshotWithTtl.getExpiresAt()), false));

        // clear some snapshots
        SnapshotManager.instance.clearSnapshot(SNAPSHOT_NO_TTL, Collections.emptyMap(), KEYSPACE);

        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots");
        assertRowsIgnoringOrder(result,
                                row(SNAPSHOT_TTL, KEYSPACE, currentTable(), toDate(snapshotWithTtl.getCreatedAt()), toDate(snapshotWithTtl.getExpiresAt()), false));
    }
}