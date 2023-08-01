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
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;

public class SnapshotsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String SNAPSHOT_TTL = "snapshotTtl";
    private static final String SNAPSHOT_NO_TTL = "snapshotNoTtl";
    private static final DurationSpec.IntSecondsBound ttl = new DurationSpec.IntSecondsBound("4h");

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
        StorageService.instance.clearSnapshot(Collections.emptyMap(), SNAPSHOT_NO_TTL, KEYSPACE);
        StorageService.instance.clearSnapshot(Collections.emptyMap(), SNAPSHOT_TTL, KEYSPACE);

        schemaChange(String.format("DROP TABLE %s", KEYSPACE + "." + currentTable()));
    }

    @Test
    public void testSnapshots() throws Throwable
    {
        Instant now = Instant.ofEpochMilli(Clock.Global.currentTimeMillis()).truncatedTo(ChronoUnit.MILLIS);
        Date createdAt = new Date(now.toEpochMilli());
        Date expiresAt = new Date(now.plusSeconds(ttl.toSeconds()).toEpochMilli());

        getCurrentColumnFamilyStore(KEYSPACE).snapshot(SNAPSHOT_NO_TTL, null, false, false, null, null, now);
        getCurrentColumnFamilyStore(KEYSPACE).snapshot(SNAPSHOT_TTL, null, false, false, ttl, null, now);

        // query all from snapshots virtual table
        UntypedResultSet result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots");
        assertRowsIgnoringOrder(result,
                                row(SNAPSHOT_NO_TTL, KEYSPACE, currentTable(), createdAt, null, false),
                                row(SNAPSHOT_TTL, KEYSPACE, currentTable(), createdAt, expiresAt, false));

        // query with conditions
        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where ephemeral = false");
        assertRows(result,
                   row(SNAPSHOT_NO_TTL, KEYSPACE, currentTable(), createdAt, null, false),
                   row(SNAPSHOT_TTL, KEYSPACE, currentTable(), createdAt, expiresAt, false));

        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where size_on_disk > 1000");
        assertRows(result,
                   row(SNAPSHOT_NO_TTL, KEYSPACE, currentTable(), createdAt, null, false),
                   row(SNAPSHOT_TTL, KEYSPACE, currentTable(), createdAt, expiresAt, false));

        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where name = ?", SNAPSHOT_TTL);
        assertRows(result,
                   row(SNAPSHOT_TTL, KEYSPACE, currentTable(), createdAt, expiresAt, false));

        // clear some snapshots
        StorageService.instance.clearSnapshot(Collections.emptyMap(), SNAPSHOT_NO_TTL, KEYSPACE);

        result = execute("SELECT name, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots");
        assertRowsIgnoringOrder(result,
                                row(SNAPSHOT_TTL, KEYSPACE, currentTable(), createdAt, expiresAt, false));
    }
}