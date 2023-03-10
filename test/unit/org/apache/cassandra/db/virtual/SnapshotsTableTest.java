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
import java.util.Date;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.apache.cassandra.db.virtual.SnapshotsTable.parseTimestamp;

public class SnapshotsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String SNAPSHOT_TTL = "snapshotTtl";
    private static final String SNAPSHOT_NO_TTL = "snapshotNoTtl";
    private static final String SNAPSHOT_EPHEMERAL = "snapshotEphemeral";
    private static final DurationSpec.IntSecondsBound ttl = new DurationSpec.IntSecondsBound("4h");

    @Before
    public void before() throws Throwable
    {
        SnapshotsTable table = new SnapshotsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");

        for (int i = 0; i != 10; ++i)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", i, i);

        flush();
    }

    @After
    public void after()
    {
        clearSnapshot(SNAPSHOT_NO_TTL, currentKeyspace());
        clearSnapshot(SNAPSHOT_TTL, currentKeyspace());
        clearSnapshot(SNAPSHOT_EPHEMERAL, currentKeyspace());
        dropTable("DROP TABLE %s");
    }

    @Test
    public void testSnapshots() throws Throwable
    {
        Instant now = Instant.now();
        Date createdAt = parseTimestamp(now.toString());
        Date expiresAt = parseTimestamp(now.plusSeconds(ttl.toSeconds()).toString());

        snapshot(SNAPSHOT_NO_TTL, now);
        snapshot(SNAPSHOT_TTL, ttl, now);
        snapshot(SNAPSHOT_EPHEMERAL, true, null, now);

        // query all from snapshots virtual table
        UntypedResultSet result = execute("SELECT id, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots");
        assertRowsIgnoringOrder(result,
                                row(SNAPSHOT_EPHEMERAL, CQLTester.KEYSPACE, currentTable(), createdAt, null, true),
                                row(SNAPSHOT_NO_TTL, CQLTester.KEYSPACE, currentTable(), createdAt, null, false),
                                row(SNAPSHOT_TTL, CQLTester.KEYSPACE, currentTable(), createdAt, expiresAt, false));

        // query with conditions
        result = execute("SELECT id, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where ephemeral = true");
        assertRows(result,
                   row(SNAPSHOT_EPHEMERAL, CQLTester.KEYSPACE, currentTable(), createdAt, null, true));

        result = execute("SELECT id, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots where id = ?", SNAPSHOT_TTL);
        assertRows(result,
                   row(SNAPSHOT_TTL, CQLTester.KEYSPACE, currentTable(), createdAt, expiresAt, false));

        // clear some snapshots
        clearSnapshot(SNAPSHOT_NO_TTL, CQLTester.KEYSPACE);

        result = execute("SELECT id, keyspace_name, table_name, created_at, expires_at, ephemeral FROM vts.snapshots");
        assertRowsIgnoringOrder(result,
                                row(SNAPSHOT_EPHEMERAL, CQLTester.KEYSPACE, currentTable(), createdAt, null, true),
                                row(SNAPSHOT_TTL, CQLTester.KEYSPACE, currentTable(), createdAt, expiresAt, false));
    }
}