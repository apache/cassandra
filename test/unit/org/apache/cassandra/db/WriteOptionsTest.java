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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.db.WriteOptions.DEFAULT;
import static org.apache.cassandra.db.WriteOptions.DEFAULT_WITHOUT_COMMITLOG;
import static org.apache.cassandra.db.WriteOptions.FOR_BATCH_REPLAY;
import static org.apache.cassandra.db.WriteOptions.FOR_BOOTSTRAP_STREAMING;
import static org.apache.cassandra.db.WriteOptions.FOR_HINT_REPLAY;
import static org.apache.cassandra.db.WriteOptions.FOR_PAXOS_COMMIT;
import static org.apache.cassandra.db.WriteOptions.FOR_READ_REPAIR;
import static org.apache.cassandra.db.WriteOptions.FOR_STREAMING;
import static org.apache.cassandra.db.WriteOptions.FOR_VIEW_BUILD;
import static org.apache.cassandra.db.WriteOptions.SKIP_INDEXES_AND_COMMITLOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriteOptionsTest extends CQLTester
{
    @Test
    public void testShouldWriteCommitLog() throws Throwable
    {
        String DURABLE_KEYSPACE = "durable_ks";
        String NON_DURABLE_KEYSPACE = "non_durable_ks";
        String DEFAULT_DURABLE_KEYSPACE = "ks_with_default_durability";

        Set<WriteOptions> WRITE_COMMIT_LOG_TRUE = Sets.newHashSet(FOR_BOOTSTRAP_STREAMING,
                FOR_STREAMING,
                FOR_PAXOS_COMMIT,
                FOR_VIEW_BUILD);
        Set<WriteOptions> WRITE_COMMIT_LOG_AUTO = Sets.newHashSet(DEFAULT,
                FOR_BATCH_REPLAY,
                FOR_HINT_REPLAY,
                FOR_READ_REPAIR);

        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true", DURABLE_KEYSPACE));
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = false", NON_DURABLE_KEYSPACE));
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", DEFAULT_DURABLE_KEYSPACE));
        try
        {
            for (WriteOptions opt : WriteOptions.values())
            {
                if (WRITE_COMMIT_LOG_AUTO.contains(opt))
                {
                    assertTrue(String.format("%s should write commit log", opt), opt.shouldWriteCommitLog(DURABLE_KEYSPACE));
                    assertTrue(String.format("%s should write commit log", opt), opt.shouldWriteCommitLog(DEFAULT_DURABLE_KEYSPACE));
                    assertFalse(String.format("%s should NOT write commit log", opt), opt.shouldWriteCommitLog(NON_DURABLE_KEYSPACE));
                }
                else
                {
                    assertEquals("CommitLog write for non durable keyspace for " + opt, WRITE_COMMIT_LOG_TRUE.contains(opt), opt.shouldWriteCommitLog(NON_DURABLE_KEYSPACE));
                    assertEquals("CommitLog write for default duratility for " + opt, WRITE_COMMIT_LOG_TRUE.contains(opt), opt.shouldWriteCommitLog(DEFAULT_DURABLE_KEYSPACE));
                }
            }
        }
        finally
        {
            execute(String.format("DROP KEYSPACE IF EXISTS %s", DURABLE_KEYSPACE));
            execute(String.format("DROP KEYSPACE IF EXISTS %s", NON_DURABLE_KEYSPACE));
        }
    }

    @Test
    public void testUpdateIndexes()
    {
        Set<WriteOptions> SKIP_INDEXES = Sets.newHashSet(SKIP_INDEXES_AND_COMMITLOG);

        for (WriteOptions opt : WriteOptions.values())
        {
            assertEquals(!SKIP_INDEXES.contains(opt), opt.updateIndexes);
        }
    }

    @Test
    public void testRequiresViewUpdate() throws Throwable
    {
        Set<WriteOptions> DO_NOT_REQUIRE_VIEW_UPDATE = Sets.newHashSet(FOR_BOOTSTRAP_STREAMING);

        String TABLE_WITHOUT_VIEW = "table_without_view";
        executeNet(String.format("CREATE TABLE %s.%s (k1 int primary key, v1 int)", keyspace(), TABLE_WITHOUT_VIEW));

        String TABLE_WITH_VIEW = "base_table";
        String VIEW = "mv";
        executeNet(String.format("CREATE TABLE %s.%s (k1 int primary key, v1 int)", keyspace(), TABLE_WITH_VIEW));
        executeNet(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s WHERE k1 IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k1)", keyspace(), VIEW, TABLE_WITH_VIEW));

        Mutation tableWithoutViewMutation = createMutation(TABLE_WITHOUT_VIEW);
        Mutation tableWithViewMutation = createMutation(TABLE_WITH_VIEW);
        Mutation mixedMutation = createMutation(TABLE_WITH_VIEW, TABLE_WITHOUT_VIEW);

        Keyspace ks = Keyspace.open(keyspace());

        for (WriteOptions opt : WriteOptions.values())
        {
            if (DO_NOT_REQUIRE_VIEW_UPDATE.contains(opt))
            {
                assertFalse(opt.requiresViewUpdate(ks.viewManager, tableWithoutViewMutation));
                assertFalse(opt.requiresViewUpdate(ks.viewManager, tableWithViewMutation));
                assertFalse(opt.requiresViewUpdate(ks.viewManager, mixedMutation));
            }
            else
            {
                assertFalse(opt.requiresViewUpdate(ks.viewManager, tableWithoutViewMutation));
                assertEquals(opt.updateIndexes, opt.requiresViewUpdate(ks.viewManager, tableWithViewMutation));
            }
        }
    }

    private Mutation createMutation(String... tables)
    {
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(keyspace(), DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.wrap("key".getBytes())));
        for (String table : tables)
            builder.update(table).row().add("v1", 1);
        return builder.build();
    }

    @Test
    public void testUsePairedViewReplication()
    {
        Set<WriteOptions> USE_PAIRED_VIEW_REPLICATION = Sets.newHashSet(DEFAULT, DEFAULT_WITHOUT_COMMITLOG, FOR_PAXOS_COMMIT, FOR_VIEW_BUILD);
        for (WriteOptions opt : WriteOptions.values())
        {
            assertEquals(USE_PAIRED_VIEW_REPLICATION.contains(opt), opt.usePairedViewReplication);
        }
    }
}