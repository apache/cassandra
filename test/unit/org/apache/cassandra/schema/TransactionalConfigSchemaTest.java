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

package org.apache.cassandra.schema;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;

import static java.lang.String.format;

public class TransactionalConfigSchemaTest
{
    private static final String KEYSPACE = "ks";
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of()));
    }

    private static void process(String fmt, Object... objects)
    {
        QueryProcessor.process(format(fmt, objects), ConsistencyLevel.ANY);
    }

    private static void assertTransactionalMode(String table, TransactionalMode mode, TransactionalMigrationFromMode migration)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, table);
        Assert.assertEquals(mode, metadata.params.transactionalMode);
        Assert.assertEquals(migration, metadata.params.transactionalMigrationFrom);
    }

    // if a table is created with an accord transactional mode, it skips having to migrate
    @Test
    public void newTableSkipsMigration()
    {
        String table = "new_table";
        process("CREATE TABLE ks.%s (k int primary key, v int) WITH transactional_mode='%s'", table, TransactionalMode.full);
        assertTransactionalMode(table, TransactionalMode.full, TransactionalMigrationFromMode.none);
    }

    // if an existing table is set to an accord transactional mode, it should be set to migrating
    @Test
    public void existingTableMigration()
    {
        String table = "existing_table";
        process("CREATE TABLE ks.%s (k int primary key, v int)", table);
        assertTransactionalMode(table, TransactionalMode.off, TransactionalMigrationFromMode.none);

        process("ALTER TABLE ks.%s WITH transactional_mode='%s'", table, TransactionalMode.full);
        assertTransactionalMode(table, TransactionalMode.full, TransactionalMigrationFromMode.off);
    }

    // changing transactional mode with an incomplete migration should fail, unless the migration mode is explicitly updated
    @Test
    public void incompleteMigrationFailure()
    {
        String table = "incomplete_table";
        process("CREATE TABLE ks.%s (k int primary key, v int)", table);
        process("ALTER TABLE ks.%s WITH transactional_mode='%s'", table, TransactionalMode.full);
        assertTransactionalMode(table, TransactionalMode.full, TransactionalMigrationFromMode.off);

        process("ALTER TABLE ks.%s WITH transactional_mode='%s'", table, TransactionalMode.off);
        assertTransactionalMode(table, TransactionalMode.off, TransactionalMigrationFromMode.full);

        // explicitly setting the migration mode should work
        process("ALTER TABLE ks.%s WITH transactional_mode='%s' AND transactional_migration_from='%s'",
                table, TransactionalMode.off, TransactionalMigrationFromMode.none);
        assertTransactionalMode(table, TransactionalMode.off, TransactionalMigrationFromMode.none);
    }
}
