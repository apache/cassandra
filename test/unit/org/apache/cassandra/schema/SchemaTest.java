/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.util.function.Predicate;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaTest
{
    private static final String KS_PREFIX = "schema_test_ks_";
    private static final String KS_ONE = KS_PREFIX + "1";
    private static final String KS_TWO = KS_PREFIX + "2";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Before
    public void clearSchema()
    {
        SchemaTestUtil.dropKeyspaceIfExist(KS_ONE, true);
        SchemaTestUtil.dropKeyspaceIfExist(KS_TWO, true);
    }

    @Test
    public void tablesInNewKeyspaceHaveCorrectEpoch()
    {
        Tables tables = Tables.of(TableMetadata.minimal(KS_ONE, "modified1"),
                                  TableMetadata.minimal(KS_ONE, "modified2"));
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1), tables);
        applyAndAssertTableMetadata((metadata, schema) -> schema.withAddedOrUpdated(ksm), true);
    }

    @Test
    public void newTablesInExistingKeyspaceHaveCorrectEpoch()
    {
        // Create an empty keyspace
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1));
        Schema.instance.submit((metadata, schema) -> schema.withAddedOrUpdated(ksm));

        // Add two tables and verify that the resultant table metadata has the correct epoch
        Tables tables = Tables.of(TableMetadata.minimal(KS_ONE, "modified1"), TableMetadata.minimal(KS_ONE, "modified2"));
        KeyspaceMetadata updated = ksm.withSwapped(tables);
        applyAndAssertTableMetadata((metadata, schema) -> schema.withAddedOrUpdated(updated), true);
    }

    @Test
    public void newTablesInNonEmptyKeyspaceHaveCorrectEpoch()
    {
        Tables tables = Tables.of(TableMetadata.minimal(KS_ONE, "unmodified"));
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1), tables);
        Schema.instance.submit((metadata, schema) -> schema.withAddedOrUpdated(ksm));

        // Add a second table and assert that its table metadata has the latest epoch, but that the
        // metadata of the other table stays unmodified
        KeyspaceMetadata updated = ksm.withSwapped(tables.with(TableMetadata.minimal(KS_ONE, "modified1")));
        applyAndAssertTableMetadata((metadata, schema) -> schema.withAddedOrUpdated(updated));
    }

    @Test
    public void createTableCQLSetsCorrectEpoch()
    {
        Tables tables = Tables.of(TableMetadata.minimal(KS_ONE, "unmodified"));
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1), tables);
        Schema.instance.submit((metadata, schema) -> schema.withAddedOrUpdated(ksm));

        applyAndAssertTableMetadata(cql(KS_ONE, "CREATE TABLE %s.modified (k int PRIMARY KEY)"));
    }

    @Test
    public void createTablesInMultipleKeyspaces()
    {
        KeyspaceMetadata ksm1 = KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1));
        KeyspaceMetadata ksm2 = KeyspaceMetadata.create(KS_TWO, KeyspaceParams.simple(1));
        Schema.instance.submit((metadata, schema) -> schema.withAddedOrUpdated(ksm1).withAddedOrUpdated(ksm2));

        // Add two tables in each ks and verify that the resultant table metadata has the correct epoch
        Tables tables1 = Tables.of(TableMetadata.minimal(KS_ONE, "modified1"), TableMetadata.minimal(KS_ONE, "modified2"));
        KeyspaceMetadata updated1 = ksm1.withSwapped(tables1);
        Tables tables2 = Tables.of(TableMetadata.minimal(KS_TWO, "modified1"), TableMetadata.minimal(KS_TWO, "modified2"));
        KeyspaceMetadata updated2 = ksm2.withSwapped(tables2);
        applyAndAssertTableMetadata((metadata, schema) -> schema.withAddedOrUpdated(updated1)
                                                                .withAddedOrUpdated(updated2),
                                    true);
    }


    @Test
    public void createTablesInMultipleNonEmptyKeyspaces()
    {
        KeyspaceMetadata ksm1 = KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1));
        KeyspaceMetadata ksm2 = KeyspaceMetadata.create(KS_TWO, KeyspaceParams.simple(1));
        Schema.instance.submit((metadata, schema) -> schema.withAddedOrUpdated(ksm1).withAddedOrUpdated(ksm2));

        // Add two tables in each ks and verify that the resultant table metadata has the correct epoch
        Tables tables1 = Tables.of(TableMetadata.minimal(KS_ONE, "unmodified1"), TableMetadata.minimal(KS_ONE, "unmodified2"));
        KeyspaceMetadata updated1 = ksm1.withSwapped(tables1);
        Tables tables2 = Tables.of(TableMetadata.minimal(KS_TWO, "unmodified1"), TableMetadata.minimal(KS_TWO, "unmodified2"));
        KeyspaceMetadata updated2 = ksm2.withSwapped(tables2);
        Schema.instance.submit((metadata, schema) -> schema.withAddedOrUpdated(updated1).withAddedOrUpdated(updated2));

        // Add a third table in one ks and assert that its table metadata has the latest epoch, but that the
        // metadata of the all other tables stays unmodified
        applyAndAssertTableMetadata(cql(KS_ONE, "CREATE TABLE %s.modified (k int PRIMARY KEY)"));
    }

    @Test
    public void alterTableAndVerifyEpoch()
    {
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1)), true);
        Schema.instance.submit(cql(KS_ONE, "CREATE TABLE %s.unmodified (k int PRIMARY KEY)"));
        Schema.instance.submit(cql(KS_ONE, "CREATE TABLE %s.modified ( " +
                                           "k int, " +
                                           "c1 int, " +
                                           "v1 text, " +
                                           "v2 text, " +
                                           "v3 text, " +
                                           "v4 text," +
                                           "PRIMARY KEY(k,c1))"));

        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified DROP v4"));
        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified ADD v5 text"));
        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified RENAME c1 TO c2"));
        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified WITH comment = 'altered'"));
    }

    @Test
    public void alterTableMultipleKeyspacesAndVerifyEpoch()
    {
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KS_ONE, KeyspaceParams.simple(1)), true);
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KS_TWO, KeyspaceParams.simple(1)), true);
        Schema.instance.submit(cql(KS_ONE, "CREATE TABLE %s.unmodified (k int PRIMARY KEY)"));
        Schema.instance.submit(cql(KS_TWO, "CREATE TABLE %s.unmodified (k int PRIMARY KEY)"));
        Schema.instance.submit(cql(KS_ONE, "CREATE TABLE %s.modified ( " +
                                           "k int, " +
                                           "c1 int, " +
                                           "v1 text, " +
                                           "v2 text, " +
                                           "v3 text, " +
                                           "v4 text," +
                                           "PRIMARY KEY(k, c1))"));

        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified DROP v4"));
        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified ADD v5 text"));
        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified RENAME c1 TO c2"));
        applyAndAssertTableMetadata(cql(KS_ONE, "ALTER TABLE %s.modified WITH comment = 'altered'"));
    }

    private void applyAndAssertTableMetadata(SchemaTransformation transformation)
    {
        applyAndAssertTableMetadata(transformation, false);
    }

    private void applyAndAssertTableMetadata(SchemaTransformation transformation, boolean onlyModified)
    {
        Epoch before = ClusterMetadata.current().epoch;
        Schema.instance.submit(transformation);
        Epoch after = ClusterMetadata.current().epoch;
        assertTrue(after.isDirectlyAfter(before));
        DistributedSchema schema = ClusterMetadata.current().schema;
        Predicate<TableMetadata> modified = (tm) -> tm.name.startsWith("modified") && tm.epoch.is(after);
        Predicate<TableMetadata> predicate = onlyModified
                                             ? modified
                                             : modified.or((tm) -> tm.name.startsWith("unmodified") && tm.epoch.isBefore(after));

        schema.getKeyspaces().forEach(keyspace -> {
            if (keyspace.name.startsWith(KS_PREFIX))
            {
                boolean containsUnmodified = keyspace.tables.stream().anyMatch(tm -> tm.name.startsWith("unmodified"));
                assertEquals("Expected an unmodified table metadata but none found in " + keyspace.name, !onlyModified, containsUnmodified);
                assertTrue(keyspace.tables.stream().allMatch(predicate));
            }
        });
    }

    private static AlterSchemaStatement cql(String keyspace, String cql)
    {
        return (AlterSchemaStatement) QueryProcessor.parseStatement(String.format(cql, keyspace))
                                                    .prepare(ClientState.forInternalCalls());
    }
}
