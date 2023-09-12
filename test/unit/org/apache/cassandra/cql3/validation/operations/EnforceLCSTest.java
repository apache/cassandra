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

import java.util.HashMap;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.transport.ProtocolVersion;

public class EnforceLCSTest extends CQLTester
{
    public static HashMap<String, Class<? extends AbstractCompactionStrategy>>
    originalSystemSchemaCompactionStrategies = getSystemSchemaCompactionStrategies();

    @Test
    public void testAlterOnCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "mutate compaction strategy",
                                  InvalidQueryException.class,
                                  formatQuery("ALTER TABLE %s WITH compaction={'class': 'LeveledCompactionStrategy'};"));
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "mutate compaction strategy",
                                  InvalidQueryException.class,
                                  formatQuery("ALTER TABLE %s WITH compaction={'class': 'LeveledCompactionStrategy'};"));
        // mutation can only be performed if enforcement level is set to none
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        execute(formatQuery("ALTER TABLE %s WITH compaction={'class': 'LeveledCompactionStrategy'};"));
        assertCompactionStrategy(LeveledCompactionStrategy.class.getSimpleName());
    }

    @Test
    public void testNonSpecifiedCompactionForCreate() throws Throwable
    {
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionStrategy(LeveledCompactionStrategy.class.getSimpleName(), KEYSPACE, table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionStrategy(LeveledCompactionStrategy.class.getSimpleName(), KEYSPACE, table2);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionStrategy(CompactionParams.DEFAULT.klass().getName(), KEYSPACE, table3);
    }

    @Test
    public void testSpecifiedCompactionForCreate() throws Throwable
    {
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "LCS enforcement is enabled",
                                  InvalidQueryException.class,
                                  "CREATE TABLE " + KEYSPACE + '.' + createTableName() +
                                  " (id text PRIMARY KEY, content text) WITH compaction={'class': 'SizeTieredCompactionStrategy'};");

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'SizeTieredCompactionStrategy'};");
        assertCompactionStrategy(LeveledCompactionStrategy.class.getSimpleName(), KEYSPACE, table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'SizeTieredCompactionStrategy'};");
        assertCompactionStrategy(SizeTieredCompactionStrategy.class.getSimpleName(), KEYSPACE, table2);
    }

    @Test
    public void testEnforcementShouldNotAffectSystemSchema() throws Throwable
    {
        Assert.assertEquals(Config.LCSEnforcementLevel.none, DatabaseDescriptor.getLCSEnforcementLevel());
        Assert.assertTrue(isCurrentSystemSchemaCompactionStrategiesUnchanged());

        // force re-write system schema
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        Schema.instance.reloadSchemaAndAnnounceVersion();
        Assert.assertEquals(Config.LCSEnforcementLevel.hard, DatabaseDescriptor.getLCSEnforcementLevel());
        Assert.assertTrue(isCurrentSystemSchemaCompactionStrategiesUnchanged());

        // force re-write system schema
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        Schema.instance.reloadSchemaAndAnnounceVersion();
        Assert.assertEquals(Config.LCSEnforcementLevel.soft, DatabaseDescriptor.getLCSEnforcementLevel());
        Assert.assertTrue(isCurrentSystemSchemaCompactionStrategiesUnchanged());
    }

    private void assertCompactionStrategy(String expected) throws Throwable
    {
        assertCompactionStrategy(expected, KEYSPACE, currentTable());
    }

    private void assertCompactionStrategy(String expected, String keyspace, String table) throws Throwable
    {
        expected = expected.contains(".")
                 ? expected
                 : "org.apache.cassandra.db.compaction." + expected;

        TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace, table);
        if (tableMetadata == null) {
            Assert.fail(String.format("TableMetadata not found for %s.%s", keyspace, table));
        }
        Assert.assertEquals(expected, tableMetadata.params.compaction.klass().getName());
    }

    private boolean isCurrentSystemSchemaCompactionStrategiesUnchanged() {
        for (String ks : SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES) {
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(ks);
            Assert.assertNotNull(ksMetadata);
            for (TableMetadata table : ksMetadata.tables) {
                if (!isSystemSchemaCompactionStrategyUnchanged(ks, table.name, table.params.compaction.klass())) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isSystemSchemaCompactionStrategyUnchanged(String keyspace,
                                                              String schema,
                                                              Class<? extends AbstractCompactionStrategy> actualCS) {
        return originalSystemSchemaCompactionStrategies.get(keyspace) != null
               ? originalSystemSchemaCompactionStrategies.get(keyspace).equals(actualCS)
               : originalSystemSchemaCompactionStrategies.get(keyspace + '.' + schema).equals(actualCS);
    }

    /**
     * Note: this schema-to-compaction-strategies map is hard-coded for confirming the enforcement
     * flag won't affect system schema behaviors in compaction options.
     * Will probably break if {@link CreateTableStatement#parse(String, String)}
     * changes or any of the System schema changes its compaction parameters.
     * @return schemaToCompactionStrategy
     */
    private static HashMap<String, Class<? extends AbstractCompactionStrategy>> getSystemSchemaCompactionStrategies() {
        HashMap<String, Class<? extends AbstractCompactionStrategy>> schemaToCompactionStrategy = new HashMap<>();
        Class<? extends AbstractCompactionStrategy> defaultCS = CompactionParams.DEFAULT.klass();

        // system
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.BUILT_INDEXES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_AVAILABLE_RANGES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.AVAILABLE_RANGES_V2,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.BATCHES,
                                       SizeTieredCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.BUILT_VIEWS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.COMPACTION_HISTORY,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LOCAL,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PAXOS,
                                       LeveledCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_PEER_EVENTS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PEER_EVENTS_V2,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_PEERS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PEERS_V2,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.REPAIRS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.TOP_PARTITIONS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PAXOS_REPAIR_HISTORY,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_SIZE_ESTIMATES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_SSTABLE_ACTIVITY,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.SSTABLE_ACTIVITY_V2,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.TABLE_ESTIMATES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_TRANSFERRED_RANGES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.TRANSFERRED_RANGES_V2,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.VIEW_BUILDS_IN_PROGRESS,
                                       defaultCS);

        // system_schema (all using default compaction strategy)
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.KEYSPACES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.TABLES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.COLUMNS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.DROPPED_COLUMNS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.TRIGGERS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.VIEWS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.INDEXES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.TYPES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.FUNCTIONS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspaceTables.AGGREGATES,
                                       defaultCS);

        // system_auth (all using default compaction strategy)
        schemaToCompactionStrategy.put(SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLES,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLE_MEMBERS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLE_PERMISSIONS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.RESOURCE_ROLE_INDEX,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.NETWORK_PERMISSIONS,
                                       defaultCS);

        // system_distributed
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.REPAIR_HISTORY,
                                       TimeWindowCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.PARENT_REPAIR_HISTORY,
                                       TimeWindowCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.VIEW_BUILD_STATUS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.AUDIT_USER,
                                       LeveledCompactionStrategy.class);

        // system_trace (all using default compaction strategy)
        schemaToCompactionStrategy.put(SchemaConstants.TRACE_KEYSPACE_NAME + '.' + TraceKeyspace.SESSIONS, defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.TRACE_KEYSPACE_NAME + '.' + TraceKeyspace.EVENTS, defaultCS);

        // system_views, system_virtual_schema are virtual keyspaces. Virtual tables won't create SSTables

        // system_auto_repair (all using default compaction strategy)
        schemaToCompactionStrategy.put(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME + '.' + "auto_repair_history", defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME + '.' + "auto_repair_priority", defaultCS);

        return schemaToCompactionStrategy;
    }
}
