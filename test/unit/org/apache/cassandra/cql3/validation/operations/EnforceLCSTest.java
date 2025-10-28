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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
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

    public static final Config.LCSEnforcementLevel originalLevel = DatabaseDescriptor.getLCSEnforcementLevel();
    public static final int originalSSTableSize = DatabaseDescriptor.getLCSSSTableSizeInMB();
    Set<String> sysKeyspaces = ImmutableSet.of(SchemaConstants.SYSTEM_KEYSPACE_NAME, SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                               SchemaConstants.AUTH_KEYSPACE_NAME, SchemaConstants.TRACE_KEYSPACE_NAME);
    List<String> systemTablesToBeVerified = Arrays.asList(
    "system.IndexInfo", "system.available_ranges", "system.available_ranges_v2",
    "system.batches", "system.built_views", "system.compaction_history",
    "system.local", "system.paxos", "system.paxos_repair_history",
    "system.peer_events", "system.peer_events_v2", "system.peers", "system.peers_v2",
    "system.prepared_statements", "system.repairs", "system.size_estimates",
    "system.sstable_activity", "system.sstable_activity_v2", "system.table_estimates",
    "system.top_partitions", "system.transferred_ranges", "system.transferred_ranges_v2",
    "system.view_backfills", "system.view_builds_in_progress",
    "system_auth.network_permissions", "system_auth.resource_role_permissons_index",
    "system_auth.role_members", "system_auth.role_permissions", "system_auth.roles",
    "system_distributed.audit_users", "system_distributed.parent_repair_history",
    "system_distributed.partition_denylist", "system_distributed.repair_history",
    "system_distributed.view_build_status", "system_distributed.auto_repair_history",
    "system_distributed.auto_repair_priority", "system_distributed.mv_backfill_status",
    "system_schema.aggregates", "system_schema.columns", "system_schema.dropped_columns",
    "system_schema.functions", "system_schema.indexes", "system_schema.keyspaces",
    "system_schema.tables", "system_schema.triggers", "system_schema.types",
    "system_schema.views",
    "system_traces.events", "system_traces.sessions"
    );

    @Before
    public void init()
    {
        DatabaseDescriptor.setLCSEnforcementLevel(originalLevel);
        DatabaseDescriptor.setLCSSSTableSizeInMB(originalSSTableSize);
    }

    @Test
    public void testNonSpecifiedCompactionForCreate() throws Throwable
    {
        CompactionParams expectedLCSefault = CompactionParams.lcs(Collections.singletonMap(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, String.valueOf(DatabaseDescriptor.getLCSSSTableSizeInMB())));
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionParams(expectedLCSefault, KEYSPACE, table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionParams(expectedLCSefault, KEYSPACE, table2);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionParams(CompactionParams.DEFAULT, KEYSPACE, table3);
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
        CompactionParams expectedLCSefault = CompactionParams.lcs(Collections.singletonMap(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, String.valueOf(DatabaseDescriptor.getLCSSSTableSizeInMB())));
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'SizeTieredCompactionStrategy'};");
        assertCompactionParams(expectedLCSefault, KEYSPACE, table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'SizeTieredCompactionStrategy'};");
        assertCompactionParams(CompactionParams.stcs(Collections.emptyMap()), KEYSPACE, table2);
    }

    @Test
    public void testEnforcementShouldNotAffectSystemSchema() throws Throwable
    {
        requireNetwork();
        Assert.assertEquals(Config.LCSEnforcementLevel.none, DatabaseDescriptor.getLCSEnforcementLevel());
        Set<String> actualTablesVerified = new TreeSet<>();
        Assert.assertTrue(isCurrentSystemSchemaCompactionStrategiesUnchanged(actualTablesVerified));
        Assert.assertEquals(new TreeSet<>(systemTablesToBeVerified), actualTablesVerified);
        actualTablesVerified.clear();

        // force re-write system schema
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        Schema.instance.reloadSchemaAndAnnounceVersion();
        Assert.assertEquals(Config.LCSEnforcementLevel.hard, DatabaseDescriptor.getLCSEnforcementLevel());
        Assert.assertTrue(isCurrentSystemSchemaCompactionStrategiesUnchanged(actualTablesVerified));
        Assert.assertEquals(new TreeSet<>(systemTablesToBeVerified), actualTablesVerified);
        actualTablesVerified.clear();

        // force re-write system schema
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        Schema.instance.reloadSchemaAndAnnounceVersion();
        Assert.assertEquals(Config.LCSEnforcementLevel.soft, DatabaseDescriptor.getLCSEnforcementLevel());
        Assert.assertTrue(isCurrentSystemSchemaCompactionStrategiesUnchanged(actualTablesVerified));
        Assert.assertEquals(new TreeSet<>(systemTablesToBeVerified), actualTablesVerified);
    }

    @Test
    public void testSkipEnforcementWhenSchemaExists() throws Throwable
    {
        Assert.assertEquals(Config.LCSEnforcementLevel.none, DatabaseDescriptor.getLCSEnforcementLevel());

        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'SizeTieredCompactionStrategy'};");

        // should skip enforcement check if already exist, and existed schema unchanged
        schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, content text) WITH " +
                                   "compaction={'class': 'SizeTieredCompactionStrategy'};", keyspace(), table1));
        assertCompactionParams(CompactionParams.stcs(Collections.emptyMap()), keyspace(), table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        try
        {
            schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, content text) WITH " +
                                       "compaction={'class': 'SizeTieredCompactionStrategy'};", keyspace(), table1));
        }
        catch (Exception e)
        {
            // should not see exception
            Assert.fail("unexpected Exception");
        }
        assertCompactionParams(CompactionParams.stcs(Collections.emptyMap()), keyspace(), table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, content text) WITH " +
                                   "compaction={'class': 'SizeTieredCompactionStrategy'};", keyspace(), table1));
        assertCompactionParams(CompactionParams.stcs(Collections.emptyMap()), keyspace(), table1);
    }

    @Test
    public void testOverrideLCSDefaultSSTableSizeInMB()
    {
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'LeveledCompactionStrategy'};");
        assertCompactionParams(CompactionParams.lcs(Collections.singletonMap(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, String.valueOf(DatabaseDescriptor.getLCSSSTableSizeInMB()))),
                               KEYSPACE, table1);
        // override if set
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '160', 'enabled': 'false'};");
        assertCompactionParams(CompactionParams.lcs(Map.of(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, String.valueOf(DatabaseDescriptor.getLCSSSTableSizeInMB()),
                                                           "enabled", "false")),
                               KEYSPACE, table2);

        // other default
        DatabaseDescriptor.setLCSSSTableSizeInMB(320);
        Assert.assertEquals(320, DatabaseDescriptor.getLCSSSTableSizeInMB());
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH " +
                                    "compaction={'class': 'LeveledCompactionStrategy'};");
        assertCompactionParams(CompactionParams.lcs(Collections.singletonMap(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, "320")),
                               KEYSPACE, table3);
    }

    private void assertCompactionParams(CompactionParams expected, String keyspace, String table)
    {
        TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace, table);
        if (tableMetadata == null)
        {
            Assert.fail(String.format("TableMetadata not found for %s.%s", keyspace, table));
        }
        Assert.assertEquals(expected, tableMetadata.params.compaction);
    }

    private boolean isCurrentSystemSchemaCompactionStrategiesUnchanged(Set<String> tablesVerified)
    {
        for (String ks : sysKeyspaces)
        {
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(ks);
            Assert.assertNotNull(ksMetadata);
            for (TableMetadata table : ksMetadata.tables)
            {
                tablesVerified.add(ks + '.' + table.name);
                if (!isSystemSchemaCompactionStrategyUnchanged(ks, table.name, table.params.compaction.klass()))
                {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isSystemSchemaCompactionStrategyUnchanged(String keyspace,
                                                              String schema,
                                                              Class<? extends AbstractCompactionStrategy> actualCS)
    {
        return originalSystemSchemaCompactionStrategies.get(keyspace) != null
               ? originalSystemSchemaCompactionStrategies.get(keyspace).equals(actualCS)
               : originalSystemSchemaCompactionStrategies.get(keyspace + '.' + schema).equals(actualCS);
    }

    /**
     * Note: this schema-to-compaction-strategies map is hard-coded for confirming the enforcement
     * flag won't affect system schema behaviors in compaction options.
     * Will probably break if {@link CreateTableStatement#parse(String, String)}
     * changes or any of the System schema changes its compaction parameters.
     *
     * @return schemaToCompactionStrategy
     */
    private static HashMap<String, Class<? extends AbstractCompactionStrategy>> getSystemSchemaCompactionStrategies()
    {
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
        schemaToCompactionStrategy.put(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.VIEW_BACKFILLS,
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
                                       LeveledCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.PARENT_REPAIR_HISTORY,
                                       LeveledCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.VIEW_BUILD_STATUS,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE,
                                       defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.AUDIT_USER,
                                       LeveledCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.AUTO_REPAIR_HISTORY,
                                       LeveledCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY,
                                       LeveledCompactionStrategy.class);
        schemaToCompactionStrategy.put(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + '.' + SystemDistributedKeyspace.MV_BACKFILL_STATUS,
                                       LeveledCompactionStrategy.class);

        // system_trace (all using default compaction strategy)
        schemaToCompactionStrategy.put(SchemaConstants.TRACE_KEYSPACE_NAME + '.' + TraceKeyspace.SESSIONS, defaultCS);
        schemaToCompactionStrategy.put(SchemaConstants.TRACE_KEYSPACE_NAME + '.' + TraceKeyspace.EVENTS, defaultCS);

        // system_views, system_virtual_schema are virtual keyspaces. Virtual tables won't create SSTables
        return schemaToCompactionStrategy;
    }
}
