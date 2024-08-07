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

package org.apache.cassandra.db.compaction;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CompactionStrategyMigrationOptions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompactionStrategyMigrationManagerTest extends CQLTester
{
    private static final String KS_1 = "ks1";
    private static final String KS_2 = "ks2";
    private static final String STCS_TBL = "stcstbl";
    private static final String LCS_TBL = "lcstbl";
    private static final String TWCS_TBL = "twcstbl";
    private static final String DEFAULT_COMPACTION_PARAMS_JSON = "{\"class\": \"LeveledCompactionStrategy\"}";
    private static final String SYSTEM_SCHEMA_TBL_COMPACTION_QUERY_TEMPLATE = "SELECT compaction FROM system_schema.tables WHERE keyspace_name='%s' AND table_name='%s';";
    private static final Map<ColumnFamilyStore, CompactionParams> originalCompactionParams = new HashMap<>();

    @Before
    public void before()
    {
        CQLTester.setUpClass();
        CQLTester.requireNetwork();
        SchemaLoader.createKeyspace(KS_1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_1, STCS_TBL)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KS_1, LCS_TBL)
                                                .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1"))),
                                    SchemaLoader.standardCFMD(KS_1, TWCS_TBL)
                                                .compaction(CompactionParams.twcs(Collections.emptyMap())));
        SchemaLoader.createKeyspace(KS_2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_2, STCS_TBL)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KS_2, LCS_TBL)
                                                .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1"))),
                                    SchemaLoader.standardCFMD(KS_2, TWCS_TBL)
                                                .compaction(CompactionParams.twcs(Collections.emptyMap())));
        for (Keyspace ks : Keyspace.all())
        {
            ks.getColumnFamilyStores().forEach(cfs -> {
                originalCompactionParams.put(cfs, cfs.getCompactionStrategyManager().getCompactionParams());
            });
        }
    }

    @After
    public void reset() throws Throwable
    {
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(new CompactionStrategyMigrationOptions(false));
        originalCompactionParams.clear();
        Schema.instance.transform(schema -> schema.without(KS_1));
        Schema.instance.transform(schema -> schema.without(KS_2));
        CompactionStrategyMigrationManager.instance.reset();
    }

    @Test
    public void featureDisabled()
    {
        assertFalse(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();
        // nothing should be changed
        compactionStrategyUnchangedForAllSchemas();
        // should report disabled
        assertFalse(CompactionStrategyMigrationManager.instance.getCompactionStrategyMigrationEnabled());
    }

    @Test
    public void featureEnabledForAllUserSchemas()
    {
        CompactionStrategyMigrationOptions options = new CompactionStrategyMigrationOptions(true);
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);

        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // user schemas should be changed to LCS
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(LeveledCompactionStrategy.class, cfs.getCompactionStrategyManager().getCompactionParams().klass());
        });
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(LeveledCompactionStrategy.class, cfs.getCompactionStrategyManager().getCompactionParams().klass());
        });
        // system schemas should be unchanged
        compactionStrategyUnchangedForSystemSchemas();

        // schema with the same target compaction strategy preserved former params
        assertEquals(originalCompactionParams.get(Keyspace.open(KS_1).getColumnFamilyStore(LCS_TBL)),
                     Keyspace.open(KS_1).getColumnFamilyStore(LCS_TBL).getCompactionStrategyManager().getCompactionParams());
        assertEquals(originalCompactionParams.get(Keyspace.open(KS_2).getColumnFamilyStore(LCS_TBL)),
                     Keyspace.open(KS_2).getColumnFamilyStore(LCS_TBL).getCompactionStrategyManager().getCompactionParams());

        // only 4 non-LCS schema are counted
        assertTrue(CompactionStrategyMigrationManager.instance.getCompactionStrategyMigrationEnabled());
        assertEquals(4, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
        assertEquals(0, CompactionStrategyMigrationManager.instance.getPendingTasksForMigration());
    }

    @Test
    public void featureEnabledForSpecificKeyspace()
    {
        CompactionStrategyMigrationOptions options = new CompactionStrategyMigrationOptions(true);
        options.keyspace_options.put(KS_1, DEFAULT_COMPACTION_PARAMS_JSON);
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().keyspace_options.containsKey(KS_1));
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().table_options.isEmpty());

        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // KS_1 should be overriden
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(LeveledCompactionStrategy.class, cfs.getCompactionStrategyManager().getCompactionParams().klass());
        });

        // KS_2 should be unchanged
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(originalCompactionParams.get(cfs),
                         cfs.getCompactionStrategyManager().getCompactionParams());
        });

        // system schemas should be unchanged
        compactionStrategyUnchangedForSystemSchemas();

        // schema with the same target compaction strategy preserved former params
        assertEquals(originalCompactionParams.get(Keyspace.open(KS_1).getColumnFamilyStore(LCS_TBL)),
                     Keyspace.open(KS_1).getColumnFamilyStore(LCS_TBL).getCompactionStrategyManager().getCompactionParams());
        assertEquals(originalCompactionParams.get(Keyspace.open(KS_2).getColumnFamilyStore(LCS_TBL)),
                     Keyspace.open(KS_2).getColumnFamilyStore(LCS_TBL).getCompactionStrategyManager().getCompactionParams());

        // only 2 non-LCS schema are counted
        assertTrue(CompactionStrategyMigrationManager.instance.getCompactionStrategyMigrationEnabled());
        assertEquals(2, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
        assertEquals(0, CompactionStrategyMigrationManager.instance.getPendingTasksForMigration());
    }

    @Test
    public void featureEnabledForSpecificTable()
    {
        CompactionStrategyMigrationOptions options = new CompactionStrategyMigrationOptions(true);
        CompactionParams expectedParams = CompactionParams.lcs(Collections.emptyMap());
        options.table_options.put(KS_1 + '.' + STCS_TBL, DEFAULT_COMPACTION_PARAMS_JSON);
        options.table_options.put(KS_2 + '.' + STCS_TBL, DEFAULT_COMPACTION_PARAMS_JSON);
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);

        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // KS_1.STCS_TBL should be overriden
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            if (cfs.name.equals(STCS_TBL))
            {
                assertEquals(expectedParams, cfs.getCompactionStrategyManager().getCompactionParams());
            }
            else
            {
                assertEquals(originalCompactionParams.get(cfs),
                             cfs.getCompactionStrategyManager().getCompactionParams());
            }
        });

        // KS_2.STCS_TBL should be overriden
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            if (cfs.name.equals(STCS_TBL))
            {
                assertEquals(expectedParams, cfs.getCompactionStrategyManager().getCompactionParams());
            }
            else
            {
                assertEquals(originalCompactionParams.get(cfs),
                             cfs.getCompactionStrategyManager().getCompactionParams());
            }
        });

        // system schemas should be unchanged
        compactionStrategyUnchangedForSystemSchemas();

        assertTrue(CompactionStrategyMigrationManager.instance.getCompactionStrategyMigrationEnabled());
        assertEquals(2, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
        assertEquals(0, CompactionStrategyMigrationManager.instance.getPendingTasksForMigration());
    }

    @Test
    public void ignoreSystemAndNotFoundTablesAndKeyspaces()
    {
        CompactionStrategyMigrationOptions options = new CompactionStrategyMigrationOptions(true);
        options.keyspace_options.put("nonexist", DEFAULT_COMPACTION_PARAMS_JSON);
        options.keyspace_options.put("system_auth", DEFAULT_COMPACTION_PARAMS_JSON);
        options.table_options.put(KS_2 + '.' + "randomTbl", DEFAULT_COMPACTION_PARAMS_JSON);
        options.table_options.put("system_schema" + '.' + "tables", DEFAULT_COMPACTION_PARAMS_JSON);
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);

        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // nothing should be changed
        compactionStrategyUnchangedForAllSchemas();

        assertTrue(CompactionStrategyMigrationManager.instance.getCompactionStrategyMigrationEnabled());
        assertEquals(0, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
        assertEquals(0, CompactionStrategyMigrationManager.instance.getPendingTasksForMigration());
    }

    @Test
    public void testGetCfsWithNonDefaultCompactionParams()
    {
        Map<String, String> res = CompactionStrategyMigrationManager.instance.getCfsWithNonDefaultCompactionParams();
        assertTrue(res.containsKey(KS_1 + '.' + LCS_TBL));
        assertTrue(res.containsKey(KS_2 + '.' + LCS_TBL));
        assertEquals(2, res.size());
        assertEquals("CompactionParams{class=org.apache.cassandra.db.compaction.LeveledCompactionStrategy, options={min_threshold=4, max_threshold=32, sstable_size_in_mb=1}}", res.get(KS_1 + '.' + LCS_TBL));
        assertEquals("CompactionParams{class=org.apache.cassandra.db.compaction.LeveledCompactionStrategy, options={min_threshold=4, max_threshold=32, sstable_size_in_mb=1}}", res.get(KS_2 + '.' + LCS_TBL));
    }

    @Test
    public void testApplySchemaChangesForCompactionParams()
    {
        CompactionStrategyMigrationOptions options = new CompactionStrategyMigrationOptions(true);
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        CompactionParams expectedParams = CompactionParams.lcs(Collections.emptyMap());
        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // apply schema change in system_schema
        CompactionStrategyMigrationManager.instance.applySchemaChangesForCompactionParams();

        // verify the system_schema table has changed for targeted tables
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            if (cfs.name.equals(LCS_TBL))
            {
                assertTableCompactionParamsInSystemSchemaKS(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")), cfs);
            }
            else
            {
                assertTableCompactionParamsInSystemSchemaKS(expectedParams, cfs);
            }
        });
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            if (cfs.name.equals(LCS_TBL))
            {
                assertTableCompactionParamsInSystemSchemaKS(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")), cfs);
            }
            else
            {
                assertTableCompactionParamsInSystemSchemaKS(expectedParams, cfs);
            }
        });
    }

    @Test
    public void testApplySchemaChangesForCompactionParamsNoopWhenFeatureDisabled()
    {
        assertFalse(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();
        // this should have no effect
        CompactionStrategyMigrationManager.instance.applySchemaChangesForCompactionParams();
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            assertTableCompactionParamsInSystemSchemaKS(originalCompactionParams.get(cfs), cfs);
        });
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            assertTableCompactionParamsInSystemSchemaKS(originalCompactionParams.get(cfs), cfs);
        });
    }

    @Test
    public void testReloadAndOverrideLocalCompactionStrategy() throws Exception
    {
        assertFalse(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        CompactionStrategyMigrationManager.instance.reloadAndOverrideLocalCompactionStrategy();
        // should still be in disabled state
        assertFalse(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);

        // change in cassandra.yaml file
        List<String> config = ImmutableList.of(
        "compaction_strategy_migration_options:",
        "    enabled: true",
        "    compaction_params_json: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy\\\"}\"",
        "    keyspace_options:",
        String.format("        %s: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\\\"}\"", KS_1),
        "    table_options:",
        String.format("        %s.%s: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy\\\"}\"", KS_2, LCS_TBL)
        );
        Path p = Files.createTempFile("test_config", ".yaml");
        URL url = p.toUri().toURL();
        List<String> originalLines = Files.readAllLines(p);
        try
        {
            List<String> lines = new ArrayList<>(config);
            // append new config to the temp file
            Files.write(p, originalLines, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            Files.write(p, lines, StandardOpenOption.APPEND);
            // a bit hacky way to override the config for this test
            Config.setOverrideLoadConfig(() -> new YamlConfigurationLoader().loadConfig(url));

            CompactionStrategyMigrationManager.instance.reloadAndOverrideLocalCompactionStrategy();
            assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
            assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().keyspace_options.containsKey(KS_1));
            assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().table_options.containsKey(KS_2 + '.' + LCS_TBL));

            // KS_1 should be overriden with LCS
            Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
                if (cfs.name.equals(LCS_TBL))
                {
                    assertEquals(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")), cfs.getCompactionStrategyManager().getCompactionParams());
                }
                else
                {
                    assertEquals(CompactionParams.lcs(Collections.emptyMap()), cfs.getCompactionStrategyManager().getCompactionParams());
                }
            });

            // Only KS_2.LCS_TBL should be overriden
            Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
                if (cfs.name.equals(LCS_TBL))
                {
                    assertEquals(CompactionParams.twcs(Collections.emptyMap()), cfs.getCompactionStrategyManager().getCompactionParams());
                }
                else
                {
                    assertEquals(originalCompactionParams.get(cfs),
                                 cfs.getCompactionStrategyManager().getCompactionParams());
                }
            });
        }
        finally
        {
            // restore
            Config.setOverrideLoadConfig(null);
            Files.delete(p);
        }
    }

    @Test
    public void testSetAndOverrideLocalCompactionStrategy()
    {
        assertFalse(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);

        String jsonOptions = "{\"enabled\":true,\"compaction_params_json\":\"{\\\"class\\\":\\\"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy\\\"}\",\"keyspace_options\":{\"ks1\":\"{\\\"class\\\":\\\"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\\\"}\"},\"table_options\":{\"ks2.lcstbl\":\"{\\\"class\\\":\\\"org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy\\\"}\"}}";
        CompactionStrategyMigrationManager.instance.setAndOverrideLocalCompactionStrategy(jsonOptions);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().keyspace_options.containsKey(KS_1));
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().table_options.containsKey(KS_2 + '.' + LCS_TBL));
        // KS_1 should be overriden with LCS
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            if (cfs.name.equals(LCS_TBL))
            {
                assertEquals(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")), cfs.getCompactionStrategyManager().getCompactionParams());
            }
            else
            {
                assertEquals(CompactionParams.lcs(Collections.emptyMap()), cfs.getCompactionStrategyManager().getCompactionParams());
            }
        });

        // Only KS_2.LCS_TBL should be overriden
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            if (cfs.name.equals(LCS_TBL))
            {
                assertEquals(CompactionParams.twcs(Collections.emptyMap()), cfs.getCompactionStrategyManager().getCompactionParams());
            }
            else
            {
                assertEquals(originalCompactionParams.get(cfs),
                             cfs.getCompactionStrategyManager().getCompactionParams());
            }
        });
    }

    private void assertTableCompactionParamsInSystemSchemaKS(CompactionParams expectedParams, ColumnFamilyStore cfs)
    {
        String keyspaceName = cfs.keyspace.getName();
        String tableName = cfs.name;
        UntypedResultSet result = QueryProcessor.executeInternal(String.format(SYSTEM_SCHEMA_TBL_COMPACTION_QUERY_TEMPLATE, keyspaceName, tableName));
        assertNotNull(result);
        UntypedResultSet.Row row = result.one();
        // should be converted to default LCS
        assertEquals(expectedParams,
                     CompactionParams.fromMap(row.getFrozenTextMap("compaction")));
    }

    private void compactionStrategyUnchangedForAllSchemas()
    {
        for (Keyspace ks : Keyspace.all())
        {
            ks.getColumnFamilyStores().forEach(cfs -> {
                assertEquals(originalCompactionParams.get(cfs),
                             cfs.getCompactionStrategyManager().getCompactionParams());
            });
        }
    }

    private void compactionStrategyUnchangedForSystemSchemas()
    {
        for (Keyspace ks : Keyspace.system())
        {
            ks.getColumnFamilyStores().forEach(cfs -> {
                assertEquals(originalCompactionParams.get(cfs),
                             cfs.getCompactionStrategyManager().getCompactionParams());
            });
        }
    }
}
