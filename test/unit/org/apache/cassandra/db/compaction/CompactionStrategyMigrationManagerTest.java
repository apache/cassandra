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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CompactionStrategyMigrationOptions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactionStrategyMigrationManagerTest
{
    private static final String KS_1 = "ks1";
    private static final String KS_2 = "ks2";
    private static final String STCS_TBL = "stcsTbl";
    private static final String LCS_TBL = "lcsTbl";
    private static final String TWCS_TBL = "twcsTbl";
    private static final String DEFAULT_COMPACTION_PARAMS_JSON = "{\"class\": \"LeveledCompactionStrategy\"}";
    private static Map<ColumnFamilyStore, CompactionParams> originalCompactionParams = new HashMap<>();

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KS_1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_1, STCS_TBL)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KS_1, LCS_TBL)
                                                .compaction(CompactionParams.lcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KS_1, TWCS_TBL)
                                                .compaction(CompactionParams.twcs(Collections.emptyMap())));
        SchemaLoader.createKeyspace(KS_2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_2, STCS_TBL)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KS_2, LCS_TBL)
                                                .compaction(CompactionParams.lcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KS_2, TWCS_TBL)
                                                .compaction(CompactionParams.twcs(Collections.emptyMap())));
        for (Keyspace ks : Keyspace.all())
        {
            ks.getColumnFamilyStores().forEach(cfs -> {
                originalCompactionParams.put(cfs, cfs.getCompactionStrategyManager().getCompactionParams());
            });
        }
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void reset()
    {
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(new CompactionStrategyMigrationOptions(false));
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            cfs.getCompactionStrategyManager().setNewLocalCompactionStrategy(originalCompactionParams.get(cfs));
            cfs.reload();
        });
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            cfs.getCompactionStrategyManager().setNewLocalCompactionStrategy(originalCompactionParams.get(cfs));
            cfs.reload();
        });
        CompactionStrategyMigrationManager.instance.reset();
        compactionStrategyUnchangedForAllSchemas();
    }

    @Test
    public void featureDisabled()
    {
        assertFalse(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);
        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();
        // nothing should be changed
        compactionStrategyUnchangedForAllSchemas();
    }

    @Test
    public void featureEnabledForAllUserSchemas()
    {
        CompactionStrategyMigrationOptions options = new CompactionStrategyMigrationOptions(true);
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        assertTrue(DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled);

        CompactionParams expectedParams = CompactionParams.lcs(Collections.emptyMap());
        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // user schemas should be changed to LCS
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(expectedParams, cfs.getCompactionStrategyManager().getCompactionParams());
        });
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(expectedParams, cfs.getCompactionStrategyManager().getCompactionParams());
        });
        // system schemas should be unchanged
        compactionStrategyUnchangedForSystemSchemas();

        assertEquals(6, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
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
        CompactionParams expectedParams = CompactionParams.lcs(Collections.emptyMap());

        CompactionStrategyMigrationManager.instance.mayOverrideLocalCompactionStrategy();

        // KS_1 should be overriden
        Keyspace.open(KS_1).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(expectedParams, cfs.getCompactionStrategyManager().getCompactionParams());
        });

        // KS_2 should be unchanged
        Keyspace.open(KS_2).getColumnFamilyStores().forEach(cfs -> {
            assertEquals(originalCompactionParams.get(cfs),
                         cfs.getCompactionStrategyManager().getCompactionParams());
        });

        // system schemas should be unchanged
        compactionStrategyUnchangedForSystemSchemas();

        assertEquals(3, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
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

        assertEquals(0, CompactionStrategyMigrationManager.instance.getNumCfsToMigrate());
        assertEquals(0, CompactionStrategyMigrationManager.instance.getPendingTasksForMigration());
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
