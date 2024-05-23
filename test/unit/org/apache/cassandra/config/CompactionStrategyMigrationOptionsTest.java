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

package org.apache.cassandra.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import org.apache.cassandra.schema.CompactionParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompactionStrategyMigrationOptionsTest
{
    @Test
    public void testCleanConfig() throws IOException
    {
        List<String> config = ImmutableList.of(
        "compaction_strategy_migration_options:",
        "    enabled: true",
        "    compaction_params_json: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy\\\"}\"",
        "    keyspace_options:",
        "        ks1: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\\\"}\"",
        "        ks2: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy\\\"}\"",
        "    table_options:",
        "        ks2.tb1: \"{\\\"class\\\": \\\"org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy\\\"}\"");

        Config loadedConfig = testYaml(false, config);
        assertNotNull(loadedConfig);
        CompactionStrategyMigrationOptions options = loadedConfig.compaction_strategy_migration_options;

        assertTrue(options.enabled);
        assertEquals("{\"class\": \"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy\"}",
                     options.compaction_params_json);
        assertTrue(options.keyspace_options.containsKey("ks1"));
        assertTrue(options.keyspace_options.containsKey("ks2"));
        assertTrue(options.table_options.containsKey("ks2.tb1"));
        assertEquals("{\"class\": \"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\"}",
                     options.keyspace_options.get("ks1"));
        assertEquals("{\"class\": \"org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy\"}",
                     options.keyspace_options.get("ks2"));
        assertEquals("{\"class\": \"org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy\"}",
                     options.table_options.get("ks2.tb1"));
    }

    @Test
    public void testDeafultConfig() throws IOException
    {
        List<String> config = ImmutableList.of(
        "cluster_name: test"
        );
        Config loadedConfig = testYaml(false, config);
        assertNotNull(loadedConfig);
        CompactionStrategyMigrationOptions options = loadedConfig.compaction_strategy_migration_options;
        // should be disabled by default
        assertFalse(options.enabled);
        // default to LCS
        assertEquals("{\"class\": \"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\"}",
                     options.compaction_params_json);
    }

    public void testIsJsonValid()
    {
        assertTrue(CompactionStrategyMigrationOptions.isJsonValid("{\"class\": \"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy\"}"));
        assertTrue(CompactionStrategyMigrationOptions.isJsonValid("{\"class\": \"SizeTieredCompactionStrategy\"}"));
        assertFalse(CompactionStrategyMigrationOptions.isJsonValid("{\"badformat\"}"));
        assertFalse(CompactionStrategyMigrationOptions.isJsonValid("{\"class\": \"SizeTieredCompactionStrategy\", \"random\": \"32\"}"));
        // need to specify class
        assertFalse(CompactionStrategyMigrationOptions.isJsonValid("{\"max_threshold\": \"32\"}"));
    }

    @Test
    public void testParseCompactionParamsJson()
    {
        CompactionParams params = CompactionStrategyMigrationOptions.parseCompactionParamsJson("{\"class\": \"SizeTieredCompactionStrategy\", \"min_threshold\": \"3\", \"max_threshold\": \"64\"}");
        assertEquals("SizeTieredCompactionStrategy", params.klass().getSimpleName());
        assertEquals(3, params.minCompactionThreshold());
        assertEquals(64, params.maxCompactionThreshold());
    }

    private static Config testYaml(boolean expectFailure, List<String> config) throws IOException
    {
        Path p = Files.createTempFile("test_config", ".yaml");
        Config loadedConfig;
        try
        {
            List<String> lines = new ArrayList<>(config);
            Files.write(p, lines);
            loadedConfig = new YamlConfigurationLoader().loadConfig(p.toUri().toURL());
        }
        catch (Exception e)
        {
            assertTrue(expectFailure);
            e.printStackTrace(System.out);
            return null;
        }
        finally
        {
            Files.delete(p);
        }
        return loadedConfig;
    }
}
