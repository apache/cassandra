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
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.config.AccordConfig.RangeIndexMode.journal_sai;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Shared fixture for lifecycle tests kept in separate classes for Ant's perTest JVM isolation. */
final class AccordJournalInitializationTestSupport
{
    private static final int LEGACY_TABLE_THRESHOLD = 400;
    private static final int LEGACY_KEYSPACE_THRESHOLD = 20;

    private AccordJournalInitializationTestSupport()
    {
    }

    static void assertFreshJvm()
    {
        assertNull("This lifecycle test requires a fresh JVM", DatabaseDescriptor.getRawConfig());
    }

    @SuppressWarnings("unchecked")
    static Config loadConfig()
    {
        // Parse legacy fields inside initialization, before DatabaseDescriptor can publish its Config.
        try (InputStream input = AccordJournalInitializationTestSupport.class.getResourceAsStream("/cassandra.yaml"))
        {
            assertNotNull("Missing test cassandra.yaml", input);
            Yaml yaml = new Yaml();
            Map<String, Object> config = yaml.load(input);
            Map<String, Object> accord = (Map<String, Object>) config.get("accord");
            accord.put("range_index_mode", "journal_sai");
            config.put("table_count_warn_threshold", LEGACY_TABLE_THRESHOLD);
            config.put("keyspace_count_warn_threshold", LEGACY_KEYSPACE_THRESHOLD);
            return YamlConfigurationLoader.loadConfig(yaml.dump(config).getBytes(UTF_8));
        }
        catch (IOException e)
        {
            throw new AssertionError("Unable to read test cassandra.yaml", e);
        }
    }

    static void assertConfiguredJournalIndex()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        assertNotNull(config);
        assertTrue(config.accord.enabled);
        assertEquals(journal_sai, config.accord.range_index_mode);
        assertEquals(LEGACY_TABLE_THRESHOLD - SchemaConstants.getLocalAndReplicatedSystemTableNames().size(), config.tables_warn_threshold);
        assertEquals(LEGACY_KEYSPACE_THRESHOLD - SchemaConstants.getLocalAndReplicatedSystemKeyspaceNames().size(), config.keyspaces_warn_threshold);

        TableMetadata journal = AccordKeyspace.metadata().tables.getNullable(AccordKeyspace.JOURNAL);
        assertNotNull(journal);
        assertTrue("Configured journal_sai must retain its record index",
                   journal.indexes.get(AccordKeyspace.JOURNAL_INDEX_NAME).isPresent());
    }
}
