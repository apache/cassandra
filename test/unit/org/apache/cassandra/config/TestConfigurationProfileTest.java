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

import org.junit.Test;

import org.apache.cassandra.TestConfigurations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

/** Exercises configuration selection before Cassandra's first initialization in each fork. */
@TestConfigurations({"default", "big-cursor", "bti", "latest"})
public class TestConfigurationProfileTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void configurationIsAppliedBeforeInitialization()
    {
        String profile = CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG.getString("").split("_jdk", 2)[0];
        // Other legacy configuration targets have their own assertions.
        assumeTrue("This probe checks the declared configurations",
                   java.util.Arrays.asList("default", "big-cursor", "bti", "latest").contains(profile));
        assertEquals(("default".equals(profile) || "big-cursor".equals(profile)) ? "big" : "bti", DatabaseDescriptor.getSelectedSSTableFormat().name());
        Config config = DatabaseDescriptor.getRawConfig();
        assertEquals("big-cursor".equals(profile) || "latest".equals(profile), config.cursor_compaction_enabled);
        assertEquals("latest".equals(profile) ? Config.MemtableAllocationType.offheap_objects
                                              : Config.MemtableAllocationType.heap_buffers,
                     config.memtable_allocation_type);
        if ("latest".equals(profile))
        {
            assertEquals("trie", config.memtable.configurations.get("default").inherits);
            assertNotNull(config.default_compaction);
            assertEquals("UnifiedCompactionStrategy", config.default_compaction.class_name);
        }
    }
}
