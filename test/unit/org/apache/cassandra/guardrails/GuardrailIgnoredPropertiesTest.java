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

package org.apache.cassandra.guardrails;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GuardrailIgnoredPropertiesTest extends GuardrailTester
{
    private static Set<String> defaultTablePropertiesIgnored;

    @Before
    public void before()
    {
        defaultTablePropertiesIgnored = DatabaseDescriptor.getGuardrailsConfig().table_properties_ignored;

        // but actually ignore "comment"
        DatabaseDescriptor.getGuardrailsConfig().table_properties_ignored = ImmutableSet.of("comment");
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().table_properties_ignored = defaultTablePropertiesIgnored;
    }

    @Test
    public void testPropertySkipping()
    {
        TableAttributes tableAttributes = new TableAttributes();
        tableAttributes.addProperty("bloom_filter_fp_chance", "0.01");
        tableAttributes.addProperty("comment", "");
        tableAttributes.addProperty("crc_check_chance", "1.0");
        tableAttributes.addProperty("default_time_to_live", "0");
        tableAttributes.addProperty("gc_grace_seconds", "864000");
        tableAttributes.addProperty("max_index_interval", "2048");
        tableAttributes.addProperty("memtable_flush_period_in_ms", "0");
        tableAttributes.addProperty("min_index_interval", "128");
        tableAttributes.addProperty("read_repair","BLOCKING'");
        tableAttributes.addProperty("speculative_retry", "99p");

        assertTrue(tableAttributes.hasProperty("comment"));
        assertEquals(10, tableAttributes.updatedProperties().size());

        // Should not throw ConcurrentModificationException (CNDB-7724)
        Guardrails.ignoredTableProperties.maybeIgnoreAndWarn(tableAttributes.updatedProperties(), tableAttributes::removeProperty, null);

        assertFalse(tableAttributes.hasProperty("comment"));
        assertEquals(9, tableAttributes.updatedProperties().size());
    }
}
