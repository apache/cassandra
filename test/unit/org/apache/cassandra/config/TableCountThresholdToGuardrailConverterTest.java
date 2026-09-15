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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.schema.SchemaConstants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Converters#TABLE_COUNT_THRESHOLD_TO_GUARDRAIL} and
 * {@link Converters#KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL} (CASSANDRA-21156).
 *
 * This test runs in a virgin JVM without {@link DatabaseDescriptor#daemonInitialization()}
 * to guarantee that the converter can be called during pre-boot YAML parsing without
 * triggering cyclic static initialization dependencies.
 */
public class TableCountThresholdToGuardrailConverterTest
{
    @BeforeClass
    public static void loadConvertersBeforeSchemaConstants()
    {
        assertNull("This test requires a fresh JVM", DatabaseDescriptor.getRawConfig());
        // The conversion must initialize the system-name sets through the converters, not the test.
        Converters.TABLE_COUNT_THRESHOLD_TO_GUARDRAIL.convert(1);
        Converters.KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL.convert(1);
        assertNull(DatabaseDescriptor.getRawConfig());
    }

    @Test
    public void testTableCountThresholdConversionWithoutDatabaseDescriptorInit()
    {
        int systemTableCount = SchemaConstants.getLocalAndReplicatedSystemTableNames().size();
        assertTrue("Expected non-zero system tables", systemTableCount > 0);

        assertThresholdBoundaries(Converters.TABLE_COUNT_THRESHOLD_TO_GUARDRAIL, systemTableCount);
    }

    @Test
    public void testKeyspaceCountThresholdConversion()
    {
        int systemKeyspaceCount = SchemaConstants.getLocalAndReplicatedSystemKeyspaceNames().size();
        assertTrue("Expected non-zero system keyspaces", systemKeyspaceCount > 0);

        assertThresholdBoundaries(Converters.KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL, systemKeyspaceCount);
    }

    private static void assertThresholdBoundaries(Converters converter, int systemCount)
    {
        assertEquals(0, converter.convert(systemCount));
        assertEquals(1, converter.convert(systemCount + 1));
        assertEquals(systemCount + 1, converter.unconvert(1));

        int legacyThreshold = systemCount + 100;
        Object guardrailThreshold = converter.convert(legacyThreshold);
        assertEquals(100, guardrailThreshold);
        assertEquals(legacyThreshold, converter.unconvert(guardrailThreshold));
        assertNull(converter.unconvert(null));
    }
}
