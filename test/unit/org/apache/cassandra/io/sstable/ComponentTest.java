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

package org.apache.cassandra.io.sstable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ComponentTest
{
    @Test
    public void testFromRepresentationExactMatches()
    {
        // Test exact string matches for standard components
        assertEquals(Component.Type.DATA, Component.Type.fromRepresentation("Data.db"));
        assertEquals(Component.Type.PARTITION_INDEX, Component.Type.fromRepresentation("Partitions.db"));
        assertEquals(Component.Type.ROW_INDEX, Component.Type.fromRepresentation("Rows.db"));
        assertEquals(Component.Type.PRIMARY_INDEX, Component.Type.fromRepresentation("Index.db"));
        assertEquals(Component.Type.FILTER, Component.Type.fromRepresentation("Filter.db"));
        assertEquals(Component.Type.COMPRESSION_INFO, Component.Type.fromRepresentation("CompressionInfo.db"));
        assertEquals(Component.Type.STATS, Component.Type.fromRepresentation("Statistics.db"));
        assertEquals(Component.Type.DIGEST, Component.Type.fromRepresentation("Digest.crc32"));
        assertEquals(Component.Type.CRC, Component.Type.fromRepresentation("CRC.db"));
        assertEquals(Component.Type.SUMMARY, Component.Type.fromRepresentation("Summary.db"));
        assertEquals(Component.Type.TOC, Component.Type.fromRepresentation("TOC.txt"));
    }

    @Test
    public void testFromRepresentationSecondaryIndexPattern()
    {
        // Test regex pattern matching for SECONDARY_INDEX
        assertEquals(Component.Type.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_.db"));
        assertEquals(Component.Type.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_something.db"));
        assertEquals(Component.Type.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_idx_name.db"));
        assertEquals(Component.Type.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_a1b2c3.db"));
        assertEquals(Component.Type.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_test_index_123.db"));
    }

    @Test
    public void testFromRepresentationCustomComponent()
    {
        // Test that unknown components return CUSTOM
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("Unknown.db"));
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("CustomComponent.db"));
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("SomethingElse.txt"));
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("Random-File.dat"));
    }

    @Test
    public void testFromRepresentationEmptyString()
    {
        // Test that empty string returns CUSTOM
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromRepresentationNullInput()
    {
        // Test that null input throws IllegalArgumentException
        Component.Type.fromRepresentation(null);
    }

    @Test
    public void testFromRepresentationNonMatches()
    {
        // Test that similar but not exact matches return CUSTOM
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("data.db"));  // lowercase
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("Data.DB"));  // different case
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("DataExtra.db"));  // extra chars
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("SI.db"));  // missing underscore
        assertEquals(Component.Type.CUSTOM, Component.Type.fromRepresentation("SI_"));  // missing .db
    }

    @Test
    public void testComponentParse()
    {
        // Test the parse method which uses fromRepresentation internally
        Component data = Component.parse("Data.db");
        assertEquals(Component.Type.DATA, data.type);
        assertEquals(Component.DATA, data);

        Component secondaryIndex = Component.parse("SI_myindex.db");
        assertEquals(Component.Type.SECONDARY_INDEX, secondaryIndex.type);
        assertEquals("SI_myindex.db", secondaryIndex.name);

        Component custom = Component.parse("CustomFile.db");
        assertEquals(Component.Type.CUSTOM, custom.type);
        assertEquals("CustomFile.db", custom.name);
    }

    @Test
    public void testComponentEquality()
    {
        Component comp1 = new Component(Component.Type.DATA);
        Component comp2 = new Component(Component.Type.DATA);

        assertEquals(comp1, comp2);
        assertEquals(comp1.hashCode(), comp2.hashCode());

        // Test singletons
        assertSame(Component.DATA, Component.parse("Data.db"));
        assertSame(Component.FILTER, Component.parse("Filter.db"));
    }

    @Test
    public void testComponentName()
    {
        assertEquals("Data.db", Component.DATA.name());
        assertEquals("Filter.db", Component.FILTER.name());
        assertEquals("Statistics.db", Component.STATS.name());
    }

    @Test
    public void testSecondaryIndexNotSingleton()
    {
        // Secondary index components should not be singletons
        Component si1 = Component.parse("SI_index1.db");
        Component si2 = Component.parse("SI_index2.db");

        assertEquals(Component.Type.SECONDARY_INDEX, si1.type);
        assertEquals(Component.Type.SECONDARY_INDEX, si2.type);
        assertNotEquals(si1, si2);  // Different names
        assertNotSame(si1, si2);  // Different instances
    }

    @Test
    public void testFromRepresentationConsistency()
    {
        String[] inputs = {"Data.db", "SI_test.db", "CustomFile.db"};

        for (String input : inputs) {
            Component.Type type1 = Component.Type.fromRepresentation(input);
            Component.Type type2 = Component.Type.fromRepresentation(input);
            assertSame("Same input should return same enum instance", type1, type2);
        }
    }
}