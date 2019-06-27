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
package org.apache.cassandra.db;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;

import static junit.framework.Assert.assertTrue;

public class LegacyCellNameTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testColumnSameNameAsPartitionKeyCompactStorage() throws Exception
    {
        CFMetaData cfm = CFMetaData.compile("CREATE TABLE cs (" +
                                            "k int PRIMARY KEY, v int)" +
                                            " WITH COMPACT STORAGE", "ks");

        LegacyLayout.LegacyCellName cellName 
            = LegacyLayout.decodeCellName(cfm, 
                                          LegacyLayout.makeLegacyComparator(cfm)
                                                      .fromString("k"));

        assertTrue(cellName.column.isRegular());
    }

    @Test
    public void testColumnSameNameAsClusteringKeyCompactStorage() throws Exception
    {
        CFMetaData cfm = CFMetaData.compile("CREATE TABLE cs (" +
                                            "k int PRIMARY KEY, v int)" +
                                            " WITH COMPACT STORAGE", "ks");

        LegacyLayout.LegacyCellName cellName 
            = LegacyLayout.decodeCellName(cfm, 
                                          LegacyLayout.makeLegacyComparator(cfm)
                                                      .fromString("column1"));

        assertTrue(cellName.column.isRegular());
    }

    @Test
    public void testColumnSameNameAsPartitionKeyCql3() throws Exception
    {
        CFMetaData cfm = CFMetaData.compile("CREATE TABLE cs (" +
                                            "k int PRIMARY KEY, v int)", "ks");

        LegacyLayout.LegacyCellName cellName 
            = LegacyLayout.decodeCellName(cfm, 
                                          LegacyLayout.makeLegacyComparator(cfm)
                                                      .fromString("k"));

        // When being grouped into Rows by LegacyLayout.CellGrouper,
        // primary key columns are filtered out
        assertTrue(cellName.column.isPrimaryKeyColumn());
    }

    @Test
    public void testCompositeWithColumnNameSameAsClusteringKeyCql3() throws Exception
    {
        CFMetaData cfm = CFMetaData.compile("CREATE TABLE cs (" +
                                            "k int, c text, v int, PRIMARY KEY(k, c))", "ks");

        LegacyLayout.LegacyCellName cellName
            = LegacyLayout.decodeCellName(cfm,
                                          LegacyLayout.makeLegacyComparator(cfm)
                                                      .fromString("c_value:c"));

        // When being grouped into Rows by LegacyLayout.CellGrouper,
        // primary key columns are filtered out
        assertTrue(cellName.column.isPrimaryKeyColumn());
    }

    // This throws IllegalArgumentException not because the cellname's value matches
    // the clustering key name, but because when converted to a Composite, the buffer
    // contains only a single component and so has no column name component
    @Test(expected=IllegalArgumentException.class)
    public void testColumnSameNameAsClusteringKeyCql3() throws Exception
    {
        CFMetaData cfm = CFMetaData.compile("CREATE TABLE cs (" +
                                            "k int, c text, v int, PRIMARY KEY(k, c))", "ks");

        LegacyLayout.LegacyCellName cellName 
            = LegacyLayout.decodeCellName(cfm, 
                                          LegacyLayout.makeLegacyComparator(cfm)
                                                      .fromString("c"));
    }
}
