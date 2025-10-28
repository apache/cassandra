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

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for LeveledManifest
 */
public class LeveledManifestTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        MockSchema.cleanup();
    }

    @Test
    public void testGetEstimatedTasksWithSingleSSTableInL0()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest manifest = new LeveledManifest(cfs, 100, 10, new SizeTieredCompactionStrategyOptions());
        
        // Add a single sstable to L0
        List<SSTableReader> sstables = new ArrayList<>();
        sstables.add(MockSchema.sstableWithLevel(1, 1000, 1, cfs));
        manifest.addSSTables(sstables);
        
        // With only 1 sstable in L0 and no other levels, estimated tasks should be 0
        int estimatedTasks = manifest.getEstimatedTasks();
        assertEquals("Should have 0 estimated tasks with single sstable in L0", 0, estimatedTasks);
    }

    /**
     * Test getEstimatedTasks() with multiple sstables in L0 and no other levels.
     */
    @Test
    public void testGetEstimatedTasksWithMultipleSSTablesInL0()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        int maxSSTableSizeInMB = 100;
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSizeInMB, 10, new SizeTieredCompactionStrategyOptions());
        
        // Add multiple sstables to L0
        List<SSTableReader> sstables = new ArrayList<>();
        // Create 3 sstables
        sstables.add(MockSchema.sstableWithLevel(1, 100*1024*12024,  0, cfs));
        sstables.add(MockSchema.sstableWithLevel(2, 100*1024*12024,  0, cfs));
        sstables.add(MockSchema.sstableWithLevel(3, 100*1024*12024, 0, cfs));
        manifest.addSSTables(sstables);
        
        // With multiple sstables in L0
        int estimatedTasks = manifest.getEstimatedTasks();
        // The actual number depends on size calculations, but it should be >= 0
        assert estimatedTasks > 0 : "Estimated tasks should be positive";
    }

    /**
     * Test getEstimatedTasks() with sstables in multiple levels.
     */
    @Test
    public void testGetEstimatedTasksWithMultipleLevels()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        int maxSSTableSizeInMB = 100;
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSizeInMB, 10, new SizeTieredCompactionStrategyOptions());
        
        // Add sstables to both L0 and L1
        List<SSTableReader> sstables = new ArrayList<>();
        sstables.add(MockSchema.sstableWithLevel(1, 100*1024*12024,  0, cfs));
        sstables.add(MockSchema.sstableWithLevel(2, 300*1024*12024,  1, cfs));
        manifest.addSSTables(sstables);
        
        // With sstables in multiple levels
        int estimatedTasks = manifest.getEstimatedTasks();
        assert estimatedTasks > 0 : "Estimated tasks should be positive";
    }

    /**
     * Test getEstimatedTasks() with a single sstable in a higher level (not L0).
     * This test verifies that even with only one sstable, if levelCount > 1, the estimation proceeds.
     */
    @Test
    public void testGetEstimatedTasksWithSingleSSTableInHigherLevel()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        int maxSSTableSizeInMB = 100;
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSizeInMB, 10, new SizeTieredCompactionStrategyOptions());
        
        // Add a single sstable to L1 (not L0)
        List<SSTableReader> sstables = new ArrayList<>();
        sstables.add(MockSchema.sstableWithLevel(1, 100*1024*12024, 1, cfs));
        manifest.addSSTables(sstables);
        
        // Even with only 1 sstable total, since levelCount > 1 (we have L0 and L1),
        int estimatedTasks = manifest.getEstimatedTasks();
        assert estimatedTasks > 0 : "Estimated tasks should be positive";
    }

    /**
     * Test getEstimatedTasks() with no sstables at all.
     * This is an edge case to ensure the method handles empty manifests gracefully.
     */
    @Test
    public void testGetEstimatedTasksWithNoSSTables()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest manifest = new LeveledManifest(cfs, 100, 10, new SizeTieredCompactionStrategyOptions());
        
        // Don't add any sstables
        int estimatedTasks = manifest.getEstimatedTasks();
        assertEquals("Should have 0 estimated tasks with no sstables", 0, estimatedTasks);
    }
}

