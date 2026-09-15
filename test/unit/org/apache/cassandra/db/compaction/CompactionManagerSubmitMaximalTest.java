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

import java.util.List;

import org.apache.cassandra.utils.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for CompactionManager.submitMaximal() method.
 *
 * CASSANDRA-21128: Tests that submitMaximal handles null return from getMaximalTasks
 * without throwing NullPointerException.
 */
public class CompactionManagerSubmitMaximalTest
{
    private static final String KEYSPACE = "CompactionManagerSubmitMaximalTest";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    /**
     * CASSANDRA-21128: Tests that submitMaximal() returns an empty list when
     * getMaximalTasks() returns null (which can happen when runWithCompactionsDisabled
     * fails due to uninterruptible compactions during node restart).
     *
     * Before the fix, this would throw a NullPointerException when calling tasks.isEmpty().
     */
    @Test
    public void testSubmitMaximalHandlesNullTasks()
    {
        // Create a mock ColumnFamilyStore with a mock CompactionStrategyManager
        // that returns null from getMaximalTasks (simulating the restart scenario)
        ColumnFamilyStore mockCfs = mock(ColumnFamilyStore.class);
        CompactionStrategyManager mockCsm = mock(CompactionStrategyManager.class);

        when(mockCfs.getCompactionStrategyManager()).thenReturn(mockCsm);
        when(mockCsm.getMaximalTasks(anyLong(), anyBoolean(), anyInt(), any(OperationType.class)))
            .thenReturn(null);

        // Call submitMaximal - before the fix, this would throw NPE
        List<Future<?>> futures = CompactionManager.instance.submitMaximal(
            mockCfs,
            FBUtilities.nowInSeconds(),
            false,
            1,
            OperationType.MAJOR_COMPACTION
        );

        // Verify it returns an empty list instead of throwing NPE
        assertNotNull("submitMaximal should not return null", futures);
        assertTrue("submitMaximal should return empty list when tasks is null", futures.isEmpty());
    }

    /**
     * Tests that submitMaximal() works correctly with empty CompactionTasks.
     */
    @Test
    public void testSubmitMaximalHandlesEmptyTasks()
    {
        // Use a real ColumnFamilyStore with no SSTables to compact
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);

        // With no SSTables, getMaximalTasks should return empty tasks
        List<Future<?>> futures = CompactionManager.instance.submitMaximal(
            cfs,
            FBUtilities.nowInSeconds(),
            false,
            1,
            OperationType.MAJOR_COMPACTION
        );

        assertNotNull("submitMaximal should not return null", futures);
        assertEquals("submitMaximal should return empty list when no SSTables to compact",
                     0, futures.size());
    }
}
