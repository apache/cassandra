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

package org.apache.cassandra.tools.nodetool.mock;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class GetSSTablesMockTest extends AbstractNodetoolMock
{
    @Test
    public void testGetSSTables()
    {
        ToolRunner.ToolResult result = invokeNodetool("getsstables");
        assertEquals(1, result.getExitCode());
        assertTrue(result.getStdout().contains("nodetool: getsstables requires ks, cf and key args"));
    }

    @Test
    public void testGetSSTablesWithFullAgruments()
    {
        ColumnFamilyStoreMBean mock = addAndGetMockColumnFamilyStore("ks", "cf", false);
        when(mock.isLeveledCompaction()).thenReturn(true);
        when(mock.getSSTablesForKeyWithLevel("key", false)).thenReturn(Collections.singletonMap(1, new HashSet<>(List.of("sstable1", "sstable2"))));
        ToolRunner.ToolResult result = invokeNodetool("getsstables", "--show-levels", "ks", "cf", "key");
        result.assertOnCleanExit();
        Mockito.verify(mock).isLeveledCompaction();
    }
}
