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

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class GetCompactionThresholdMockTest extends AbstractNodetoolMock
{
    @Test
    public void testGetCompactionThreshold()
    {
        ToolRunner.ToolResult result = invokeNodetool("getcompactionthreshold");
        assertEquals(1, result.getExitCode());
        assertTrue(result.getStdout().contains("Missing required parameters: '<keyspace>', '<table>'"));
    }

    @Test
    public void testGetCompactionThresholdWithKeyspace()
    {
        ColumnFamilyStoreMBean mock = addAndGetMockColumnFamilyStore("ks", "cf", false);
        when(mock.getMinimumCompactionThreshold()).thenReturn(4);
        when(mock.getMaximumCompactionThreshold()).thenReturn(32);
        ToolRunner.ToolResult result = invokeNodetool("getcompactionthreshold", "ks", "cf");
        result.assertOnCleanExit();
        assertTrue(result.getStdout().contains("Current compaction thresholds for ks/cf: \n"));
        assertTrue(result.getStdout().contains(" min = 4,  max = 32"));
    }
}
