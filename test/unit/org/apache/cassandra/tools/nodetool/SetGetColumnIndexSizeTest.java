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

package org.apache.cassandra.tools.nodetool;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.tools.ToolRunner.ToolResult;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@code nodetool setcolumnindexsize} and {@code nodetool getcolumnindexsize}.
 */
public class SetGetColumnIndexSizeTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testNull()
    {
        assertSetInvalidColumnIndexSize(null, "Required parameters are missing: column_index_size", 1);
    }

    @Test
    public void testPositive()
    {
        assertSetGetValidColumnIndexSize(7);
    }

    @Test
    public void testMaxValue()
    {
        assertSetGetValidColumnIndexSize(2097151);
    }

    @Test
    public void testZero()
    {
        assertSetGetValidColumnIndexSize(0);
    }

    @Test
    public void testNegative()
    {
        assertSetInvalidColumnIndexSize("-7", "Invalid data storage: value must be non-negative", 1);
    }

    @Test
    public void testInvalidValue()
    {
        assertSetInvalidColumnIndexSize("2097152", "column_index_size must be positive value <= 2147483646B, but was 2147483648B", 2);
    }

    @Test
    public void testUnparseable()
    {
        assertSetInvalidColumnIndexSize("1.2", "column_index_size: can not convert \"1.2\" to a int", 1);
        assertSetInvalidColumnIndexSize("value", "column_index_size: can not convert \"value\" to a int", 1);
    }

    private static void assertSetGetValidColumnIndexSize(int columnIndexSizeInKB)
    {
        ToolResult tool = invokeNodetool("setcolumnindexsize", String.valueOf(columnIndexSizeInKB));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughput(columnIndexSizeInKB);

        assertThat(StorageService.instance.getColumnIndexSizeInKiB()).isEqualTo(columnIndexSizeInKB);
    }

    private static void assertSetInvalidColumnIndexSize(String columnIndexSizeInKB, String expectedErrorMessage, int expectedErrorCode)
    {
        ToolResult tool = columnIndexSizeInKB == null ? invokeNodetool("setcolumnindexsize")
                                             : invokeNodetool("setcolumnindexsize", columnIndexSizeInKB);
        assertThat(tool.getExitCode()).isEqualTo(expectedErrorCode);
        assertThat(expectedErrorCode == 1 ? tool.getStdout() : tool.getStderr()).contains(expectedErrorMessage);
    }

    private static void assertGetThroughput(int expected)
    {
        ToolResult tool = invokeNodetool("getcolumnindexsize");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Current value for column_index_size: " + expected + " KiB");
    }
}
