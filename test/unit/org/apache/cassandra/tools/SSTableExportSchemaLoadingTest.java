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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.cassandra.utils.JsonUtils;
import org.assertj.core.api.Assertions;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Moved to separate class from SSTableExportTest to avoid schema getting loaded before
 * running a test where we don't expect the schema to be loaded
 */
public class SSTableExportSchemaLoadingTest extends OfflineToolUtils
{
    private static final ObjectMapper mapper = JsonUtils.JSON_OBJECT_MAPPER;
    private static final TypeReference<List<Map<String, Object>>> jacksonListOfMapsType = new TypeReference<List<Map<String, Object>>>() {};
    private static String sstable;

    @BeforeClass
    public static void setupTest() throws IOException
    {
        sstable = findOneSSTable("legacy_sstables", "legacy_ma_simple");
    }

    @Test
    public void testDefaultCall() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable);
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
        assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testCQLRowArg()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-d");
        assertThat(tool.getStdout(), startsWith("[0]"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testPKOnlyArg()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-e");
        assertEquals(tool.getStdout(), "[ [ \"0\" ], [ \"1\" ], [ \"2\" ], [ \"3\" ], [ \"4\" ]\n]", tool.getStdout());
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testPKArg() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-k", "0");
        assertKeys(tool, "0");
        assertPostTestEnv();
    }

    @Test
    public void testMultiplePKArg() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-k", "0", "2");
        assertKeys(tool, "0", "2");
        assertPostTestEnv();
    }

    @Test
    public void testExcludePKArg() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-x", "0");
        assertKeys(tool, "1", "2", "3", "4");
        assertPostTestEnv();
    }

    @Test
    public void testMultipleExcludePKArg() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-x", "0", "2");
        assertKeys(tool, "1", "3", "4");
        assertPostTestEnv();
    }

    @SuppressWarnings("rawtypes")
    private void assertKeys(ToolRunner.ToolResult tool, String... expectedKeys) throws IOException
    {
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        String[] actualKeys = parsed.stream()
                                    .map(x -> (Map) x.get("partition"))
                                    .map(x -> (List) x.get("key"))
                                    .map(x -> (String) x.get(0))
                                    .toArray(String[]::new);
        assertArrayEquals(expectedKeys, actualKeys);
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testTSFormatArg() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-t");
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertEquals(tool.getStdout(),
                     "1445008632854000",
                     ((Map) ((List<Map>) parsed.get(0).get("rows")).get(0).get("liveness_info")).get("tstamp"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "DynamicRegexReplaceableByCompiledPattern" })
    public void testJSONLineArg() throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-l");
        try
        {
            mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
            fail("Shouldn't be able to deserialize that output, now it's not a collection anymore.");
        }
        catch (MismatchedInputException e)
        {
            assertThat(e.getMessage(), startsWith("Cannot deserialize"));
        }

        int parsedCount = 0;
        for (String jsonLine : tool.getStdout().split("\\R"))
        {
            Map line = mapper.readValue(jsonLine, Map.class);
            assertTrue(jsonLine, line.containsKey("partition"));
            parsedCount++;
        }

        assertEquals(tool.getStdout(), 5, parsedCount);
        assertThat(tool.getStdout(), startsWith("{\""));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    /**
     * Runs post-test assertions about loaded classed and started threads.
     *
     */
    private void assertPostTestEnv()
    {
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, true);
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
}
