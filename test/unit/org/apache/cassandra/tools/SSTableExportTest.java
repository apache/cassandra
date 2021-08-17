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
import org.junit.runner.RunWith;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class) // tests calling assertSchemaNotLoaded should be the first ones
public class SSTableExportTest extends OfflineToolUtils
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> jacksonListOfMapsType = new TypeReference<List<Map<String, Object>>>() {};
    private static String sstable;

    @BeforeClass
    public static void setupTest() throws IOException
    {
        sstable = findOneSSTable("legacy_sstables", "legacy_ma_simple");
    }

    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("You must supply exactly one sstable"));
        assertThat(tool.getCleanedStderr(), not(containsStringIgnoringCase("before the -k/-x options")));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv(false);
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class);
        String help = "usage: sstabledump <sstable file path> <options>\n" +
                       "                   \n" +
                       "Dump contents of given SSTable to standard output in JSON format.\n" +
                       " -d         CQL row per line internal representation\n" +
                       " -e         enumerate partition keys only\n" +
                       " -k <arg>   List of included partition keys\n" +
                       " -l         Output json lines, by partition\n" +
                       " -t         Print raw timestamps instead of iso8601 date strings\n" +
                       " -x <arg>   List of excluded partition keys\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
        assertPostTestEnv(false);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "--debugwrong", sstable);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv(false);
    }

    @Test
    public void testPKArgOutOfOrder()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "-k", "0", sstable);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("You must supply exactly one sstable"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("before the -k/-x options"));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv(false);
    }

    @Test
    public void testExcludePKArgOutOfOrder()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "-x", "0", sstable);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("You must supply exactly one sstable"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("before the -k/-x options"));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv(false);
    }

    @Test
    public void testDefaultCall() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable);
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
        assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv(true);
    }

    @Test
    public void testCQLRowArg()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-d");
        assertThat(tool.getStdout(), startsWith("[0]"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv(true);
    }

    @Test
    public void testPKOnlyArg()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-e");
        assertEquals(tool.getStdout(), "[ [ \"0\" ], [ \"1\" ], [ \"2\" ], [ \"3\" ], [ \"4\" ]\n]", tool.getStdout());
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv(true);
    }

    @Test
    public void testPKArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-k", "0");
        assertKeys(tool, "0");
        assertPostTestEnv(true);
    }

    @Test
    public void testMultiplePKArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-k", "0", "2");
        assertKeys(tool, "0", "2");
        assertPostTestEnv(true);
    }

    @Test
    public void testExcludePKArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-x", "0");
        assertKeys(tool, "1", "2", "3", "4");
        assertPostTestEnv(true);
    }

    @Test
    public void testMultipleExcludePKArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-x", "0", "2");
        assertKeys(tool, "1", "3", "4");
        assertPostTestEnv(true);
    }

    @SuppressWarnings("rawtypes")
    private void assertKeys(ToolResult tool, String... expectedKeys) throws IOException
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
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-t");
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertEquals(tool.getStdout(),
                     "1445008632854000",
                     ((Map) ((List<Map>) parsed.get(0).get("rows")).get(0).get("liveness_info")).get("tstamp"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv(true);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "DynamicRegexReplaceableByCompiledPattern" })
    public void testJSONLineArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable, "-l");
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
        assertPostTestEnv(true);
    }

    /**
     * Runs post-test assertions about loaded classed and started threads.
     *
     * @param maybeLoadsSchema {@code true} if the test may or may not have loaded the schema depending on the JVM,
     * {@code false} if the test shoudln't load the schema in any case. Note that a test not loading the schema can
     * still end with the schema loaded if a previous test already loaded it, so we should always run first the tests
     * that don't load the schema, and then the ones that may or may not load it. We also need to use the
     * {@link OrderedJUnit4ClassRunner} runner to guarantee the desired run order.
     */
    private void assertPostTestEnv(boolean maybeLoadsSchema)
    {
        assertNoUnexpectedThreadsStarted(null, OPTIONAL_THREADS_WITH_SCHEMA);
        // schema loading seems to depend on the JVM version,
        // so we only verify the cases where we are sure it's not loaded
        if (!maybeLoadsSchema)
            assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
}
