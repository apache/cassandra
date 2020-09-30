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

import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class SSTableExportTest extends OfflineToolUtils
{
    private ObjectMapper mapper = new ObjectMapper();
    private TypeReference<List<Map<String, Object>>> jacksonListOfMapsType = new TypeReference<List<Map<String, Object>>>() {};

    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("You must supply exactly one sstable"));
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, OPTIONAL_THREADS_WITH_SCHEMA);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
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
                       " -k <arg>   Partition key\n" + 
                       " -l         Output json lines, by partition\n" + 
                       " -t         Print raw timestamps instead of iso8601 date strings\n" + 
                       " -x <arg>   Excluded partition key\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "--debugwrong", findOneSSTable("legacy_sstables", "legacy_ma_simple"));
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testDefaultCall() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class,findOneSSTable("legacy_sstables", "legacy_ma_simple"));
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertTrue(tool.getStdout(), parsed.get(0).get("partition") != null);
        assertTrue(tool.getStdout(), parsed.get(0).get("rows") != null);
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testCQLRowArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, findOneSSTable("legacy_sstables", "legacy_ma_simple"), "-d");
        assertThat(tool.getStdout(), CoreMatchers.startsWith("[0]"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testPKOnlyArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, findOneSSTable("legacy_sstables", "legacy_ma_simple"), "-e");
        assertEquals(tool.getStdout(), "[ [ \"0\" ], [ \"1\" ], [ \"2\" ], [ \"3\" ], [ \"4\" ]\n]", tool.getStdout());
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testPKArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, findOneSSTable("legacy_sstables", "legacy_ma_simple"), "-k", "0");
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertEquals(tool.getStdout(), 1, parsed.size());
        assertEquals(tool.getStdout(), "0", ((List) ((Map) parsed.get(0).get("partition")).get("key")).get(0));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testExcludePKArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, findOneSSTable("legacy_sstables", "legacy_ma_simple"), "-x", "0");
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertEquals(tool.getStdout(), 4, parsed.size());
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testTSFormatArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, findOneSSTable("legacy_sstables", "legacy_ma_simple"), "-t");
        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertEquals(tool.getStdout(),
                     "1445008632854000",
                     ((Map) ((List<Map>) parsed.get(0).get("rows")).get(0).get("liveness_info")).get("tstamp"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    @Test
    public void testJSONLineArg() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, findOneSSTable("legacy_sstables", "legacy_ma_simple"), "-l");
        try
        {
            mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
            fail("Shouldn't be able to deserialize that output, now it's not a collection anymore.");
        }
        catch(MismatchedInputException e)
        {
        }

        int parsedCount = 0;
        for (String jsonLine : tool.getStdout().split("\\R"))
        {
            Map line = mapper.readValue(jsonLine, Map.class);
            assertTrue(jsonLine, line.containsKey("partition"));
            parsedCount++;
        }

        assertEquals(tool.getStdout(), 5, parsedCount);
        assertThat(tool.getStdout(), CoreMatchers.startsWith("{\""));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        tool.assertOnExitCode();
        assertPostTestEnv();
    }

    private void assertPostTestEnv()
    {
        assertNoUnexpectedThreadsStarted(null, OPTIONAL_THREADS_WITH_SCHEMA);
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertServerNotLoaded();
    }
}
