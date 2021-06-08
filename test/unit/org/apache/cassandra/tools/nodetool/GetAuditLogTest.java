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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class GetAuditLogTest extends CQLTester
{
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
    }

    @After
    public void afterTest()
    {
        disableAuditLog();
    }

    @Test
    public void getAuditLogTest()
    {
        testDefaultOutput(getAuditLog());

        enableAuditLog();
        testChangedOutput(getAuditLog());

        disableAuditLog();
        testDefaultOutput(getAuditLog());
    }

    private String getAuditLog()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getauditlog");
        tool.assertOnCleanExit();
        return tool.getStdout();
    }

    private void disableAuditLog()
    {
        ToolRunner.invokeNodetool("disableauditlog").assertOnCleanExit();
    }

    private void enableAuditLog()
    {
        ToolRunner.invokeNodetool("enableauditlog").assertOnCleanExit();
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testChangedOutput(final String getAuditLogOutput)
    {
        final String output = getAuditLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).contains("enabled true");
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testDefaultOutput(final String getAuditLogOutput)
    {
        final AuditLogOptions options = new AuditLogOptions();
        final String output = getAuditLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).contains("enabled false");
    }
}
