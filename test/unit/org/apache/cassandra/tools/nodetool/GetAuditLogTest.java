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
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetAuditLogTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @After
    public void afterTest()
    {
        disableAuditLog();
    }

    @Test
    public void getDefaultOutputTest()
    {
        testDefaultOutput(getAuditLog());
    }

    @Test
    public void getSimpleOutputTest()
    {
        enableAuditLogSimple();
        testChangedOutputSimple(getAuditLog());
    }

    @Test
    public void getComplexOutputTest()
    {
        enableAuditLogComplex();
        testChangedOutputComplex(getAuditLog());
    }

    @Test
    public void disablingAuditLogResetsOutputTest()
    {
        enableAuditLogComplex();
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

    private void enableAuditLogSimple()
    {
        ToolRunner.invokeNodetool("enableauditlog").assertOnCleanExit();
    }

    private void enableAuditLogComplex()
    {
        ToolRunner.invokeNodetool("enableauditlog",
                                  "--included-keyspaces", "ks1,ks2,ks3",
                                  "--excluded-categories", "ddl,dcl").assertOnCleanExit();
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testChangedOutputSimple(final String getAuditLogOutput)
    {
        final String output = getAuditLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).startsWith("enabled true");
        assertThat(output).contains("logger BinAuditLogger");
        assertThat(output).contains("roll_cycle HOURLY");
        assertThat(output).contains("block true");
        assertThat(output).contains("max_log_size 17179869184");
        assertThat(output).contains("max_queue_weight 268435456");
        assertThat(output).contains("max_archive_retries 10");
        assertThat(output).contains("included_keyspaces \n");
        assertThat(output).contains("excluded_keyspaces system,system_schema,system_virtual_schema");
        assertThat(output).contains("included_categories \n");
        assertThat(output).contains("excluded_categories \n");
        assertThat(output).contains("included_users \n");
        assertThat(output).endsWith("excluded_users");
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testChangedOutputComplex(final String getAuditLogOutput)
    {
        final String output = getAuditLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).startsWith("enabled true");
        assertThat(output).contains("logger BinAuditLogger");
        assertThat(output).contains("roll_cycle HOURLY");
        assertThat(output).contains("block true");
        assertThat(output).contains("max_log_size 17179869184");
        assertThat(output).contains("max_queue_weight 268435456");
        assertThat(output).contains("max_archive_retries 10");
        assertThat(output).contains("included_keyspaces ks1,ks2,ks3");
        assertThat(output).contains("excluded_keyspaces system,system_schema,system_virtual_schema");
        assertThat(output).contains("included_categories \n");
        assertThat(output).contains("excluded_categories DDL,DCL");
        assertThat(output).contains("included_users \n");
        assertThat(output).endsWith("excluded_users");
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testDefaultOutput(final String getAuditLogOutput)
    {
        final String output = getAuditLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).startsWith("enabled false");
        assertThat(output).contains("logger BinAuditLogger");
        assertThat(output).contains("roll_cycle HOURLY");
        assertThat(output).contains("block true");
        assertThat(output).contains("max_log_size 17179869184");
        assertThat(output).contains("max_queue_weight 268435456");
        assertThat(output).contains("max_archive_retries 10");
        assertThat(output).contains("included_keyspaces \n");
        assertThat(output).contains("excluded_keyspaces system,system_schema,system_virtual_schema");
        assertThat(output).contains("included_categories \n");
        assertThat(output).contains("excluded_categories \n");
        assertThat(output).contains("included_users \n");
        assertThat(output).endsWith("excluded_users");
    }
}
