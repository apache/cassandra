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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.sqel.SampledQueryEventLoggerOptions;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetSampledQueryEventLoggerTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
    }

    @After
    public void afterTest() throws InterruptedException
    {
        disableSampledQueryEventLogger();
    }

    private String getSampledQueryEventLogger()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getsqel");
        tool.assertOnCleanExit();
        return tool.getStdout();
    }

    private void disableSampledQueryEventLogger()
    {
        ToolRunner.invokeNodetool("disablesqel");
    }

    private void enableSampledQueryEventLogger()
    {
        ToolRunner.invokeNodetool("enablesqel",
                                    "--query-success-rate","1.0",
                                    "--query-failure-rate","1.0",
                                    "--batch-success-rate","1.0",
                                    "--batch-failure-rate","1.0",
                                    "--execute-success-rate","1.0",
                                    "--execute-failure-rate","1.0",
                                    "--prepare-success-rate","1.0",
                                    "--prepare-failure-rate","1.0",
                                    "--auth-success-rate","1.0",
                                    "--auth-failure-rate","1.0")
                .assertOnCleanExit();
    }

    private void fullyDisableSampledQueryEventLogger() 
    {
        ToolRunner.invokeNodetool("enablesqel",
                                    "--query-success-rate","0.0",
                                    "--query-failure-rate","0.0",
                                    "--batch-success-rate","0.0",
                                    "--batch-failure-rate","0.0",
                                    "--execute-success-rate","0.0",
                                    "--execute-failure-rate","0.0",
                                    "--prepare-success-rate","0.0",
                                    "--prepare-failure-rate","0.0",
                                    "--auth-success-rate","0.0",
                                    "--auth-failure-rate","0.0")
                .assertOnCleanExit();
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testOutput(final String getSampledQueryEventLoggerOutput, final SampledQueryEventLoggerOptions options)
    {
        // final SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder().withEnabled(true).build();
        final String output = getSampledQueryEventLoggerOutput.replaceAll("( )+", " ").trim();
        assertThat(output).contains("enabled " + Boolean.toString(options.enabled));
        assertThat(output).contains("query_success_sample_rate " + options.query_success_sample_rate);
        assertThat(output).contains("query_failure_sample_rate " + options.query_failure_sample_rate);
        assertThat(output).contains("batch_success_sample_rate " + options.batch_success_sample_rate);
        assertThat(output).contains("batch_failure_sample_rate " + options.batch_failure_sample_rate);
        assertThat(output).contains("execute_success_sample_rate " + options.execute_success_sample_rate);
        assertThat(output).contains("execute_failure_sample_rate " + options.execute_failure_sample_rate);
        assertThat(output).contains("prepare_success_sample_rate " + options.prepare_success_sample_rate);
        assertThat(output).contains("prepare_failure_sample_rate " + options.prepare_failure_sample_rate);
        assertThat(output).contains("auth_success_sample_rate " + options.auth_success_sample_rate);
        assertThat(output).contains("auth_failure_sample_rate " + options.auth_failure_sample_rate);
    }

    @Test
    public void getSampledQueryEventLoggerTest()
    {
        testOutput(getSampledQueryEventLogger(), new SampledQueryEventLoggerOptions());
    }

    @Test
    public void enableSampledQueryEventLoggerTest()
    {
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withEnabled(true)
            .withQuerySuccessSampleRate(1.0)
            .withQueryFailureSampleRate(1.0)
            .withBatchSuccessSampleRate(1.0)
            .withBatchFailureSampleRate(1.0)
            .withExecuteSuccessSampleRate(1.0)
            .withExecuteFailureSampleRate(1.0)
            .withPrepareSuccessSampleRate(1.0)
            .withPrepareFailureSampleRate(1.0)
            .withAuthSuccessSampleRate(1.0)
            .withAuthFailureSampleRate(1.0)
            .build();
        enableSampledQueryEventLogger();
        testOutput(getSampledQueryEventLogger(), options);
    }

    @Test
    public void toggleSampledQueryEventLoggerTest()
    {
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withEnabled(true)
            .withQuerySuccessSampleRate(1.0)
            .withQueryFailureSampleRate(1.0)
            .withBatchSuccessSampleRate(1.0)
            .withBatchFailureSampleRate(1.0)
            .withExecuteSuccessSampleRate(1.0)
            .withExecuteFailureSampleRate(1.0)
            .withPrepareSuccessSampleRate(1.0)
            .withPrepareFailureSampleRate(1.0)
            .withAuthSuccessSampleRate(1.0)
            .withAuthFailureSampleRate(1.0)
            .build();
        enableSampledQueryEventLogger();
        testOutput(getSampledQueryEventLogger(), options);
        disableSampledQueryEventLogger();
        fullyDisableSampledQueryEventLogger();
        testOutput(getSampledQueryEventLogger(), new SampledQueryEventLoggerOptions());
        enableSampledQueryEventLogger();
        testOutput(getSampledQueryEventLogger(), options);
    }
}