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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
@Command(name = "enablesampledqueryeventlogger", description = "Enables SampledQueryEventLogger")
public class EnableSampledQueryEventLogger extends NodeTool.NodeToolCmd
{

    @Option(title = "query_success_rate", name = {"--query-success-rate"}, description = "Sampling rate for successful queries.")
    private double querySuccessRate = 0.1;

    @Option(title = "query_failure_rate", name = {"--query-failure-rate"}, description = "Sampling rate for failed queries.")
    private double queryFailureRate = 0.1;

    @Option(title = "batch_success_rate", name = {"--batch-success-rate"}, description = "Sampling rate for successful batches.")
    private double batchSuccessRate = 0.1;

    @Option(title = "batch_failure_rate", name = {"--batch-failure-rate"}, description = "Sampling rate for failed batches.")
    private double batchFailureRate = 0.1;

    @Option(title = "execute_success_rate", name = {"--execute-success-rate"}, description = "Sampling rate for successful execute statements.")
    private double executeSuccessRate = 0.1;

    @Option(title = "execute_failure_rate", name = {"--execute-failure-rate"}, description = "Sampling rate for failed execute statements.")
    private double executeFailureRate = 0.1;

    @Option(title = "prepare_success_rate", name = {"--prepare-success-rate"}, description = "Sampling rate for successful prepare statements.")
    private double prepareSuccessRate = 0.1;

    @Option(title = "prepare_failure_rate", name = {"--prepare-failure-rate"}, description = "Sampling rate for failed prepare statements.")
    private double prepareFailureRate = 0.1;
    @Override
    public void execute(NodeProbe probe)
    {
        probe.getStorageService().enableSimpleQueryEventLogger(querySuccessRate, queryFailureRate, batchSuccessRate, batchFailureRate, executeSuccessRate, executeFailureRate, prepareSuccessRate, prepareFailureRate);
    }
}
