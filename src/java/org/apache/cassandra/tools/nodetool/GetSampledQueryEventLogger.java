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
import org.apache.cassandra.sqel.SampledQueryEventLogger;
import org.apache.cassandra.sqel.SampledQueryEventLoggerOptions;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "getsqel", description = "Print configuration of sampled query event logger if enabled, otherwise the configuration reflected in cassandra.yaml")
public class GetSampledQueryEventLogger extends NodeToolCmd
{
    protected void execute(NodeProbe probe)
    {
        final TableBuilder tableBuilder = new TableBuilder();

        tableBuilder.add("enabled", Boolean.toString(probe.getStorageService().isSampledQueryEventLoggerEnabled()));
        
        final SampledQueryEventLoggerOptions options = probe.getSampledQueryEventLoggerOptions();

        tableBuilder.add("query_success_sample_rate", Double.toString(options.query_success_sample_rate));
        tableBuilder.add("query_failure_sample_rate", Double.toString(options.query_failure_sample_rate));
        tableBuilder.add("batch_success_sample_rate", Double.toString(options.batch_success_sample_rate));
        tableBuilder.add("batch_failure_sample_rate", Double.toString(options.batch_failure_sample_rate));
        tableBuilder.add("execute_success_sample_rate", Double.toString(options.execute_success_sample_rate));
        tableBuilder.add("execute_failure_sample_rate", Double.toString(options.execute_failure_sample_rate));
        tableBuilder.add("prepare_success_sample_rate", Double.toString(options.prepare_success_sample_rate));
        tableBuilder.add("prepare_failure_sample_rate", Double.toString(options.prepare_failure_sample_rate));
        tableBuilder.add("auth_success_sample_rate", Double.toString(options.auth_success_sample_rate));
        tableBuilder.add("auth_failure_sample_rate", Double.toString(options.auth_failure_sample_rate));

        tableBuilder.printTo(probe.output().out);
    }
}