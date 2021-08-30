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
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "getfullquerylog", description = "Print configuration of fql if enabled, otherwise the configuration reflected in cassandra.yaml")
public class GetFullQueryLog extends NodeToolCmd
{
    protected void execute(NodeProbe probe)
    {
        final TableBuilder tableBuilder = new TableBuilder();

        tableBuilder.add("enabled", Boolean.toString(probe.getStorageService().isFullQueryLogEnabled()));

        final FullQueryLoggerOptions options = probe.getFullQueryLoggerOptions();

        tableBuilder.add("log_dir", options.log_dir);
        tableBuilder.add("archive_command", options.archive_command);
        tableBuilder.add("roll_cycle", options.roll_cycle);
        tableBuilder.add("block", Boolean.toString(options.block));
        tableBuilder.add("max_log_size", Long.toString(options.max_log_size));
        tableBuilder.add("max_queue_weight", Integer.toString(options.max_queue_weight));
        tableBuilder.add("max_archive_retries", Long.toString(options.max_archive_retries));

        tableBuilder.printTo(probe.output().out);
    }
}
