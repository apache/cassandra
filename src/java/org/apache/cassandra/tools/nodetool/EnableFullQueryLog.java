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
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "enablefullquerylog", description = "Enable full query logging")
public class EnableFullQueryLog extends NodeToolCmd
{
    @Option(title = "roll_cycle", name = {"--roll-cycle"}, description = "How often to roll the log file (MINUTELY, HOURLY, DAILY). Default HOURLY.")
    private String rollCycle = "HOURLY";

    @Option(title = "blocking", name = {"--blocking"}, description = "If the queue is full whether to block producers or drop samples. Default true.")
    private boolean blocking = true;

    @Option(title = "max_queue_weight", name = {"--max-queue-weight"}, description = "Maximum number of bytes of query data to queue to disk before blocking or dropping samples. Default 256 megabytes.")
    private int maxQueueWeight = 256 * 1024 * 1024;

    @Option(title = "max_log_size", name = {"--max-log-size"}, description = "How many bytes of log data to store before dropping segments. Might not be respected if a log file hasn't rolled so it can be deleted. Default 16 gigabytes.")
    private long maxLogSize = 16L * 1024L * 1024L * 1024L;

    @Option(title = "path", name = {"--path"}, description = "Path to store the full query log at. Will have it's contents recursively deleted. If not set the value from cassandra.yaml will be used.")
    private String path = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.getSpProxy().configureFullQueryLogger(path, rollCycle, blocking, maxQueueWeight, maxLogSize);
    }
}