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

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "setcompactionthroughput", description = "Set the MB/s throughput cap for compaction in the system, or 0 to disable throttling")
public class SetCompactionThroughput extends NodeToolCmd
{
    @Arguments(title = "compaction_throughput", usage = "<value_in_mb>", description = "Value in MB, 0 to disable throttling", required = true)
    private Integer compactionThroughput = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.setCompactionThroughput(compactionThroughput);
    }
}