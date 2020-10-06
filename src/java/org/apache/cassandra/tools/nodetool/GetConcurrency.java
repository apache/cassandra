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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getconcurrency", description = "Get maximum concurrency for processing stages")
public class GetConcurrency extends NodeToolCmd
{
    @Arguments(title = "[stage-names]",
    usage = "[stage-names]",
    description = "optional list of stage names, otherwise display all stages")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        probe.output().out.printf("%-25s%16s%16s%n", "Stage", "CorePoolSize", "MaximumPoolSize");
        probe.getMaximumPoolSizes(args).entrySet().stream()
             .sorted(Map.Entry.comparingByKey())
             .forEach(entry ->
                probe.output().out.printf("%-25s%16d%16d%n",
                                   entry.getKey(),
                                   entry.getValue().get(0),
                                   entry.getValue().get(1)));

    }
}
