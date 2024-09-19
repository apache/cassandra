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

import java.util.List;
import java.util.Map;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

public abstract class AccordAdmin extends NodeTool.NodeToolCmd
{
    @Command(name = "describe", description = "Describe current cluster metadata relating to Accord")
    public static class Describe extends NodeTool.NodeToolCmd
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            Map<String, String> info = probe.getAccordOperationsProxy().describe();
            output.out.printf("Accord Service:%n");
            output.out.printf("Epoch: %s%n", info.get("EPOCH"));
            output.out.printf("Stale Replicas: %s%n", info.get("STALE_REPLICAS"));
        }
    }

    @Command(name = "mark_stale", description = "Mark a replica as being stale and no longer able to participate in durability status coordination")
    public static class MarkStale extends AccordAdmin
    {
        @Arguments(required = true, description = "One or more node IDs to mark stale", usage = "<nodeId>+")
        public List<String> nodeIds;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getAccordOperationsProxy().accordMarkStale(nodeIds);
        }
    }

    @Command(name = "mark_rejoining", description = "Mark a stale replica as being allowed to participate in durability status coordination again")
    public static class MarkRejoining extends AccordAdmin
    {
        @Arguments(required = true, description = "One or more node IDs to mark no longer stale", usage = "<nodeId>+")
        public List<String> nodeIds;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getAccordOperationsProxy().accordMarkRejoining(nodeIds);
        }
    }
}
