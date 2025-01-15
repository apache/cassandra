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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "removenode", description = "Show status of current node removal, abort removal or remove provided ID")
public class RemoveNode extends NodeToolCmd
{
    @Arguments(title = "remove_operation", usage = "<status>|<ID>|<ID> --force", description = "Show status of current node removal, or remove provided ID", required = true)
    private List<String> removeOperation = null;

    @Override
    public void execute(NodeProbe probe)
    {
        switch (removeOperation.get(0))
        {
            case "status":
                probe.output().out.println("RemovalStatus: " + probe.getRemovalStatus(printPort));
                break;
            case "force":
                throw new IllegalArgumentException("Can't force a nodetool removenode. Instead abort the ongoing removenode and retry.");
            default:
                boolean force = removeOperation.size() > 1 && removeOperation.get(1).equals("--force");
                probe.removeNode(removeOperation.get(0), force);
                break;
        }
    }

    @Command(name = "abortremovenode", description = "Abort a removenode command")
    public static class Abort extends NodeToolCmd
    {
        @Option(title = "node id", name="--node", description = "The node being removed", required = true)
        private String nodeId;

        public void execute(NodeProbe probe)
        {
            probe.abortRemoveNode(nodeId);
        }
    }
}
