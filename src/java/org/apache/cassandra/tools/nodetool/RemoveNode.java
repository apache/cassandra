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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "removenode",
         description = "Show status of current node removal, abort removal or remove provided ID",
         subcommands = { RemoveNode.Status.class })
public class RemoveNode extends AbstractCommand
{
    @ParentCommand
    public NodetoolCommand parent;

    @Parameters(paramLabel = "nodeId", description = "The ID of the node to remove", arity = "0..1")
    private String nodeId;

    @Option(names = { "--force" }, description = "Force node removal")
    private boolean force = false;

    @Override
    public void execute(NodeProbe probe)
    {
        // In order the picocli to parse the subcommand correctly, we need to check the nodeId here, or use @ArgGroup
        checkArgument(nodeId != null, "nodeId is required");
        probe.removeNode(nodeId, force);
    }

    @Command(name = "abortremovenode", description = "Abort a removenode command")
    public static class Abort extends AbstractCommand
    {
        @Option(paramLabel = "nodeId", names = { "--node" }, description = "The node being removed")
        private String nodeId;

        public void execute(NodeProbe probe)
        {
            checkArgument(nodeId != null, "nodeId is required");
            probe.abortRemoveNode(nodeId);
        }
    }

    @Command(name = "status", description = "Show status of the current node removal operation")
    public static class Status extends AbstractCommand
    {
        @ParentCommand
        private RemoveNode parent;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.output().out.println("RemovalStatus: " + probe.getRemovalStatus(parent.parent.printPort));
        }
    }
}
