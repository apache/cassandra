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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "accord",
         description = "Manage the operation of Accord",
         subcommands = {
             AccordAdmin.Describe.class,
             AccordAdmin.MarkStale.class,
             AccordAdmin.MarkRejoining.class
         })
public class AccordAdmin extends AbstractCommand
{
    @Override
    protected void execute(NodeProbe probe)
    {
        AbstractCommand cmd = new AccordAdmin.Describe();
        cmd.probe(probe);
        cmd.logger(output);
        cmd.run();
    }

    @Command(name = "describe", description = "Describe current cluster metadata relating to Accord")
    public static class Describe extends AbstractCommand
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
    public static class MarkStale extends AbstractCommand
    {
        @Parameters(arity = "1..*", description = "One or more node IDs to mark stale")
        public List<String> nodeIds;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getAccordOperationsProxy().accordMarkStale(nodeIds);
        }
    }

    @Command(name = "mark_rejoining", description = "Mark a stale replica as being allowed to participate in durability status coordination again")
    public static class MarkRejoining extends AbstractCommand
    {
        @Parameters(arity = "1", description = "One or more node IDs to mark no longer stale")
        public List<String> nodeIds;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getAccordOperationsProxy().accordMarkRejoining(nodeIds);
        }
    }
}
