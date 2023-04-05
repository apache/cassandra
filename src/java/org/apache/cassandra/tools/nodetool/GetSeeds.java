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

import io.airlift.airline.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getseeds", description = "Get the currently in use seed node IP list excluding the node IP")
public class GetSeeds extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        List<String> seedList = probe.getSeeds();
        if (seedList.isEmpty())
        {
            probe.output().out.println("Seed node list does not contain any remote node IPs");
        }
        else
        {
            probe.output().out.println("Current list of seed node IPs, excluding the current node's IP: " + String.join(" ", seedList));
        }

    }
}
