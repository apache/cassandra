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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getnaturalendpointsforalltokenranges", description = "Print, for each token range, the endpoints that owns it")
public class GetNaturalEndpointsForAllTokenRanges extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<dc> <keyspace>", usage = "<dc> <keyspace>", description = "Datacenter and keyspace", required = true)
    final private List<String> args = new ArrayList<>();

    @Option(title = "status_of_nodes", name = {"-s", "--show-node-status"}, description = "Show status of nodes")
    private boolean statusOfNodes = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        String dc = args.get(0);
        String keyspace = args.get(1);
        String res = "";
        try {
            res = probe.getNaturalEndpointsForAllTokenRanges(dc, keyspace, statusOfNodes);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred during getNaturalEndpointsForAllTokenRanges", e);
        }

        System.out.println(res);
    }
}
