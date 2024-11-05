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

import io.airlift.airline.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "checktokenmetadata", description = "compares the Gossip endpointState and TokenMetadata cache; printing any mismatches found")
public class CheckTokenMetadata extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        /** Cassandra maintains the token information in two places: 1) Gossip endpointState, and 2) TokenMetadata.
         * The probabilistic view of the cluster is the Gossip endpointState.
         * This then updates the TokenMetadata, a resulting topology view that's usable by hotpaths (e.g. read and write requests).
         * Bugs can result in these falling out of sync.
         * This command compares the Gossip endpointState and TokenMetadata cache, printing any mismatches found.
         */
        StringBuilder sb = new StringBuilder();
        Map<String, List<String>> mismatches = probe.compareGossipAndTokenMetadata();

        for (Map.Entry<String,List<String>> e : mismatches.entrySet())
            sb.append("Mismatch on : ").append(e.getKey())
              .append("\n  Gossip tokens: ").append(e.getValue().get(0))
              .append("\n  TokenMetadata: ").append(e.getValue().get(1)).append('\n');

        System.out.println(sb);
    }
}