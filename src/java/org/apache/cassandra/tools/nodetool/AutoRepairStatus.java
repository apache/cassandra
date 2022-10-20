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

import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.airlift.airline.Command;
import org.apache.cassandra.repair.AutoRepairUtils;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "autorepairstatus", description = "Print autorepair status")
public class AutoRepairStatus extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        TableBuilder table = new TableBuilder();

        table.add("data center group", "number of nodes doing repair", "host id(s)");
        int totalRepairingNodes = 0;
        Set<String> allhosts = new HashSet<>();
        Set<Set<String>> dcGroups = probe.getDCGroups();
        if (dcGroups == null || dcGroups.isEmpty()) {
            dcGroups = new HashSet<>();
            dcGroups.add(new HashSet<>());
        }
        for (Set<String> group : dcGroups)
        {
            Set<String> hostIds = probe.getOnGoingRepairHostIdsByGroupHash(group.hashCode());
            String groupName = group.isEmpty() ? "ALL NODES" : group.toString();
            table.add(groupName, String.valueOf(hostIds.size()), getSetString(hostIds));
            totalRepairingNodes += hostIds.size();
            allhosts.addAll(hostIds);
        }
        table.add("Total", String.valueOf(totalRepairingNodes), allhosts.isEmpty() ? "EMPTY" : getSetString(allhosts));
        table.printTo(out);
    }

    private String getSetString(Set<String> hostIds) {
        StringBuilder sb = new StringBuilder();
        for (String id :hostIds)
        {
            sb.append(id);
            sb.append(",");
        }
        // remove last ","
        sb.setLength(Math.max(sb.length() - 1, 0));
        return sb.toString();
    }
}
