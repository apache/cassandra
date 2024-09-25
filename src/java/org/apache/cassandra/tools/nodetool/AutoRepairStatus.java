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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "autorepairstatus", description = "Print autorepair status")
public class AutoRepairStatus extends NodeTool.NodeToolCmd
{
    @VisibleForTesting
    @Option(title = "v2", name = { "--v2" }, description = "Use v2 repair framework")
    protected boolean v2 = false;

    @VisibleForTesting
    @Option(title = "repair type", name = { "-t", "--repair-type" }, description = "Repair type")
    protected RepairType repairType;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(v2 || repairType == null, "--repair-type is only supported with the -v2 option.");
        checkArgument(!v2 || repairType != null, "--repair-type is required when using -v2 option.");

        AutoRepairConfig config = probe.getAutoRepairConfig();

        PrintStream out = probe.output().out;
        TableBuilder table = new TableBuilder();

        table.add("Data center group", "Active repairs", "Acitve force repairs");
        Set<String> allHosts = new HashSet<>();
        Set<String> allForceHosts = new HashSet<>();
        Set<Set<String>> dcGroups = v2 ? getDCGroups(config, repairType) : probe.getDCGroups();
        if (dcGroups == null || dcGroups.isEmpty())
        {
            dcGroups = new HashSet<>();
            dcGroups.add(new HashSet<>());
        }
        for (Set<String> group : dcGroups)
        {
            Set<String> ongoingRepairHostIds = v2 ? probe.getOnGoingRepairHostIdsByGroupHash(repairType, group.hashCode())
                                                  : probe.getOnGoingRepairHostIdsByGroupHash(group.hashCode());
            Set<String> ongoingForceRepairHostIds = v2 ? probe.getOnGoingForceRepairHostIdsByGroupHash(repairType, group.hashCode())
                                                       : probe.getOnGoingForceRepairHostIdsByGroupHash(group.hashCode());
            String groupName = group.isEmpty() ? "ALL NODES" : group.toString();
            table.add(groupName, getSetString(ongoingRepairHostIds), getSetString(ongoingForceRepairHostIds));
            allHosts.addAll(ongoingRepairHostIds);
            allForceHosts.addAll(ongoingForceRepairHostIds);
        }
        table.add("Total", getSetString(allHosts), getSetString(allForceHosts));
        table.printTo(out);
    }

    private String getSetString(Set<String> hostIds)
    {
        if (hostIds.isEmpty())
        {
            return "EMPTY";
        }
        StringBuilder sb = new StringBuilder();
        for (String id : hostIds)
        {
            sb.append(id);
            sb.append(",");
        }
        // remove last ","
        sb.setLength(Math.max(sb.length() - 1, 0));
        return sb.toString();
    }

    private Set<Set<String>> getDCGroups(AutoRepairConfig config, RepairType repairType)
    {
        return config.getDCGroups(repairType).stream().map(
        (String group) -> Arrays.stream(group.split(",")).collect(Collectors.toSet())).collect(Collectors.toSet());
    }
}
