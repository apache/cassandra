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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.PrintStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setautorepairconfig", description = "sets the autorepair configuration")
public class SetAutoRepairConfig extends NodeToolCmd
{
    @VisibleForTesting
    @Arguments(title = "<autorepairparam> <value>", usage = "<autorepairparam> <value>",
    description = "autorepair param and value.\nPossible autorepair parameters are as following: " +
                  "[threads|subranges|minrepairintervalinhours|sstablehigherthreshold" +
                  "|enabled|tablemaxrepairtimeinsec|priorityhost|forcerepairhosts|ignoredcs" +
                  "|historydeletehostsclearbufferinsec|primarytokenrangeonly|parallelrepaircount|parallelrepairpercentage|mvrepairenabled]",
    required = true)
    protected List<String> args = new ArrayList<>();

    @VisibleForTesting
    @Option(title = "repair type", name = { "-t", "--repair-type" }, description = "Repair type")
    protected RepairType repairType;

    @VisibleForTesting
    protected PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(repairType != null, "--repair-type is required.");
        checkArgument(args.size() == 2, "setautorepairconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (!probe.getAutoRepairConfig().isAutoRepairSchedulingEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        if (paramType.equals("historydeletehostsclearbufferinsec"))
        {
            probe.setAutoRepairHistoryClearDeleteHostsBufferInSecV2(Integer.parseInt(paramVal));
            return;
        }

        // options below require --repair-type option
        checkArgument(repairType != null, "--repair-type is required for this parameter.");
        Set<InetAddressAndPort> hosts;
        switch (paramType)
        {
            case "enabled":
                probe.setAutoRepairEnabled(repairType, Boolean.parseBoolean(paramVal));
                break;
            case "threads":
                probe.setRepairThreads(repairType, Integer.parseInt(paramVal));
                break;
            case "subranges":
                probe.setRepairSubRangeNum(repairType, Integer.parseInt(paramVal));
                break;
            case "minrepairintervalinhours":
                probe.setRepairMinIntervalInHours(repairType, Integer.parseInt(paramVal));
                break;
            case "sstablehigherthreshold":
                probe.setRepairSSTableCountHigherThreshold(repairType, Integer.parseInt(paramVal));
                break;
            case "tablemaxrepairtimeinsec":
                probe.setAutoRepairTableMaxRepairTimeInSec(repairType, Long.parseLong(paramVal));
                break;
            case "priorityhost":
                hosts = validateLocalGroupHosts(probe, repairType, paramVal);
                if (!hosts.isEmpty())
                {
                    probe.setRepairPriorityForHosts(repairType, hosts);
                }
                break;
            case "forcerepairhosts":
                hosts = validateLocalGroupHosts(probe, repairType, paramVal);
                if (!hosts.isEmpty())
                {
                    probe.setForceRepairForHosts(repairType, hosts);
                }
                break;
            case "ignoredcs":
                Set<String> ignoreDCs = new HashSet<>();
                for (String dc : Splitter.on(',').split(paramVal))
                {
                    ignoreDCs.add(dc);
                }
                probe.setAutoRepairIgnoreDCs(repairType, ignoreDCs);
                break;
            case "primarytokenrangeonly":
                probe.setPrimaryTokenRangeOnly(repairType, Boolean.parseBoolean(paramVal));
                break;
            case "parallelrepaircount":
                probe.setParallelRepairCountInGroup(repairType, Integer.parseInt(paramVal));
                break;
            case "parallelrepairpercentage":
                probe.setParallelRepairPercentageInGroup(repairType, Integer.parseInt(paramVal));
                break;
            case "mvrepairenabled":
                probe.setMVRepairEnabled(repairType, Boolean.parseBoolean(paramVal));
                break;
            default:
                throw new IllegalArgumentException("Unknown parameter: " + paramType);
        }
    }

    private Set<InetAddressAndPort> validateLocalGroupHosts(NodeProbe probe, RepairType repairType, String paramVal) {
        Set<InetAddressAndPort> hosts = new HashSet<>();
        for (String host : Splitter.on(',').split(paramVal))
        {
            try
            {
                hosts.add(InetAddressAndPort.getByName(host));
            }
            catch (UnknownHostException e)
            {
                out.println("invalid ip address: " + host);
            }
        }

        return probe.filterHostsInLocalGroup(repairType, hosts);
    }
}
