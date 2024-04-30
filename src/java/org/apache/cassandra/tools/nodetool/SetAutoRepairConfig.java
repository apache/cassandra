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
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.AutoRepairUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.PrintStream;
import java.net.InetAddress;
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
                  "[threads|subranges|minrepairfreqinhours|minrepairintervalinhours|sstablehigherthreshold|ignorekeyspacesregex" +
                  "|enabled|repaironlykeyspacesregex|tablemaxrepairtimeinsec|priorityhost|forcerepairhosts|ignoredcs|repaircheckintervalinsec" +
                  "|historydeletehostsclearbufferinsec|primarytokenrangeonly|parallelrepaircount|parallelrepairpercentage|mvrepairenabled]",
    required = true)
    protected List<String> args = new ArrayList<>();

    @VisibleForTesting
    @Option(title = "repair type", name = { "-t", "--repair-type" }, description = "Repair type (uses v2 framework)")
    protected RepairType repairType;

    @VisibleForTesting
    @Option(title = "v2", name = { "--v2" }, description = "Use v2 repair framework")
    protected boolean v2 = false;

    @VisibleForTesting
    protected PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(v2 || repairType == null, "--repair-type is only supported with the -v2 option.");

        if (!v2)
        {
            modifyLegacyConfig(probe);
            return;
        }

        checkArgument(args.size() == 2, "setautorepairconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (!probe.getAutoRepairConfig().isAutoRepairSchedulingEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        switch (paramType)
        {
            case "historydeletehostsclearbufferinsec":
                probe.setAutoRepairHistoryClearDeleteHostsBufferInSecV2(Integer.parseInt(paramVal));
                return;
            case "repaircheckintervalinsec":
                probe.setAutoRepairCheckInterval(Integer.parseInt(paramVal));
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
            case "ignorekeyspacesregex":
                probe.setRepairIgnoreKeyspaces(repairType, paramVal);
                break;
            case "repaironlykeyspacesregex":
                probe.setRepairOnlyKeyspaces(repairType, paramVal);
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

    // TODO: deprecate once incremental repair migration is done
    private void modifyLegacyConfig(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setautorepairconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        checkArgument(
        !paramType.equals("minrepairintervalinhours")
        && !paramType.equals("enabled")
        && !paramType.equals("repaircheckintervalinsec"),
        String.format("%s is only supported with the -v2 option.", paramType));

        if (!probe.isAutoRepairEnabled())
        {
            out.println("AutoRepair is not enabled");
            return;
        }

        if (paramType.equals("threads"))
        {
            probe.setRepairThreads(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("subranges"))
        {
            probe.setRepairSubRangeNum(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("minrepairfreqinhours"))
        {
            probe.setRepairMinFrequencyInHours(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("historydeletehostsclearbufferinsec"))
        {
            probe.setAutoRepairHistoryClearDeleteHostsBufferInSec(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("sstablehigherthreshold"))
        {
            probe.setRepairSSTableCountHigherThreshold(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("ignorekeyspacesregex"))
        {
            probe.setRepairIgnoreKeyspaces(paramVal);
        }
        else if (paramType.equals("repairOnlykeyspacesregex"))
        {
            probe.setRepairOnlyKeyspaces(paramVal);
        }
        else if (paramType.equals("tablemaxrepairtimeinsec"))
        {
            probe.setAutoRepairTableMaxRepairTimeInSec(Long.parseLong(paramVal));
        }
        else if (paramType.equals("priorityhost"))
        {
            Set<InetAddressAndPort> hostsInCurrentRing = validateLocalGroupHosts(paramVal);
            if (hostsInCurrentRing.size() > 0)
            {
                probe.setRepairPriorityForHosts(hostsInCurrentRing);
            }
        }
        else if (paramType.equals("forcerepairhosts"))
        {
            Set<InetAddressAndPort> hostsInCurrentRing = validateLocalGroupHosts(paramVal);
            if (hostsInCurrentRing.size() > 0)
            {
                probe.setForceRepairForHosts(hostsInCurrentRing);
            }
        }
        else if (paramType.equals("ignoredcs"))
        {
            Set<String> ignoreDCs = new HashSet<>();
            for (String dc : Splitter.on(',').split(paramVal))
            {
                ignoreDCs.add(dc);
            }
            probe.setAutoRepairIgnoreDCs(ignoreDCs);
        }
        else if (paramType.equals("primarytokenrangeonly"))
        {
            probe.setPrimaryTokenRangeOnly(Boolean.parseBoolean(paramVal));
        }
        else if (paramType.equals("parallelrepaircount"))
        {
            probe.setParallelRepairCountInGroup(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("parallelrepairpercentage"))
        {
            probe.setParallelRepairPercentageInGroup(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("mvrepairenabled"))
        {
            probe.setMVRepairEnabled(Boolean.parseBoolean(paramVal));
        }
    }

    // some commands require user to input a list of hosts that is in the local group, this function helps to filter out
    // any hosts that are not part of same local group with the node running this command
    private Set<InetAddressAndPort> validateLocalGroupHosts(String paramVal)
    {
        Set<InetAddressAndPort> hosts = new HashSet<>();
        for (String host : Splitter.on(',').split(paramVal))
        {
            try
            {
                hosts.add(InetAddressAndPort.getByName(host));
            }
            catch (UnknownHostException e)
            {
                System.out.println("invalid ip address: " + host);
                continue;
            }
        }
        // We can only process hosts in local group
        Set<InetAddressAndPort> hostsInCurrentRing = AutoRepairUtils.processNodesByGroup(hosts);
        if (hostsInCurrentRing.size() != hosts.size())
        {
            for (String host : Splitter.on(',').split(paramVal))
            {
                InetAddress address;
                try
                {
                    address = InetAddress.getByName(host);
                }
                catch (UnknownHostException e)
                {
                    continue;
                }
                if (!hostsInCurrentRing.contains(address))
                {
                    System.out.println(host + " doesn't belong to this group, please add this host on another node" +
                                       "which is located in the same DC.");
                }
            }
        }
        return hostsInCurrentRing;
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
