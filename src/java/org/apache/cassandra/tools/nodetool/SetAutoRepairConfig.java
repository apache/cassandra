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

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

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
    @Arguments(title = "<autorepairparam> <value>", usage = "<autorepairparam> <value>",
            description = "autorepair param and value.\nPossible autorepair parameters are as following: " +
                    "[threads|subranges|minrepairfreqinhours|sstablehigherthreshold|ignorekeyspacesregex|repairOnlykeyspacesregex|tablemaxrepairtimeinsec|priorityhost]",
            required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setautorepairconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (!probe.isAutoRepairEnabled())
        {
            System.out.println("AutoRepair is not enabled");
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
            Set<InetAddress> hosts = new HashSet<>();
            for (String host : Splitter.on(',').split(paramVal))
            {
                try
                {
                    hosts.add(InetAddress.getByName(host));
                }
                catch (UnknownHostException e)
                {
                    System.out.println("invalid ip address: " + host);
                    continue;
                }
            }
            if (hosts.size() > 0)
            {
                probe.setRepairPriorityForHosts(hosts);
            }

        }
    }
}
