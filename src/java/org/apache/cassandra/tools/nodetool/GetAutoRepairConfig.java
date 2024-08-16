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
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getautorepairconfig", description = "Print autorepair configurations")
public class GetAutoRepairConfig extends NodeToolCmd
{
    @Option(title = "v2", name = { "--v2" }, description = "Use v2 auto-repair framework")
    protected boolean v2 = false;

    @VisibleForTesting
    protected static PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        if (!v2)
        {
            printLegacyConfig(probe);
            return;
        }

        AutoRepairConfig config = probe.getAutoRepairConfig();
        if (config == null || !config.isAutoRepairSchedulingEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("repair scheduler configuration:");
        sb.append("\n\trepair eligibility check interval: " + config.getRepairCheckIntervalInSec() + " seconds");
        sb.append("\n\tTTL for repair history for dead nodes: " + config.getAutoRepairHistoryClearDeleteHostsBufferInSec() + " seconds");
        for (RepairType repairType : RepairType.values())
        {
            sb.append(formatRepairTypeConfig(probe, repairType, config));
        }

        out.println(sb.toString());
    }

    // TODO: deprecate legacy config
    private void printLegacyConfig(NodeProbe probe)
    {
        if (probe.isAutoRepairEnabled())
        {
            StringBuilder sb = new StringBuilder();
            sb.append("repair threads: " + probe.getRepairThreads());
            sb.append("\nnumber of repair subranges: " + probe.getRepairSubRangeNum());
            sb.append("\nignore keyspaces: " + probe.getRepairIgnoreKeyspaces());
            sb.append("\nrepair only keyspaces: " + probe.getRepairOnlyKeyspaces());
            sb.append("\npriority hosts: " + Joiner.on(',').skipNulls().join(probe.getRepairPriorityForHosts()));
            sb.append("\nminimum repair frequency in hours: " + probe.getRepairMinFrequencyInHours());
            sb.append("\nsstable count higher threshold: " + probe.getRepairSSTableCountHigherThreshold());
            sb.append("\ntable max repair time in sec: " + probe
                                                           .getAutoRepairTableMaxRepairTimeInSec());
            sb.append("\nignore datacenters: " + Joiner.on(',').skipNulls().join(probe.getAutoRepairIgnoreDCs()));
            sb.append("\ndatacenter groups: ");
            for (Set<String> dcGroup : probe.getDCGroups())
            {
                sb.append('\n' + Joiner.on(',').skipNulls().join(dcGroup));
            }
            sb.append("\nauto repair history table delete hosts clear buffer in seconds: " + probe.getAutoRepairHistoryClearDeleteHostsBufferInSec());
            sb.append("\nrepair primary token-range: " + probe.getPrimaryTokenRangeOnly());
            sb.append("\nnumber of parallel repairs within group: " + probe.getParallelRepairCountInGroup());
            sb.append("\npercentage of parallel repairs within group: " + probe.getParallelRepairPercentageInGroup());
            sb.append("\nmv repair enabled: " + probe.getMVRepairEnabled());

            out.println(sb.toString());
        }
        else
        {
            out.println("AutoRepair is not enabled");
        }
    }

    private String formatRepairTypeConfig(NodeProbe probe, RepairType repairType, AutoRepairConfig config)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\nconfiguration for repair type: " + repairType);
        sb.append("\n\tenabled: " + config.isAutoRepairEnabled(repairType));
        sb.append("\n\tminimum repair interval in hours: " + config.getRepairMinIntervalInHours(repairType));
        sb.append("\n\trepair threads: " + config.getRepairThreads(repairType));
        sb.append("\n\tnumber of repair subranges: " + config.getRepairSubRangeNum(repairType));
        sb.append("\n\tignore keyspaces: " + config.getRepairIgnoreKeyspaces(repairType));
        sb.append("\n\trepair only keyspaces: " + config.getRepairOnlyKeyspaces(repairType));
        sb.append("\n\tpriority hosts: " + Joiner.on(',').skipNulls().join(probe.getRepairPriorityForHosts(repairType)));
        sb.append("\n\tsstable count higher threshold: " + config.getRepairSSTableCountHigherThreshold(repairType));
        sb.append("\n\ttable max repair time in sec: " + config.getAutoRepairTableMaxRepairTimeInSec(repairType));
        sb.append("\n\tignore datacenters: " + Joiner.on(',').skipNulls().join(config.getIgnoreDCs(repairType)));
        sb.append("\n\tdatacenter groups:");
        for (String dcGroup : config.getDCGroups(repairType))
        {
            sb.append("\n\t\t" + dcGroup);
        }
        sb.append("\n\trepair primary token-range: " + config.getRepairPrimaryTokenRangeOnly(repairType));
        sb.append("\n\tnumber of parallel repairs within group: " + config.getParallelRepairCountInGroup(repairType));
        sb.append("\n\tpercentage of parallel repairs within group: " + config.getParallelRepairPercentageInGroup(repairType));
        sb.append("\n\tmv repair enabled: " + config.getMVRepairEnabled(repairType));
        sb.append("\n\tinitial scheduler delay in seconds: " + config.getInitialSchedulerDelayInSec(repairType));

        return sb.toString();
    }
}
