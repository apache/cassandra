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
import com.google.common.base.Joiner;
import io.airlift.airline.Command;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.PrintStream;

@Command(name = "getautorepairconfig", description = "Print autorepair configurations")
public class GetAutoRepairConfig extends NodeToolCmd
{
    @VisibleForTesting
    protected static PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        AutoRepairConfig config = probe.getAutoRepairConfig();
        if (config == null || !config.isAutoRepairSchedulingEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("repair scheduler configuration:");
        sb.append("\n\trepair eligibility check interval: " + config.getRepairCheckInterval());
        sb.append("\n\tTTL for repair history for dead nodes: " + config.getAutoRepairHistoryClearDeleteHostsBufferInterval());
        for (RepairType repairType : RepairType.values())
        {
            sb.append(formatRepairTypeConfig(probe, repairType, config));
        }

        out.println(sb);
    }

    private String formatRepairTypeConfig(NodeProbe probe, RepairType repairType, AutoRepairConfig config)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\nconfiguration for repair type: " + repairType);
        sb.append("\n\tenabled: " + config.isAutoRepairEnabled(repairType));
        sb.append("\n\tminimum repair interval: " + config.getRepairMinInterval(repairType));
        sb.append("\n\trepair threads: " + config.getRepairThreads(repairType));
        sb.append("\n\tnumber of repair subranges: " + config.getRepairSubRangeNum(repairType));
        sb.append("\n\tpriority hosts: " + Joiner.on(',').skipNulls().join(probe.getRepairPriorityForHosts(repairType)));
        sb.append("\n\tsstable count higher threshold: " + config.getRepairSSTableCountHigherThreshold(repairType));
        sb.append("\n\ttable max repair time in sec: " + config.getAutoRepairTableMaxRepairTime(repairType));
        sb.append("\n\tignore datacenters: " + Joiner.on(',').skipNulls().join(config.getIgnoreDCs(repairType)));
        sb.append("\n\trepair primary token-range: " + config.getRepairPrimaryTokenRangeOnly(repairType));
        sb.append("\n\tnumber of parallel repairs within group: " + config.getParallelRepairCountInGroup(repairType));
        sb.append("\n\tpercentage of parallel repairs within group: " + config.getParallelRepairPercentageInGroup(repairType));
        sb.append("\n\tmv repair enabled: " + config.getMVRepairEnabled(repairType));
        sb.append("\n\tinitial scheduler delay: " + config.getInitialSchedulerDelay(repairType));

        return sb.toString();
    }
}
