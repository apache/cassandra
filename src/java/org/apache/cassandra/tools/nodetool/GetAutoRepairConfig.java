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

import java.util.Set;

import com.google.common.base.Joiner;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getautorepairconfig", description = "Print autorepair configurations")
public class GetAutoRepairConfig extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
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
            for (Set<String> dcGroup : probe.getDCGroups()) {
                sb.append("\n" + Joiner.on(',').skipNulls().join(dcGroup));
            }
            sb.append("\n auto repair history table delete hosts clear buffer in seconds: " + probe.getAutoRepairHistoryClearDeleteHostsBufferInSec());

            System.out.println(sb.toString());
        }
        else
        {
            System.out.println("AutoRepair is not enabled");
        }
    }
}