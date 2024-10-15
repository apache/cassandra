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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "autorepairstatus", description = "Print autorepair status")
public class AutoRepairStatus extends NodeTool.NodeToolCmd
{
    @VisibleForTesting
    @Option(title = "repair type", name = { "-t", "--repair-type" }, description = "Repair type")
    protected AutoRepairConfig.RepairType repairType;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(repairType != null, "--repair-type is required.");
        PrintStream out = probe.output().out;

        AutoRepairConfig config = probe.getAutoRepairConfig();
        if (config == null || !config.isAutoRepairSchedulingEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        TableBuilder table = new TableBuilder();
        table.add("Active Repairs");
        Set<String> ongoingRepairHostIds = probe.getOnGoingRepairHostIds(repairType);
        table.add(getSetString(ongoingRepairHostIds));
        table.printTo(out);
    }

    private String getSetString(Set<String> hostIds)
    {
        if (hostIds.isEmpty())
        {
            return "NONE";
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
}
