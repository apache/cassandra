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

import java.util.UUID;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getautorepairringhostids", description = "Print autorepair ring host ids")
public class GetAutoRepairRingHostIds extends NodeToolCmd
{
    protected void execute(NodeProbe probe)
    {
        if (probe.isAutoRepairEnabled())
        {
            StringBuilder sb = new StringBuilder();
            sb.append("repair ring host ids: ");
            for (UUID hostId : probe.getCurrentRingHostIds()) {
                sb.append("\n" + hostId);
            }
            System.out.println(sb.toString());
        }
        else
        {
            System.out.println("AutoRepair is not enabled");
        }
    }
}
