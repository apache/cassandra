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

import java.util.Map;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getcfswithnondefaultcompactionparams", description = "Print all user tables with non default compaction params")
public class GetCfsWithNonDefaultCompactionParams extends NodeTool.NodeToolCmd
{
    protected void execute(NodeProbe probe)
    {
        Map<String, String> cfs = probe.getCfsWithNonDefaultCompactionParams();
        StringBuilder sb = new StringBuilder();
        sb.append("User tables with non-default compaction params:\n");
        cfs.entrySet().stream().forEach(entry -> {
            String name = entry.getKey();
            String option = entry.getValue();
            sb.append("Table: ").append(name).append(", option: ").append(option).append('\n');
        });
        System.out.println(sb.toString());
    }
}
