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

import java.util.ArrayList;
import java.util.List;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "getauditlogrolefilter", description = "Prints audit log filters")
public class GetAuditLogRoleFilter extends NodeToolCmd
{
    @Option(title = "refresh", name = "--refresh", description = "Force an immediate refresh of the cache from the "+
                                                                "underlying talbe.")
    private boolean refresh = false;

    @Arguments(title = "roles", usage = "[<role> ...]", description = "A list of roles for which to return configured " +
                                                                      "audit filter values", required = false)
    private ArrayList<String> roles = new ArrayList<>();

    @Override
    protected void execute(NodeProbe probe)
    {
        List<List<String>> filters = probe.getAuditLogRoleFilter(roles, refresh);

        if (filters.isEmpty() || filters.get(0).isEmpty())
        {
            probe.output().out.println("No audit log filters found.");
            return;
        }

        final TableBuilder tableBuilder = new TableBuilder();

        tableBuilder.add("Role", "Account Type", "Filter Percent");
        for (List<String> filter : filters)
        {
            tableBuilder.add(filter);
        }
        tableBuilder.printTo(probe.output().out);
    }
}
