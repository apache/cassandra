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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.Arguments;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;


import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Command(name = "disableauditlogrolefilter", description = "Disables audit log filtering")
public class DisableAuditLogRoleFilter extends NodeToolCmd
{
    private static final Pattern PATTERN = Pattern.compile("^[a-z_]+$");
    @Option(title = "delete", name = { "--delete"}, description = "Delete the audit log filter entry")
    boolean shouldDelete = false;

    @Option(title = "refresh", name = "--refresh", description = "Force an immediate refresh of the cache from the "+
                                                                "underlying talbe.")
    private boolean refresh = false;

    @Arguments(title = "roles", usage = "[<role> ...]", description = "A list of roles for which to remove audit " +
                                                                      "filter values", required = false)
    private ArrayList<String> roles = new ArrayList<>();

    @Override
    protected void execute(NodeProbe probe)
    {
        if (roles.isEmpty())
        {
            throw new IllegalArgumentException("Roles must be specified");
        }
        roles = filterRoles(roles);

        if (roles.isEmpty())
        {
            probe.output().out.println("Roles " + roles + " not found");
            return;
        }

        probe.output().out.println("Disabling audit log role filters for " + roles);
        probe.disableAuditLogRoleFilter(shouldDelete, roles, refresh);
    }

    private ArrayList<String> filterRoles(ArrayList<String> roles)
    {
        return roles.stream()
                    .filter(e -> PATTERN.matcher(e).matches())
                    .collect(Collectors.toCollection(ArrayList::new));

    }
}
