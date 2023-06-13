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
package org.apache.cassandra.tools;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

/**
 * Nodetool command to list available CIDR groups, in the table {@link AuthKeyspace#CIDR_GROUPS}
 */
@Command(name = "listcidrgroups", description = "List existing cidr groups")
public class ListCIDRGroups extends NodeToolCmd
{
    @Arguments(usage = "[<cidrGroup>]", description = "LIST operation can be invoked with or without cidr group name")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;

        if (args.size() < 1)
        {
            probe.printSet(out, "CIDR Groups", probe.listAvailableCidrGroups());
            return;
        }

        String cidrGroup = args.get(0);
        probe.printSet(out, "CIDRs", probe.listCidrsOfCidrGroup(cidrGroup));
    }
}
