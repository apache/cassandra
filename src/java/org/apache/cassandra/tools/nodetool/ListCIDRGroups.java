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

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.CommandUtils.printSet;

/**
 * Nodetool command to list available CIDR groups, in the table {@link AuthKeyspace#CIDR_GROUPS}
 */
@Command(name = "listcidrgroups", description = "List existing cidr groups")
public class ListCIDRGroups extends AbstractCommand
{
    @Parameters(paramLabel = "cidrGroup", description = "LIST operation can be invoked with or without cidr group name", arity = "0..1")
    private String cidrGroup;

    @Override
    public void execute(NodeProbe probe)
    {
        if (StringUtils.isEmpty(cidrGroup))
        {
            printSet(probe.output().out, "CIDR Groups", probe.listAvailableCidrGroups());
            return;
        }

        printSet(probe.output().out, "CIDRs", probe.listCidrsOfCidrGroup(cidrGroup));
    }
}
