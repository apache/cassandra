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
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Nodetool command to drop a CIDR group and associated mapping from the table {@link AuthKeyspace#CIDR_GROUPS}
 */
@Command(name = "dropcidrgroup", description = "Drop an existing cidr group")
public class DropCIDRGroup extends NodeToolCmd
{
    @Arguments(usage = "<cidrGroup>", description = "Requires a cidr group name")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 1, "dropcidrgroup command requires a cidr group name");

        String cidrGroupName = args.get(0);
        probe.dropCidrGroup(cidrGroupName);

        probe.output().out.println("Deleted CIDR group " + cidrGroupName);
    }
}
