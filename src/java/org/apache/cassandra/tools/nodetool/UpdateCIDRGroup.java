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
 * Nodetool command to insert/update a CIDR group and associated mapping in the table {@link AuthKeyspace#CIDR_GROUPS}
 */
@Command(name = "updatecidrgroup", description = "Insert/Update a cidr group")
public class UpdateCIDRGroup extends NodeToolCmd
{
    @Arguments(usage = "[<cidrGroup> <cidr> ...]", description = "Requires a cidr group name, followed by one or more CIDRs separated by space")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() > 1, "updatecidrgroup command requires a cidr group name and atleast one CIDR");

        String cidrGroupName = args.get(0);

        try
        {
            probe.updateCidrGroup(cidrGroupName, new ArrayList<>(args.subList(1, args.size())));
        }
        catch (IllegalArgumentException e)
        {
            // IllegalArgumentException message going to stdout, so throw different type of exception to land in stderr
            throw new RuntimeException(e);
        }
    }
}
