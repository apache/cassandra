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
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

/**
 * Nodetool command to invalidate CIDR permissions cache, for a give role or for all roles in the cache.
 */
@Command(name = "invalidatecidrpermissionscache", description = "Invalidate the cidr permissions cache")
public class InvalidateCIDRPermissionsCache extends NodeToolCmd
{
    @Arguments(usage = "[<role>...]", description = "List of roles to invalidate. By default, all roles")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        if (args.isEmpty())
        {
            probe.invalidateCidrPermissionsCache("");
            probe.output().out.println("Invalidated CIDR permissions cache");
        }
        else
        {
            for (String roleName : args)
            {
                if (probe.invalidateCidrPermissionsCache(roleName))
                    probe.output().out.println("Invalidated the role " + roleName + " from CIDR permissions cache");
                else
                    probe.output().out.println("Not found role " + roleName + " in CIDR permissions cache, nothing to invalidate");
            }
        }
    }
}
