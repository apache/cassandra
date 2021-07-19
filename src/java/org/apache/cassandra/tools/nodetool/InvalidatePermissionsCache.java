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

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "invalidatepermissionscache", description = "Invalidate the permissions cache")
public class InvalidatePermissionsCache extends NodeToolCmd
{
    @Arguments(usage = "[<user> <resource>...]", description = "The user followed by one or many resources. By default, all users")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        if (args.isEmpty())
        {
            probe.invalidatePermissionsCache();
        }
        else
        {
            checkArgument(args.size() >= 2, "invalidatepermissionscache requires user and at least one resource args");

            String userName = args.get(0);
            for (String resourceName : args.subList(1, args.size()))
            {
                probe.invalidatePermissionsCache(userName, resourceName);
            }
        }
    }
}