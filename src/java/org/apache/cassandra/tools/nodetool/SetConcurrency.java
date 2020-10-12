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

@Command(name = "setconcurrency", description = "Set maximum concurrency for processing stage")
public class SetConcurrency extends NodeToolCmd
{
    @Arguments(title = "<pool-name> <maximum-concurrency> | <stage-name> <core-pool> <maximum-concurrency>",
    usage = "<stage-name> <maximum-concurrency> | <stage-name> <core-pool> <maximum-concurrency>",
    description = "Set concurrency for processing stage",
    required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 2 && args.size() <= 3, "setconcurrency requires stage name, optional core pool size and maximum concurrency");

        int corePoolSize = args.size() == 2 ? -1 : Integer.valueOf(args.get(1));
        int maximumPoolSize = args.size() == 2 ? Integer.valueOf(args.get(1)) : Integer.valueOf(args.get(2));

        checkArgument(args.size() == 2 || corePoolSize >= 0, "Core pool size must be non-negative");
        checkArgument(maximumPoolSize >= 0, "Maximum pool size must be non-negative");

        try
        {
            probe.setConcurrency(args.get(0), corePoolSize, maximumPoolSize);
        }
        catch (IllegalArgumentException e)
        {
            String message = e.getMessage() != null ? e.getMessage() : "invalid pool size";
            probe.output().out.println("Unable to set concurrency: " + message);
            System.exit(1);
        }
    }
}
