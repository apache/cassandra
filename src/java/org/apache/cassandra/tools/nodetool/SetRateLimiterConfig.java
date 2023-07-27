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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setratelimiterconfig", description = "sets the ratelimiter configuration")
public class SetRateLimiterConfig extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<ratelimiterparam> <value>", usage = "<ratelimiterparam> <value>",
            description = "ratelimiter param and value.\nPossible ratelimiter parameters are as following: " +
                    "[var1|var2noop]",
            required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setratelimiterconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);


        if (paramType.equals("var1"))
        {
            probe.setRatLimiterVar1(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("var2noop"))
        {
            System.out.println("var2 no-op");
        }
    }
}
