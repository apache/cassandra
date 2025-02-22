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

@Command(name = "setcacheperiod", description = "Set global key, row, and counter cache period in second units")
public class SetCachePeriod extends NodeToolCmd
{
    @Arguments(title = "<key-cache-period> <row-cache-period> <counter-cache-period>",
               usage = "<key-cache-period> <row-cache-period> <counter-cache-period>",
               description = "Key cache, row cache, and counter period in second units",
               required = true)
    private List<Integer> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3, "setcacheperiod requires key-cache-period, row-cache-period, and counter-cache-period args.");
        probe.setCachePeriod(args.get(0), args.get(1), args.get(2));
    }
}
