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
import org.apache.cassandra.tools.NodeTool;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

@Command(name = "setminimumrf", description = "Sets minimum keyspace replication factor.")
public class SetMinimumKeyspaceRF extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<setWarn> <value>", usage = "<setWarn> <value>", description = "warn flag and minimum keyspace rf value", required = true)
    private List<String> args = new ArrayList<>();
    private static int DISABLED_GUARDRAIL = -1;

    protected void execute(NodeProbe probe)
    {
        boolean setWarn = parseBoolean(args.get(0));
        int value = parseInt(args.get(1));
        if(setWarn)
            probe.setMinimumKeyspaceReplicationFactor(value, DISABLED_GUARDRAIL);
        else
            probe.setMinimumKeyspaceReplicationFactor(DISABLED_GUARDRAIL, value);
    }
}
