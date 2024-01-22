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

@Command(name = "setforcedisabletrace", description =  "Set force_disable_trace flag for this node to disable/enable trace.")
public class SetForceDisableTrace extends NodeTool.NodeToolCmd
{
    @Arguments(title = "disabled", usage = "<true>|<false>", description = "Set the force_disable_trace flag", required = true)
    private String disabled;

    @Override
    public void execute(NodeProbe probe)
    {
        switch (disabled)
        {
            case "true":
                probe.setForceDisableTrace(true);
                break;
            case "false":
                probe.setForceDisableTrace(false);
                break;
            default:
                System.out.println("Unknown force_disable_trace flag: " + disabled);
                break;
        }
    }
}
