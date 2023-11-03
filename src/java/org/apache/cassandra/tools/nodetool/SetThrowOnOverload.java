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

@Command(name = "setthrowonoverload", description =  "Set the throw_on_overload flag to enable/disable memory-based throttler")
public class SetThrowOnOverload extends NodeTool.NodeToolCmd
{
    @Arguments(title = "enabled", usage = "<ture>|<false>", description = "Set the throw_on_overload flag", required = true)
    private String enabled;

    @Override
    public void execute(NodeProbe probe)
    {
        switch (enabled)
        {
            case "true":
                probe.setThrowOnOverload(true);
                break;
            case "false":
                probe.setThrowOnOverload(false);
                break;
            default:
                System.out.println("Unknown throw_on_overload flag: " + enabled);
                break;
        }
    }
}
