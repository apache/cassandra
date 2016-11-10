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

import io.airlift.command.Command;

import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getlogginglevels", description = "Get the runtime logging levels")
public class GetLoggingLevels extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        // what if some one set a very long logger name? 50 space may not be enough...
        System.out.printf("%n%-50s%10s%n", "Logger Name", "Log Level");
        for (Map.Entry<String, String> entry : probe.getLoggingLevels().entrySet())
            System.out.printf("%-50s%10s%n", entry.getKey(), entry.getValue());
    }
}