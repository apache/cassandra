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

import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "truncatehints", description = "Truncate all hints on the local node, or truncate hints for the endpoint(s) specified.")
public class TruncateHints extends NodeToolCmd
{
    @Arguments(usage = "[endpoint ... ]", description = "Endpoint address(es) to delete hints for, either ip address (\"127.0.0.1\") or hostname")
    private String endpoint = EMPTY;

    @Override
    public void execute(NodeProbe probe)
    {
        if (endpoint.isEmpty())
            probe.truncateHints();
        else
            probe.truncateHints(endpoint);
    }
}
