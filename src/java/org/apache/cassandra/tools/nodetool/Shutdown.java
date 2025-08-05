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

import java.net.UnknownHostException;

import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "shutdown", description = "Forcefully shutdown a node. Use to shutdown a node running on bad host. Use -f to shutdown " +
                                          "the target node so if anyone tries to bring the node up, the node will crash because it is" +
                                          "marked as force shutdown. To remove the target node from force shutdown, run this command without" +
                                          " -f so the node will be marked as normal shutdown and the node should be able to get up. Only use " +
                                          "this command if there is no other way to shutdown the target node. If the given node is not part of " +
                                          "the ring, this command will return error. This means this command can only shutdown a node that has" +
                                          "successfully joined the ring before.")
public class Shutdown extends NodeToolCmd
{
    @Arguments(title = "ip address", usage = "<ip_address>", description = "IP address of the endpoint to shutdown", required = true)
    private String endpoint = EMPTY;

    @Option(title = "force",
    name = {"-f", "--force"},
    description = "Force shutdown of the given node")
    private boolean force = false;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            probe.shutdownEndpoint(endpoint, force);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}