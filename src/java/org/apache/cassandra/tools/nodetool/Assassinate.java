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

import java.net.UnknownHostException;

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@Command(name = "assassinate", description = "Forcefully remove a dead node without re-replicating any data.  Use as a last resort if you cannot removenode")
public class Assassinate extends AbstractCommand
{
    @Parameters(paramLabel = "ip_address", description = "IP address of the endpoint to assassinate", arity = "1")
    private String endpoint = EMPTY;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            probe.assassinateEndpoint(endpoint);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
