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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "altertopology", description = "Modify the datacenter and/or rack of one or more nodes")
public class AlterTopology extends AbstractCommand
{
    @Parameters(description = { "One or more node identifiers, which may be either a node id, host id or broadcast address, each with a target dc:rack",
                                "<node=dc:rack> [<node=dc:rack>...]" })
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(!args.isEmpty(), "Invalid arguments; no changes specified");
        try
        {
            probe.getStorageService().alterTopology(String.join(",", args));
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
