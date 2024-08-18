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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;


import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "sstablerepairedset", description = "Set the repaired state of SSTables for given keyspace/tables")
public class SSTableRepairedSet extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace> [<table...>]", description = "The keyspace optionally followed by one or more tables", required = true)
    protected List<String> args = new ArrayList<>();

    @Option(title = "really-set",
    name = { "--really-set" },
    description = "Really set the repaired state of SSTables. If not set, only print SSTables that would be affected.")
    protected boolean reallySet = false;

    @Option(title = "is-repaired",
    name = { "--is-repaired" },
    description = "Set SSTables to repaired state.")
    protected boolean isRepaired = false;

    @Option(title = "is-unrepaired",
    name = { "--is-unrepaired" },
    description = "Set SSTables to unrepaired state.")
    protected boolean isUnrepaired = false;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;

        String message;
        if (reallySet)
        {
            message = "Mutating repaired state of SSTables for";
        }
        else
        {
            message = "Previewing repaired state mutation of SSTables for";
        }

        if (args.isEmpty())
        {
            out.println("At least a keyspace name must be provided.");
            return;
        }
        String keyspace = args.get(0);

        List<String> tables;
        if (args.size() > 1)
        {
            tables = args.subList(1, args.size());
            message += " tables " + String.join(", ", tables) + " in";
        }
        else
        {
            tables = probe.getTablesForKeyspace(keyspace);
            message += " all tables in";
        }
        message += " keyspace " + keyspace;

        if (isRepaired == isUnrepaired)
        {
            out.println("Exactly one of --is-repaired or --is-unrepaired must be provided.");
            return;
        }
        message += " to " + (isRepaired ? "repaired" : "unrepaired");
        out.println(message);

        try
        {
            List<String> mutatedSSTables = probe.mutateSSTableRepairedState(isRepaired, !reallySet, keyspace, new ArrayList<>(tables));
            if (!reallySet)
                out.println("The following SSTables would be mutated:");
            else
                out.println("The following SSTables were mutated:");
            for (String sstable : mutatedSSTables)
                out.println(sstable);
        }
        catch (InvalidRequestException e)
        {
            out.println(e.getMessage());
        }
    }
}
