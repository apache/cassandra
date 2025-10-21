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
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspaceNonLocal;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalTables;

/**
 * Provides a way to set the repaired state of SSTables without any downtime through nodetool.
 */
@Command(name = "sstablerepairedset", description = "Set the repaired state of SSTables for given keyspace/tables")
public class SSTableRepairedSet extends AbstractCommand
{
    protected List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "Keyspace to set the repaired state of SSTables. If not provided, all keyspaces are used.")
    public String keyspace;

    @Parameters(index = "1..*", description = "Tables to set the repaired state of SSTables. If not provided, all tables in the keyspace are used.")
    public String[] tables;

    @Option(paramLabel = "really_set",
            names = { "--really-set" },
            description = "Really set the repaired state of SSTables. If not set, only print SSTables that would be affected.")
    protected boolean reallySet = false;

    @Option(paramLabel = "is_repaired",
            names = { "--is-repaired" },
            description = "Set SSTables to repaired state.")
    protected boolean isRepaired = false;

    @Option(paramLabel = "is_unrepaired",
            names = { "--is-unrepaired" },
            description = "Set SSTables to unrepaired state.")
    protected boolean isUnrepaired = false;

    @Override
    public void execute(NodeProbe probe)
    {
        args = args.isEmpty() ? CommandUtils.concatArgs(keyspace, tables) : args;
        PrintStream out = probe.output().out;

        if (isRepaired == isUnrepaired)
        {
            out.println("Exactly one of --is-repaired or --is-unrepaired must be provided.");
            return;
        }

        String message;
        if (reallySet)
            message = "Mutating repaired state of SSTables for";
        else
            message = "Previewing repaired state mutation of SSTables for";

        List<String> keyspaces = parseOptionalKeyspaceNonLocal(args, probe);
        List<String> tables = new ArrayList<>(Arrays.asList(parseOptionalTables(args)));

        if (args.isEmpty())
            message += " all keyspaces";
        else
            message += tables.isEmpty() ? " all tables" : " tables " + String.join(", ", tables)
                                                          + " in keyspace " + keyspaces.get(0);
        message += " to " + (isRepaired ? "repaired" : "unrepaired");
        out.println(message);

        List<String> sstableList = new ArrayList<>();
        for (String keyspace : keyspaces)
        {
            try
            {
                sstableList.addAll(probe.mutateSSTableRepairedState(isRepaired, !reallySet, keyspace,
                                                                    tables.isEmpty()
                                                                    ? probe.getAutoRepairTablesForKeyspace(keyspace) // mutate all tables
                                                                    : tables)); // mutate specific tables
            }
            catch (InvalidRequestException e)
            {
                out.println(e.getMessage());
            }
        }
        if (!reallySet)
            out.println("The following SSTables would be mutated:");
        else
            out.println("The following SSTables were mutated:");
        for (String sstable : sstableList)
            out.println(sstable);
    }
}
