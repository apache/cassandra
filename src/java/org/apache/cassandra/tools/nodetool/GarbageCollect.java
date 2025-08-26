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

import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.tools.NodeProbe;

import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspace;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalTables;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "garbagecollect", description = "Remove deleted data from one or more tables")
public class GarbageCollect extends AbstractCommand
{
    @CassandraUsage(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "The keyspace followed by one or many tables to garbage collect", arity = "0..1")
    private String keyspace;

    @Parameters(index = "1..*", description = "The tables to garbage collect", arity = "0..*")
    private String[] tables;

    @Option(paramLabel = "granularity",
            names = { "-g", "--granularity" },
            description = "Granularity of garbage removal. ROW (default) removes deleted partitions and rows, CELL also removes overwritten or deleted cells.")
    private CompactionParams.TombstoneOption tombstoneOption = CompactionParams.TombstoneOption.ROW;

    @Option(paramLabel = "jobs",
            names = { "-j", "--jobs" },
            description = "Number of sstables to cleanup simultanously, set to 0 to use all available compaction " +
                          "threads. Defaults to 1 so that collections of newer tables can see the data is deleted " +
                          "and also remove tombstones.")
    private int jobs = 1;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, tables);

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        for (String keyspace : keyspaces)
        {
            try
            {
                probe.garbageCollect(probe.output().out, tombstoneOption.toString(), jobs, keyspace, tableNames);
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during garbage collection", e);
            }
        }
    }
}
