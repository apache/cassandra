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

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspace;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalTables;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "upgradesstables", description = "Rewrite sstables (for the requested tables) that are not on the current version (thus upgrading them to said current version)")
public class UpgradeSSTable extends AbstractCommand
{
    @CassandraUsage(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "The keyspace followed by one or many tables", arity = "0..1")
    private String keyspace;

    @Parameters(index = "1..*", description = "The tables to upgrade", arity = "0..*")
    private List<String> tables;

    @Option(paramLabel = "include_all",
            names = { "-a", "--include-all-sstables" },
            description = "Use -a to include all sstables, even those already on the current version")
    private boolean includeAll = false;

    @Option(paramLabel = "max_timestamp",
            names = { "-t", "--max-timestamp" },
            description = "Use -t to compact only SSTables that have local creation time _older_ than the given timestamp")
    private long maxSSTableTimestamp = Long.MAX_VALUE;

    @Option(paramLabel = "jobs",
            names = { "-j", "--jobs" },
            description = "Number of sstables to upgrade simultanously, set to 0 to use all available compaction threads")
    private int jobs = 2;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, tables);
        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        for (String keyspace : keyspaces)
        {
            for (int retries = 0; retries < 5; retries++)
            {
                try
                {
                    if (retries > 0)
                        Thread.sleep(500);
                    probe.upgradeSSTables(probe.output().out, keyspace, !includeAll, maxSSTableTimestamp, jobs, tableNames);
                    break;
                }
                catch (RuntimeException cie)
                {
                    // Spin retry. See CASSANDRA-18635
                    if (ExceptionUtils.indexOfThrowable(cie, CompactionInterruptedException.class) != -1 && retries == 4)
                        throw (cie);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during enabling auto-compaction", e);
                }
            }
        }
    }
}
