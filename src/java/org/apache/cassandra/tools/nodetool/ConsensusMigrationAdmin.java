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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;

/**
 * For managing migration from one consensus protocol to another.
 *
 * Mark ranges as migrating, and list the migrating ranges.
 */
public abstract class ConsensusMigrationAdmin extends NodeTool.NodeToolCmd
{
    @Command(name = "list", description = "List migrating tables and ranges")
    public static class ListCmd extends ConsensusMigrationAdmin
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> schemaArgs = new ArrayList<>();

        @Option(title = "format", name = {"-f", "--format"}, description = "Output format, YAML and JSON are the only supported formats, default YAML, prefix with `minified-` to turn off pretty printing")
        private String format = "yaml";

        protected void execute(NodeProbe probe)
        {
            Set<String> keyspaceNames = schemaArgs.size() > 0 ? singleton(schemaArgs.get(0)) : null;
            Set<String> tableNames = schemaArgs.size() > 1 ? new HashSet<>(schemaArgs.subList(1, schemaArgs.size())) : null;
            String output = probe.getStorageService().listConsensusMigrations(keyspaceNames, tableNames, format);
            probe.output().out.println(output);
        }
    }

    @Command(name = "begin-migration", description = "Mark the range as migrating for the specified token range and tables")
    public static class BeginMigration extends ConsensusMigrationAdmin
    {
        @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
        private String startToken = null;

        @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
        private String endToken = null;

        @Option(title = "target_protocol", name = {"-tp", "--target-protocol"}, description = "Use -tp to specify what consensus protocol should be migrated to", required=true)
        private String targetProtocol = null;

        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> schemaArgs = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            checkArgument((endToken != null && startToken != null) || (endToken == null && startToken == null), "Must specify start and end token together");
            String maybeRangesStr = startToken != null ? startToken + ":" + endToken : null;
            List<String> keyspaceNames = parseOptionalKeyspace(schemaArgs, probe, KeyspaceSet.ACCORD_MANAGED);
            List<String> maybeTableNames = schemaArgs.size() > 1 ? schemaArgs.subList(1, schemaArgs.size()) : null;
            probe.getStorageService().migrateConsensusProtocol(targetProtocol, keyspaceNames, maybeTableNames, maybeRangesStr);
            probe.output().out.println("Marked requested ranges as migrating. Repair needs to be run in order to complete the migration");
        }
    }

    @Command(name = "finish-migration", description = "Complete the migration for a range that has already begun migration")
    public static class FinishMigration extends ConsensusMigrationAdmin
    {
        @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts (exclusive)")
        private String startToken = null;

        @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends (inclusive)")
        private String endToken = null;

        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> schemaArgs = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            checkArgument((endToken != null) == (startToken != null), "Start and end token must be specified together");
            String maybeRangesStr = startToken != null ? startToken + ":" + endToken : null;
            List<String> keyspaceNames = parseOptionalKeyspace(schemaArgs, probe, KeyspaceSet.ACCORD_MANAGED);
            List<String> maybeTableNames = schemaArgs.size() > 1 ? schemaArgs.subList(1, schemaArgs.size()) : null;
            for (String keyspace : keyspaceNames)
            {
                List<Integer> commands = probe.getStorageService().finishConsensusMigration(keyspace, maybeTableNames, maybeRangesStr);
                for (Integer command : commands)
                {
                    try
                    {
                        probe.blockOnAsyncRepair(probe.output().out, keyspace, command);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Error occurred attempting to finish migration for keyspace " + keyspace + " tables " + maybeTableNames + " and ranges " + maybeRangesStr, e);
                    }
                }
            }
            probe.output().out.printf("Finished consensus migration range (%s) of keyspaces %s and tables %s%n", maybeRangesStr, keyspaceNames, maybeTableNames);
        }
    }
}
