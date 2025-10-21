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

import org.apache.cassandra.service.consensus.migration.ConsensusMigrationTarget;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.RepairRunner.RepairCmd;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspaceAccordManaged;

/**
 * For managing migration from one consensus protocol to another.
 *
 * Mark ranges as migrating, and list the migrating ranges.
 */
@CommandLine.Command(name = "consensus_admin", description = "List and mark ranges as migrating between consensus protocols",
                     subcommands = { ConsensusMigrationAdmin.BeginMigration.class,
                                    ConsensusMigrationAdmin.FinishMigration.class,
                                    ConsensusMigrationAdmin.ListCmd.class })
public class ConsensusMigrationAdmin extends AbstractCommand
{
    @Override
    protected void execute(NodeProbe probe)
    {
        AbstractCommand cmd = new CMSAdmin.DescribeCMS();
        cmd.probe(probe);
        cmd.logger(output);
        cmd.run();
    }

    @Command(name = "list", description = "List migrating tables and ranges")
    public static class ListCmd extends AbstractCommand
    {
        @Parameters(index = "0", arity = "0..1", description = "The keyspace followed by one or many tables")
        public String keyspace;

        @Parameters(index = "1..*", arity = "0..*", description = "The tables")
        public String[] tables;

        @Option(names = {"-f", "--format"}, description = "Output format, YAML and JSON are the only supported formats, default YAML, prefix with `minified-` to turn off pretty printing")
        private String format = "yaml";

        protected void execute(NodeProbe probe)
        {
            List<String> schemaArgs = CommandUtils.concatArgs(keyspace, tables);
            Set<String> keyspaceNames = schemaArgs.size() > 0 ? singleton(schemaArgs.get(0)) : null;
            Set<String> tableNames = schemaArgs.size() > 1 ? new HashSet<>(schemaArgs.subList(1, schemaArgs.size())) : null;
            String output = probe.getStorageService().listConsensusMigrations(keyspaceNames, tableNames, format);
            probe.output().out.println(output);
        }
    }

    @Command(name = "begin-migration", description = "Mark the range as migrating for the specified token range and tables")
    public static class BeginMigration extends AbstractCommand
    {
        @Option(paramLabel = "start_token", names = { "-st", "--start-token" }, description = "Use -st to specify a token at which the repair range starts")
        private String startToken = null;

        @Option(paramLabel = "end_token", names = { "-et", "--end-token" }, description = "Use -et to specify a token at which repair range ends")
        private String endToken = null;

        @Parameters(index = "0", arity = "0..1", description = "The keyspace followed by one or many tables")
        private String keyspace;

        @Parameters(index = "1..*", arity = "0..*", description = "The tables")
        private String[] tables;

        protected void execute(NodeProbe probe)
        {
            checkArgument((endToken != null && startToken != null) || (endToken == null && startToken == null), "Must specify start and end token together");
            List<String> schemaArgs = CommandUtils.concatArgs(keyspace, tables);
            String maybeRangesStr = startToken != null ? startToken + ":" + endToken : null;
            List<String> keyspaceNames = parseOptionalKeyspaceAccordManaged(schemaArgs, probe);
            List<String> maybeTableNames = schemaArgs.size() > 1 ? schemaArgs.subList(1, schemaArgs.size()) : null;
            probe.getStorageService().migrateConsensusProtocol(keyspaceNames, maybeTableNames, maybeRangesStr);
            probe.output().out.println("Marked requested ranges as migrating. Repair needs to be run in order to complete the migration");
        }
    }

    @Command(name = "finish-migration", description = "Complete the migration for a range that has already begun migration")
    public static class FinishMigration extends AbstractCommand
    {
        @Option(paramLabel = "start_token", names = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts (exclusive)")
        private String startToken = null;

        @Option(paramLabel = "end_token", names = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends (inclusive)")
        private String endToken = null;

        @Parameters(index = "0", arity = "0..1", description = "The keyspace followed by one or many tables")
        private String keyspace;

        @Parameters(index = "1..*", arity = "0..*", description = "The tables")
        private String[] tables;

        private static class FinishMigrationRepairCommand extends RepairCmd
        {
            private final NodeProbe probe;
            private final String keyspace;
            private final List<String> maybeTableNames;
            private final String maybeRangesStr;
            private final ConsensusMigrationTarget target;

            public FinishMigrationRepairCommand(NodeProbe probe, String keyspace, List<String> maybeTableNames, String maybeRangesStr, ConsensusMigrationTarget target)
            {
                super(keyspace);
                this.probe = probe;
                this.keyspace = keyspace;
                this.maybeTableNames = maybeTableNames;
                this.maybeRangesStr = maybeRangesStr;
                this.target = target;
            }

            @Override
            public Integer start()
            {
                return probe.getStorageService().finishConsensusMigration(keyspace, maybeTableNames, maybeRangesStr, target.toString());
            }
        }

        protected void execute(NodeProbe probe)
        {
            checkArgument((endToken != null) == (startToken != null), "Start and end token must be specified together");
            List<String> schemaArgs = CommandUtils.concatArgs(keyspace, tables);
            String maybeRangesStr = startToken != null ? startToken + ":" + endToken : null;
            List<String> keyspaceNames = parseOptionalKeyspaceAccordManaged(schemaArgs, probe);
            List<String> maybeTableNames = schemaArgs.size() > 1 ? schemaArgs.subList(1, schemaArgs.size()) : null;
            List<RepairCmd> repairCmds = new ArrayList<>(keyspaceNames.size() * 2);
            // Finish can't actually finish with one set of repairs when migrating from Paxos -> Accord
            // and it's async when the next invocation will see TCM updates from the repair that will correctly determine
            // the next set of repairs needed. If we spin we will issue redundant repairs.
            // It's also pretty involved not to return handles on the repairs since there is already a lot of plumbing
            // leveraging monitoring in progress repairs.
            output.out.println("Starting first round of repairs");
            for (String keyspace : keyspaceNames)
            {
                repairCmds.add(new FinishMigrationRepairCommand(probe, keyspace, maybeTableNames, maybeRangesStr, ConsensusMigrationTarget.paxos));
                repairCmds.add(new FinishMigrationRepairCommand(probe, keyspace, maybeTableNames, maybeRangesStr, ConsensusMigrationTarget.accord));
            }
            try
            {
                probe.startAndBlockOnAsyncRepairs(probe.output().out, repairCmds);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error occurred attempting to finish migration for keyspace(s) " + keyspaceNames + " tables " + maybeTableNames + " and ranges " + maybeRangesStr, e);
            }
            // The repair should have at least committed the TCM change to the node we asked to coordinate the repair
            // so calling finishedConsensusMigration a second time should trigger any needed 2nd phase repairs
            // or does nothing if none are needed
            output.out.println("Starting second round of repairs (may do nothing if migrating from Accord to Paxos)");
            repairCmds.clear();
            for (String keyspace : keyspaceNames)
            {
                repairCmds.add(new FinishMigrationRepairCommand(probe, keyspace, maybeTableNames, maybeRangesStr, ConsensusMigrationTarget.accord));
            }
            try
            {
                probe.startAndBlockOnAsyncRepairs(probe.output().out, repairCmds);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error occurred attempting to finish migration for keyspace(s) " + keyspaceNames + " tables " + maybeTableNames + " and ranges " + maybeRangesStr, e);
            }
            probe.output().out.printf("Finished consensus migration range (%s) of keyspaces %s and tables %s%n", maybeRangesStr, keyspaceNames, maybeTableNames);
        }
    }
}
