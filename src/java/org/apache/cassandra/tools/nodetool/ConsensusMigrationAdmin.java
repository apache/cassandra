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
import java.util.Optional;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.repair.consistent.admin.RepairStats;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * For managing migration from one consensus protocol to another.
 *
 * Mark ranges as migrating, and list the migrating ranges.
 */
public abstract class ConsensusMigrationAdmin extends NodeTool.NodeToolCmd
{
    @Command(name = "list", description = "list migrating ranges")
    public static class ListCmd extends ConsensusMigrationAdmin
    {
//        @Option(title = "all", name = {"-a", "--all"}, description = "include completed and failed sessions")
//        private boolean all;
//
//        @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
//        private String startToken = StringUtils.EMPTY;
//
//        @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
//        private String endToken = StringUtils.EMPTY;

        protected void execute(NodeProbe probe)
        {
//            PrintStream out = probe.output().out;
//            List<Map<String, String>> sessions = probe.getRepairServiceProxy().getSessions(all, getRangeString(startToken, endToken));
//            if (sessions.isEmpty())
//            {
//                out.println("no migrations");
//            }
//            else
//            {
//                List<List<String>> rows = new ArrayList<>();
//                rows.add(Lists.newArrayList("id",
//                                            "state",
//                                            "last activity",
//                                            "coordinator",
//                                            "participants",
//                                            "participants_wp"));
//                int now = FBUtilities.nowInSeconds();
//                for (Map<String, String> session : sessions)
//                {
//                    int updated = Integer.parseInt(session.get(LocalSessionInfo.LAST_UPDATE));
//                    List<String> values = Lists.newArrayList(session.get(LocalSessionInfo.SESSION_ID),
//                                                             session.get(LocalSessionInfo.STATE),
//                                                             (now - updated) + " (s)",
//                                                             session.get(LocalSessionInfo.COORDINATOR),
//                                                             session.get(LocalSessionInfo.PARTICIPANTS),
//                                                             session.get(LocalSessionInfo.PARTICIPANTS_WP));
//                    rows.add(values);
//                }
//
//                printTable(rows, out);
//            }
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
            checkNotNull(startToken, "Start token must be specified");
            checkNotNull(endToken, "End token must be specified");
            checkArgument(schemaArgs.size() >= 2, "Must specify a keyspace and at least one table");
            checkArgument((endToken != null && startToken != null) || (endToken == null && startToken == null), "Must specify start and end token together");
            Optional<String> maybeRangesStr = startToken != null ? Optional.of(startToken + ":" + endToken) : Optional.empty();
            List<String> keyspaces = schemaArgs.size() > 0 ? singletonList(schemaArgs.get(0)) : emptyList();
            Optional<List<String>> maybeTables = schemaArgs.size() > 1 ? Optional.of(schemaArgs.subList(1, schemaArgs.size())) : Optional.empty();
            probe.getStorageService().migrateConsensusProtocol(targetProtocol, keyspaces, maybeTables, maybeRangesStr);
        }
    }

    @Command(name = "set-target-protocol", description = "Set or change the target consensus protocol of the specified tables. If a migration is in progress then the migration will be reversed with migrating ranges still migrating, unmigrated ranges marked as migrated, and migrating ranges will need migration. Be aware that if no migration was in progress for a table it will immediately cause the table to run on the target protocol because the ranges requiring migration are derived from the migrated ranges that don't exist.")
    public static class SetTargetProtocol extends ConsensusMigrationAdmin
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> schemaArgs = new ArrayList<>();

        @Option(title = "target_protocol", name = {"-tp", "--target-protocol"}, description = "Use -tp to specify what consensus protocol should be migrated to", required=true)
        private String targetProtocol = null;

        protected void execute(NodeProbe probe)
        {
            checkArgument(schemaArgs.size() >= 2, "Must specify a keyspace and at least one table");
            List<String> keyspaces = schemaArgs.size() > 0 ? singletonList(schemaArgs.get(0)) : emptyList();
            Optional<List<String>> maybeTables = schemaArgs.size() > 1 ? Optional.of(schemaArgs.subList(1, schemaArgs.size())) : Optional.empty();
            probe.getStorageService().setConsensusMigrationTargetProtocol(targetProtocol, keyspaces, maybeTables);
        }
    }

    @Command(name = "summarize-repaired", description = "return the most recent repairedAt timestamp for the given token range " +
                                                        "(or all replicated ranges if no tokens are provided)")
    public static class SummarizeRepairedCmd extends ConsensusMigrationAdmin
    {
        @Option(title = "verbose", name = {"-v", "--verbose"}, description = "print additional info ")
        private boolean verbose = false;

        @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
        private String startToken = StringUtils.EMPTY;

        @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
        private String endToken = StringUtils.EMPTY;

        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> schemaArgs = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            PrintStream out = probe.output().out;
            List<CompositeData> compositeData = probe.getRepairServiceProxy().getRepairStats(schemaArgs, getRangeString(startToken, endToken));

            if (compositeData.isEmpty())
            {
                out.println("no stats");
                return;
            }

            List<RepairStats> stats = new ArrayList<>(compositeData.size());
            compositeData.forEach(cd -> stats.add(RepairStats.fromComposite(cd)));

            stats.sort((l, r) -> {
                int cmp = l.keyspace.compareTo(r.keyspace);
                if (cmp != 0)
                    return cmp;

                return l.table.compareTo(r.table);
            });

            List<String> header = Lists.newArrayList("keyspace", "table", "min_repaired", "max_repaired");
            if (verbose)
                header.add("detail");

            List<List<String>> rows = new ArrayList<>(stats.size() + 1);
            rows.add(header);

            for (RepairStats stat : stats)
            {
                List<String> row = Lists.newArrayList(stat.keyspace,
                                                      stat.table,
                                                      Long.toString(stat.minRepaired),
                                                      Long.toString(stat.maxRepaired));
                if (verbose)
                {
                    row.add(Joiner.on(", ").join(Iterables.transform(stat.sections, RepairStats.Section::toString)));
                }
                rows.add(row);
            }

            printTable(rows, out);
        }
    }

    @Command(name = "cleanup", description = "cleans up pending data from completed sessions. " +
                                             "This happens automatically, but the command is provided " +
                                             "for situations where it needs to be expedited." +
                                            " Use --force to cancel compactions that are preventing promotion")
    public static class CleanupDataCmd extends ConsensusMigrationAdmin
    {
        @Option(title = "force", name = {"-f", "--force"}, description = "Force a cleanup.")
        private boolean force = false;

        @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
        private String startToken = StringUtils.EMPTY;

        @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
        private String endToken = StringUtils.EMPTY;

        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> schemaArgs = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            PrintStream out = probe.output().out;
            out.println("Cleaning up data from completed sessions...");
            List<CompositeData> compositeData = probe.getRepairServiceProxy().cleanupPending(schemaArgs, getRangeString(startToken, endToken), force);

            List<CleanupSummary> summaries = new ArrayList<>(compositeData.size());
            compositeData.forEach(cd -> summaries.add(CleanupSummary.fromComposite(cd)));

            summaries.sort((l, r) -> {
                int cmp = l.keyspace.compareTo(r.keyspace);
                if (cmp != 0)
                    return cmp;

                return l.table.compareTo(r.table);
            });

            List<String> header = Lists.newArrayList("keyspace", "table", "successful sessions", "unsuccessful sessions");
            List<List<String>> rows = new ArrayList<>(summaries.size() + 1);
            rows.add(header);

            boolean hasFailures = false;
            for (CleanupSummary summary : summaries)
            {
                List<String> row = Lists.newArrayList(summary.keyspace,
                                                      summary.table,
                                                      Integer.toString(summary.successful.size()),
                                                      Integer.toString(summary.unsuccessful.size()));

                hasFailures |= !summary.unsuccessful.isEmpty();
                rows.add(row);
            }

            if (hasFailures)
                out.println("Some tables couldn't be cleaned up completely");

            printTable(rows, out);
        }
    }

    @Command(name = "cancel", description = "cancel an incremental repair session." +
                                            " Use --force to cancel from a node other than the repair coordinator" +
                                            " Attempting to cancel FINALIZED or FAILED sessions is an error.")
    public static class CancelCmd extends ConsensusMigrationAdmin
    {
        @Option(title = "force", name = {"-f", "--force"}, description = "Force a cancellation.")
        private boolean force = false;

        @Option(title = "session", name = {"-s", "--session"}, description = "The session to cancel", required = true)
        private String sessionToCancel;

        protected void execute(NodeProbe probe)
        {
            probe.getRepairServiceProxy().failSession(sessionToCancel, force);
        }
    }

    private static void printTable(List<List<String>> rows, PrintStream out)
    {
        if (rows.isEmpty())
            return;

        // get max col widths
        int[] widths = new int[rows.get(0).size()];
        for (List<String> row : rows)
        {
            assert row.size() == widths.length;
            for (int i = 0; i < widths.length; i++)
            {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        List<String> fmts = new ArrayList<>(widths.length);
        for (int i = 0; i < widths.length; i++)
        {
            fmts.add("%-" + widths[i] + "s");
        }

        // print
        for (List<String> row : rows)
        {
            List<String> formatted = new ArrayList<>(row.size());
            for (int i = 0; i < widths.length; i++)
            {
                formatted.add(String.format(fmts.get(i), row.get(i)));
            }
            out.println(Joiner.on(" | ").join(formatted));
        }
    }

    static String getRangeString(String startToken, String endToken)
    {
        String rangeStr = null;
        if (!startToken.isEmpty() || !endToken.isEmpty())
            rangeStr = startToken + ':' + endToken;
        return rangeStr;
    }
}
