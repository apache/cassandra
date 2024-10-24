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
package org.apache.cassandra.tools;

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.management.InstanceNotFoundException;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import io.airlift.airline.UsageHelper;
import io.airlift.airline.UsagePrinter;
import io.airlift.airline.model.CommandGroupMetadata;
import io.airlift.airline.model.CommandMetadata;
import io.airlift.airline.model.GlobalMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.nodetool.*;
import org.apache.cassandra.tools.nodetool.layout.CassandraHelpLayout;
import org.apache.cassandra.utils.FBUtilities;
import picocli.CommandLine;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class NodeTool
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final int FALLBACK_NODETOOL_V1_CODE = -100;
    private static final String HISTORYFILE = "nodetool.history";

    private final INodeProbeFactory nodeProbeFactory;
    private final Output output;

    public static void main(String... args)
    {
        System.exit(new NodeTool(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeTool(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    public int execute(String... args)
    {
        List<Class<? extends NodeToolCmdRunnable>> commands = newArrayList(
                CassHelp.class,
                CIDRFilteringStats.class,
                Cleanup.class,
                ClearSnapshot.class,
                ClientStats.class,
                CompactionHistory.class,
                CompactionStats.class,
                DataPaths.class,
                Decommission.class,
                DescribeCluster.class,
                DescribeRing.class,
                DisableAuditLog.class,
                DisableAutoCompaction.class,
                DisableBackup.class,
                DisableBinary.class,
                DisableFullQueryLog.class,
                DisableGossip.class,
                DisableHandoff.class,
                DisableHintsForDC.class,
                DisableOldProtocolVersions.class,
                Drain.class,
                DropCIDRGroup.class,
                EnableAuditLog.class,
                EnableAutoCompaction.class,
                EnableBackup.class,
                EnableBinary.class,
                EnableFullQueryLog.class,
                EnableGossip.class,
                EnableHandoff.class,
                EnableHintsForDC.class,
                EnableOldProtocolVersions.class,
                FailureDetectorInfo.class,
                Flush.class,
                GarbageCollect.class,
                GcStats.class,
                GetAuditLog.class,
                GetAuthCacheConfig.class,
                GetBatchlogReplayTrottle.class,
                GetCIDRGroupsOfIP.class,
                GetColumnIndexSize.class,
                GetCompactionThreshold.class,
                GetCompactionThroughput.class,
                GetConcurrency.class,
                GetConcurrentCompactors.class,
                GetConcurrentViewBuilders.class,
                GetDefaultKeyspaceRF.class,
                GetEndpoints.class,
                GetFullQueryLog.class,
                GetInterDCStreamThroughput.class,
                GetLoggingLevels.class,
                GetMaxHintWindow.class,
                GetSSTables.class,
                GetSeeds.class,
                GetSnapshotThrottle.class,
                GetStreamThroughput.class,
                GetTimeout.class,
                GetTraceProbability.class,
                GossipInfo.class,
                Import.class,
                Info.class,
                InvalidateCIDRPermissionsCache.class,
                InvalidateCounterCache.class,
                InvalidateCredentialsCache.class,
                InvalidateJmxPermissionsCache.class,
                ReloadCIDRGroupsCache.class,
                InvalidateKeyCache.class,
                InvalidateNetworkPermissionsCache.class,
                InvalidatePermissionsCache.class,
                InvalidateRolesCache.class,
                InvalidateRowCache.class,
                Join.class,
                ListCIDRGroups.class,
                ListPendingHints.class,
                ListSnapshots.class,
                Move.class,
                NetStats.class,
                PauseHandoff.class,
                ProfileLoad.class,
                ProxyHistograms.class,
                RangeKeySample.class,
                Rebuild.class,
                RebuildIndex.class,
                RecompressSSTables.class,
                Refresh.class,
                RefreshSizeEstimates.class,
                ReloadLocalSchema.class,
                ReloadSeeds.class,
                ReloadSslCertificates.class,
                ReloadTriggers.class,
                RelocateSSTables.class,
                RemoveNode.class,
                Repair.class,
                ReplayBatchlog.class,
                ResetFullQueryLog.class,
                ResetLocalSchema.class,
                ResumeHandoff.class,
                Ring.class,
                Scrub.class,
                SetAuthCacheConfig.class,
                SetBatchlogReplayThrottle.class,
                SetCacheCapacity.class,
                SetCacheKeysToSave.class,
                SetColumnIndexSize.class,
                SetCompactionThreshold.class,
                SetCompactionThroughput.class,
                SetConcurrency.class,
                SetConcurrentCompactors.class,
                SetConcurrentViewBuilders.class,
                SetDefaultKeyspaceRF.class,
                SetHintedHandoffThrottleInKB.class,
                SetInterDCStreamThroughput.class,
                SetLoggingLevel.class,
                SetMaxHintWindow.class,
                SetSnapshotThrottle.class,
                SetStreamThroughput.class,
                SetTimeout.class,
                SetTraceProbability.class,
                Sjk.class,
                Snapshot.class,
                Status.class,
                StatusAutoCompaction.class,
                StatusBackup.class,
                StatusBinary.class,
                StatusGossip.class,
                StatusHandoff.class,
                Stop.class,
                StopDaemon.class,
                TableHistograms.class,
                TableStats.class,
                TopPartitions.class,
                TpStats.class,
                TruncateHints.class,
                UpdateCIDRGroup.class,
                UpgradeSSTable.class,
                Verify.class,
                Version.class,
                ViewBuildStatus.class
        );

        Cli.CliBuilder<NodeToolCmdRunnable> builder = Cli.builder("nodetool");

        builder.withDescription("Manage your Cassandra cluster")
                 .withDefaultCommand(CassHelp.class)
                 .withCommands(commands);

        builder.withGroup("repair_admin")
               .withDescription("list and fail incremental repair sessions")
               .withDefaultCommand(RepairAdmin.ListCmd.class)
               .withCommand(RepairAdmin.ListCmd.class)
               .withCommand(RepairAdmin.CancelCmd.class)
               .withCommand(RepairAdmin.CleanupDataCmd.class)
               .withCommand(RepairAdmin.SummarizePendingCmd.class)
               .withCommand(RepairAdmin.SummarizeRepairedCmd.class);

        builder.withGroup("cms")
               .withDescription("Manage cluster metadata")
               .withDefaultCommand(CMSAdmin.DescribeCMS.class)
               .withCommand(CMSAdmin.DescribeCMS.class)
               .withCommand(CMSAdmin.InitializeCMS.class)
               .withCommand(CMSAdmin.ReconfigureCMS.class)
               .withCommand(CMSAdmin.Snapshot.class)
               .withCommand(CMSAdmin.Unregister.class);

        Cli<NodeToolCmdRunnable> parser = builder.build();

        int status = 0;
        try
        {
            // Try to run the command with the new parser, if it у fails, fallback to the old parser.
            int result = new NodeToolV2(nodeProbeFactory, output)
                             // Filter out the help command, and nodetool command, and fallback to the old help message.
                             .withCommandNameFilter(cmd -> cmd.equals("help") || cmd.equals("nodetool"), FALLBACK_NODETOOL_V1_CODE)
                             .withParameterExceptionHandler((ex, arg) -> {
                                 if (ex instanceof CommandLine.UnmatchedArgumentException)
                                     return FALLBACK_NODETOOL_V1_CODE;
                                 badUse(ex);
                                 return 1;
                             })
                             .withExecutionExceptionHandler((ex, c, arg) -> {
                                 err(ex);
                                 return 2;
                             }).execute(args);

            if (result >= 0)
                return result;

            // Fallback to the old parser, and run the command.
            assert result == FALLBACK_NODETOOL_V1_CODE;
            NodeToolCmdRunnable cmd = parser.parse(args);
            cmd.run(nodeProbeFactory, output);
        } catch (IllegalArgumentException |
                IllegalStateException |
                ParseArgumentsMissingException |
                ParseArgumentsUnexpectedException |
                ParseOptionConversionException |
                ParseOptionMissingException |
                ParseOptionMissingValueException |
                ParseCommandMissingException |
                ParseCommandUnrecognizedException e)
        {
            badUse(e);
            status = 1;
        } catch (Throwable throwable)
        {
            err(Throwables.getRootCause(throwable));
            status = 2;
        }

        return status;
    }

    public static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE).newWriter(APPEND))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    public static void badUse(Consumer<String> out, Throwable e)
    {
        out.accept("nodetool: " + e.getMessage());
        out.accept("See 'nodetool help' or 'nodetool help <command>'.");
    }

    protected void badUse(Exception e)
    {
        badUse(output.out::println, e);
    }

    public static void err(Consumer<String> out, Throwable e)
    {
        // CASSANDRA-11537: friendly error message when server is not ready
        if (e instanceof InstanceNotFoundException)
            throw new IllegalArgumentException("Server is not initialized yet, cannot run nodetool.");

        out.accept("error: " + e.getMessage());
        out.accept("-- StackTrace --");
        out.accept(getStackTraceAsString(e));
    }

    protected void err(Throwable e)
    {
        err(output.err::println, e);
    }

    public static class CassHelp extends Help implements NodeToolCmdRunnable
    {
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            StringBuilder sb = new StringBuilder();
            NodeToolV2 cmd = new NodeToolV2(nodeProbeFactory, output);
            if (command.isEmpty())
            {
                usage(global, cmd.getTopLevelCommandsDescription(), sb);
            }
            else
            {
                if (cmd.isCommandPresent(command))
                    cmd.execute(Stream.concat(Stream.of("help"), command.stream()).toArray(String[]::new));
                else
                    help(global, command, sb);
            }

            output.out.println(sb);
        }

        public static void usage(GlobalMetadata global, Map<String, String> extraCommands, StringBuilder sb)
        {
            UsagePrinter out = new UsagePrinter(sb, CassandraHelpLayout.DEFAULT_USAGE_HELP_WIDTH);
            List<String> commandArguments = global.getOptions().stream()
                                                  .sorted((o1, o2) -> StringUtils.compare(o1.getTitle(), o2.getTitle()))
                                                  .filter(option -> !option.isHidden())
                                                  .map(UsageHelper::toUsage)
                                                  .collect(toImmutableList());

            out.newPrinterWithHangingIndent(CassandraHelpLayout.COLUMN_INDENT)
               .append(CassandraHelpLayout.TOP_LEVEL_SYNOPSIS_LIST_PREFIX)
               .append(global.getName())
               .appendWords(commandArguments)
               .append(CassandraHelpLayout.SYNOPSIS_SUBCOMMANDS_LABEL)
               .newline()
               .newline();

            Map<String, String> commands = getCommandsDescription(global);
            // Remove the help command from the list of extra commands if exists, as it's not applicable for backward compatibility.
            extraCommands.remove("help");
            commands.putAll(extraCommands);

            out.append(CassandraHelpLayout.TOP_LEVEL_COMMAND_HEADING).newline();
            out.newIndentedPrinter(CassandraHelpLayout.SUBCOMMANDS_INDENT)
               .appendTable(commands.entrySet().stream()
                                    .map(entry -> ImmutableList.of(entry.getKey(), firstNonNull(entry.getValue(), "")))
                                    .collect(toList()));
            out.newline();
            out.append(CassandraHelpLayout.USAGE_HELP_FOOTER);
        }

        private static Map<String, String> getCommandsDescription(GlobalMetadata global)
        {
            Map<String, String> commands = new TreeMap<>();
            for (CommandMetadata meta : global.getDefaultGroupCommands())
                commands.put(meta.getName(), meta.getDescription());
            for (CommandGroupMetadata grp : global.getCommandGroups())
                commands.put(grp.getName(), grp.getDescription());
            return commands;
        }
    }

    interface NodeToolCmdRunnable
    {
        void run(INodeProbeFactory nodeProbeFactory, Output output);
    }

    public static abstract class NodeToolCmd implements NodeToolCmdRunnable
    {

        @Option(type = OptionType.GLOBAL, name = {"-h", "--host"}, description = "Node hostname or ip address")
        private String host = "127.0.0.1";

        @Option(type = OptionType.GLOBAL, name = {"-p", "--port"}, description = "Remote jmx agent port number")
        private String port = "7199";

        @Option(type = OptionType.GLOBAL, name = {"-u", "--username"}, description = "Remote jmx agent username")
        private String username = EMPTY;

        @Option(type = OptionType.GLOBAL, name = {"-pw", "--password"}, description = "Remote jmx agent password")
        private String password = EMPTY;

        @Option(type = OptionType.GLOBAL, name = {"-pwf", "--password-file"}, description = "Path to the JMX password file")
        private String passwordFilePath = EMPTY;

        @Option(type = OptionType.GLOBAL, name = { "-pp", "--print-port"}, description = "Operate in 4.0 mode with hosts disambiguated by port number", arity = 0)
        protected boolean printPort = false;

        private INodeProbeFactory nodeProbeFactory;
        protected Output output;

        @Override
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            this.nodeProbeFactory = nodeProbeFactory;
            this.output = output;
            runInternal();
        }

        public void runInternal()
        {
            if (isNotEmpty(username)) {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            try (NodeProbe probe = connect())
            {
                execute(probe);
                if (probe.isFailed())
                    throw new RuntimeException("nodetool failed, check server logs");
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error while closing JMX connection", e);
            }

        }

        public static String readUserPasswordFromFile(String username, String passwordFilePath)
        {
            String password = EMPTY;

            File passwordFile = new File(passwordFilePath);
            try (Scanner scanner = new Scanner(passwordFile.toJavaIOFile()).useDelimiter("\\s+"))
            {
                while (scanner.hasNextLine())
                {
                    if (scanner.hasNext())
                    {
                        String jmxRole = scanner.next();
                        if (jmxRole.equals(username) && scanner.hasNext())
                        {
                            password = scanner.next();
                            break;
                        }
                    }
                    scanner.nextLine();
                }
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }

            return password;
        }

        public static String promptAndReadPassword()
        {
            String password = EMPTY;

            Console console = System.console();
            if (console != null)
                password = String.valueOf(console.readPassword("Password:"));

            return password;
        }

        protected abstract void execute(NodeProbe probe);

        private NodeProbe connect()
        {
            NodeProbe nodeClient = null;

            try
            {
                if (username.isEmpty())
                    nodeClient = nodeProbeFactory.create(host, parseInt(port));
                else
                    nodeClient = nodeProbeFactory.create(host, parseInt(port), username, password);

                nodeClient.setOutput(output);
            } catch (IOException | SecurityException e)
            {
                Throwable rootCause = Throwables.getRootCause(e);
                output.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
                System.exit(1);
            }

            return nodeClient;
        }

        protected enum KeyspaceSet
        {
            ALL, NON_SYSTEM, NON_LOCAL_STRATEGY
        }

        public static List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe)
        {
            return parseOptionalKeyspace(cmdArgs, nodeProbe, KeyspaceSet.ALL);
        }

        public static List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe, KeyspaceSet defaultKeyspaceSet)
        {
            List<String> keyspaces = new ArrayList<>();


            if (cmdArgs == null || cmdArgs.isEmpty())
            {
                if (defaultKeyspaceSet == KeyspaceSet.NON_LOCAL_STRATEGY)
                    keyspaces.addAll(keyspaces = nodeProbe.getNonLocalStrategyKeyspaces());
                else if (defaultKeyspaceSet == KeyspaceSet.NON_SYSTEM)
                    keyspaces.addAll(keyspaces = nodeProbe.getNonSystemKeyspaces());
                else
                    keyspaces.addAll(nodeProbe.getKeyspaces());
            }
            else
            {
                keyspaces.add(cmdArgs.get(0));
            }

            for (String keyspace : keyspaces)
            {
                if (!nodeProbe.getKeyspaces().contains(keyspace))
                    throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }

            return Collections.unmodifiableList(keyspaces);
        }

        public static String[] parseOptionalTables(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
        }

        public static String[] parsePartitionKeys(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 2 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(2, cmdArgs.size()), String.class);
        }
    }

    public static SortedMap<String, SetHostStatWithPort> getOwnershipByDcWithPort(NodeProbe probe, boolean resolveIp,
                                                                  Map<String, String> tokenToEndpoint,
                                                                  Map<String, Float> ownerships)
    {
        SortedMap<String, SetHostStatWithPort> ownershipByDc = Maps.newTreeMap();
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        try
        {
            for (Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
            {
                String dc = epSnitchInfo.getDatacenter(tokenAndEndPoint.getValue());
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStatWithPort(resolveIp));
                ownershipByDc.get(dc).add(tokenAndEndPoint.getKey(), tokenAndEndPoint.getValue(), ownerships);
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return ownershipByDc;
    }
}
