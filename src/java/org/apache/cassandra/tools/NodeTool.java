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

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.*;

import io.airlift.command.*;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.nodetool.*;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.*;

public class NodeTool
{
    private static final String HISTORYFILE = "nodetool.history";

    public static void main(String... args)
    {
        List<Class<? extends Runnable>> commands = newArrayList(
                Help.class,
                Info.class,
                Ring.class,
                NetStats.class,
                CfStats.class,
                TableStats.class,
                CfHistograms.class,
                TableHistograms.class,
                Cleanup.class,
                ClearSnapshot.class,
                Compact.class,
                Scrub.class,
                Verify.class,
                Flush.class,
                UpgradeSSTable.class,
                DisableAutoCompaction.class,
                EnableAutoCompaction.class,
                CompactionStats.class,
                CompactionHistory.class,
                Decommission.class,
                DescribeCluster.class,
                DisableBinary.class,
                EnableBinary.class,
                EnableGossip.class,
                DisableGossip.class,
                EnableHandoff.class,
                EnableThrift.class,
                GcStats.class,
                GetCompactionThreshold.class,
                GetCompactionThroughput.class,
                GetTimeout.class,
                GetStreamThroughput.class,
                GetTraceProbability.class,
                GetInterDCStreamThroughput.class,
                GetEndpoints.class,
                GetSSTables.class,
                GossipInfo.class,
                InvalidateKeyCache.class,
                InvalidateRowCache.class,
                InvalidateCounterCache.class,
                Join.class,
                Move.class,
                PauseHandoff.class,
                ResumeHandoff.class,
                ProxyHistograms.class,
                Rebuild.class,
                Refresh.class,
                RemoveNode.class,
                Assassinate.class,
                Repair.class,
                ReplayBatchlog.class,
                SetCacheCapacity.class,
                SetHintedHandoffThrottleInKB.class,
                SetCompactionThreshold.class,
                SetCompactionThroughput.class,
                SetTimeout.class,
                SetStreamThroughput.class,
                SetInterDCStreamThroughput.class,
                SetTraceProbability.class,
                Snapshot.class,
                ListSnapshots.class,
                Status.class,
                StatusBinary.class,
                StatusGossip.class,
                StatusThrift.class,
                StatusBackup.class,
                StatusHandoff.class,
                Stop.class,
                StopDaemon.class,
                Version.class,
                DescribeRing.class,
                RebuildIndex.class,
                RangeKeySample.class,
                EnableBackup.class,
                DisableBackup.class,
                ResetLocalSchema.class,
                ReloadTriggers.class,
                SetCacheKeysToSave.class,
                DisableThrift.class,
                DisableHandoff.class,
                Drain.class,
                TruncateHints.class,
                TpStats.class,
                TopPartitions.class,
                SetLoggingLevel.class,
                GetLoggingLevels.class,
                DisableHintsForDC.class,
                EnableHintsForDC.class,
                FailureDetectorInfo.class,
                RefreshSizeEstimates.class,
                RelocateSSTables.class,
                ViewBuildStatus.class
        );

        Cli.CliBuilder<Runnable> builder = Cli.builder("nodetool");

        builder.withDescription("Manage your Cassandra cluster")
                 .withDefaultCommand(Help.class)
                 .withCommands(commands);

        // bootstrap commands
        builder.withGroup("bootstrap")
                .withDescription("Monitor/manage node's bootstrap process")
                .withDefaultCommand(Help.class)
                .withCommand(BootstrapResume.class);

        Cli<Runnable> parser = builder.build();

        int status = 0;
        try
        {
            Runnable parse = parser.parse(args);
            printHistory(args);
            parse.run();
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

        System.exit(status);
    }

    private static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = new FileWriter(new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE), true))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    private static void badUse(Exception e)
    {
        System.out.println("nodetool: " + e.getMessage());
        System.out.println("See 'nodetool help' or 'nodetool help <command>'.");
    }

    private static void err(Throwable e)
    {
        System.err.println("error: " + e.getMessage());
        System.err.println("-- StackTrace --");
        System.err.println(getStackTraceAsString(e));
    }

    public static abstract class NodeToolCmd implements Runnable
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

        @Override
        public void run()
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

        private String readUserPasswordFromFile(String username, String passwordFilePath) {
            String password = EMPTY;

            File passwordFile = new File(passwordFilePath);
            try (Scanner scanner = new Scanner(passwordFile).useDelimiter("\\s+"))
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
            } catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }

            return password;
        }

        private String promptAndReadPassword()
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
                    nodeClient = new NodeProbe(host, parseInt(port));
                else
                    nodeClient = new NodeProbe(host, parseInt(port), username, password);
            } catch (IOException e)
            {
                Throwable rootCause = Throwables.getRootCause(e);
                System.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
                System.exit(1);
            }

            return nodeClient;
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe)
        {
            return parseOptionalKeyspace(cmdArgs, nodeProbe, false);
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe, boolean includeSystemKS)
        {
            List<String> keyspaces = new ArrayList<>();

            if (cmdArgs == null || cmdArgs.isEmpty())
                keyspaces.addAll(includeSystemKS ? nodeProbe.getKeyspaces() : nodeProbe.getNonSystemKeyspaces());
            else
                keyspaces.add(cmdArgs.get(0));

            for (String keyspace : keyspaces)
            {
                if (!nodeProbe.getKeyspaces().contains(keyspace))
                    throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }

            return Collections.unmodifiableList(keyspaces);
        }

        protected String[] parseOptionalTables(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
        }
    }

    public static SortedMap<String, SetHostStat> getOwnershipByDc(NodeProbe probe, boolean resolveIp,
                                                                  Map<String, String> tokenToEndpoint,
                                                                  Map<InetAddress, Float> ownerships)
    {
        SortedMap<String, SetHostStat> ownershipByDc = Maps.newTreeMap();
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        try
        {
            for (Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
            {
                String dc = epSnitchInfo.getDatacenter(tokenAndEndPoint.getValue());
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStat(resolveIp));
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
