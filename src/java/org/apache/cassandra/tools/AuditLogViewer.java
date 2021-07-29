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

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.cassandra.io.util.File;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import org.apache.cassandra.audit.BinAuditLogger;
import org.apache.cassandra.utils.binlog.BinLog;

/**
 * Tool to view the contenst of AuditLog files in human readable format. Default implementation for AuditLog files
 * logs audit messages in {@link org.apache.cassandra.utils.binlog.BinLog} format, this tool prints the contens of
 * binary audit log files in text format.
 */
public class AuditLogViewer
{
    private static final String TOOL_NAME = "auditlogviewer";
    private static final String ROLL_CYCLE = "roll_cycle";
    private static final String FOLLOW = "follow";
    private static final String IGNORE = "ignore";
    private static final String HELP_OPTION = "help";

    public static void main(String[] args)
    {
        AuditLogViewerOptions options = AuditLogViewerOptions.parseArgs(args);

        try
        {
            dump(options.pathList, options.rollCycle, options.follow, options.ignoreUnsupported, System.out::print);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    static void dump(List<String> pathList, String rollCycle, boolean follow, boolean ignoreUnsupported, Consumer<String> displayFun)
    {
        //Backoff strategy for spinning on the queue, not aggressive at all as this doesn't need to be low latency
        Pauser pauser = Pauser.millis(100);
        List<ExcerptTailer> tailers = pathList.stream()
                                              .distinct()
                                              .map(path -> SingleChronicleQueueBuilder.single(new File(path).toJavaIOFile()).readOnly(true).rollCycle(RollCycles.valueOf(rollCycle)).build())
                                              .map(SingleChronicleQueue::createTailer)
                                              .collect(Collectors.toList());
        boolean hadWork = true;
        while (hadWork)
        {
            hadWork = false;
            for (ExcerptTailer tailer : tailers)
            {
                while (tailer.readDocument(new DisplayRecord(ignoreUnsupported, displayFun)))
                {
                    hadWork = true;
                }
            }

            if (follow)
            {
                if (!hadWork)
                {
                    //Chronicle queue doesn't support blocking so use this backoff strategy
                    pauser.pause();
                }
                //Don't terminate the loop even if there wasn't work
                hadWork = true;
            }
        }
    }

    private static class DisplayRecord implements ReadMarshallable
    {
        private final boolean ignoreUnsupported;
        private final Consumer<String> displayFun;

        DisplayRecord(boolean ignoreUnsupported, Consumer<String> displayFun)
        {
            this.ignoreUnsupported = ignoreUnsupported;
            this.displayFun = displayFun;
        }

        public void readMarshallable(WireIn wireIn) throws IORuntimeException
        {
            int version = wireIn.read(BinLog.VERSION).int16();
            if (!isSupportedVersion(version))
            {
                return;
            }
            String type = wireIn.read(BinLog.TYPE).text();
            if (!isSupportedType(type))
            {
                return;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Type: ")
              .append(type)
              .append(System.lineSeparator())
              .append("LogMessage: ")
              .append(wireIn.read(BinAuditLogger.AUDITLOG_MESSAGE).text())
              .append(System.lineSeparator());

            displayFun.accept(sb.toString());
        }

        private boolean isSupportedVersion(int version)
        {
            if (version <= BinAuditLogger.CURRENT_VERSION)
            {
                return true;
            }

            if (ignoreUnsupported)
            {
                return false;
            }

            throw new IORuntimeException("Unsupported record version [" + version
                                         + "] - highest supported version is [" + BinAuditLogger.CURRENT_VERSION + ']');
        }

        private boolean isSupportedType(String type)
        {
            if (BinAuditLogger.AUDITLOG_TYPE.equals(type))
            {
                return true;
            }

            if (ignoreUnsupported)
            {
                return false;
            }

            throw new IORuntimeException("Unsupported record type field [" + type
                                         + "] - supported type is [" + BinAuditLogger.AUDITLOG_TYPE + ']');
        }
    }

    private static class AuditLogViewerOptions
    {
        private final List<String> pathList;
        private String rollCycle = "HOURLY";
        private boolean follow;
        private boolean ignoreUnsupported;

        private AuditLogViewerOptions(String[] pathList)
        {
            this.pathList = Arrays.asList(pathList);
        }

        static AuditLogViewerOptions parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            Options options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length <= 0)
                {
                    System.err.println("Audit log files directory path is a required argument.");
                    printUsage(options);
                    System.exit(1);
                }

                AuditLogViewerOptions opts = new AuditLogViewerOptions(args);

                opts.follow = cmd.hasOption(FOLLOW);

                opts.ignoreUnsupported = cmd.hasOption(IGNORE);

                if (cmd.hasOption(ROLL_CYCLE))
                {
                    opts.rollCycle = cmd.getOptionValue(ROLL_CYCLE);
                }

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        static void errorMsg(String msg, Options options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        static Options getCmdLineOptions()
        {
            Options options = new Options();

            options.addOption(new Option("r", ROLL_CYCLE, true, "How often to roll the log file was rolled. May be necessary for Chronicle to correctly parse file names. (MINUTELY, HOURLY, DAILY). Default HOURLY."));
            options.addOption(new Option("f", FOLLOW, false, "Upon reacahing the end of the log continue indefinitely waiting for more records"));
            options.addOption(new Option("i", IGNORE, false, "Silently ignore unsupported records"));
            options.addOption(new Option("h", HELP_OPTION, false, "display this help message"));

            return options;
        }

        static void printUsage(Options options)
        {
            String usage = String.format("%s <path1> [<path2>...<pathN>] [options]", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("View the audit log contents in human readable format");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
