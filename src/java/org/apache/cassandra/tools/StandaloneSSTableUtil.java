/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.tools;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneSSTableUtil
{
    private static final String TOOL_NAME = "sstableutil";
    private static final String TYPE_OPTION  = "type";
    private static final String OP_LOG_OPTION  = "oplog";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String CLEANUP_OPTION = "cleanup";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            CFMetaData metadata = Schema.instance.getCFMetaData(options.keyspaceName, options.cfName);
            if (metadata == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspaceName,
                                                                 options.cfName));

            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);

            if (options.cleanup)
            {
                handler.output("Cleanuping up...");
                LifecycleTransaction.removeUnfinishedLeftovers(metadata);
            }
            else
            {
                handler.output("Listing files...");
                listFiles(options, metadata, handler);
            }

            System.exit(0);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void listFiles(Options options, CFMetaData metadata, OutputHandler handler) throws IOException
    {
        Directories directories = new Directories(metadata);

        for (File dir : directories.getCFDirectories())
        {
            for (File file : LifecycleTransaction.getFiles(dir.toPath(), getFilter(options), Directories.OnTxnErr.THROW))
                handler.output(file.getCanonicalPath());
        }
    }

    private static BiFunction<File, Directories.FileType, Boolean> getFilter(Options options)
    {
        return (file, type) ->
        {
            switch(type)
            {
                case FINAL:
                    return options.type != Options.FileType.TMP;
                case TEMPORARY:
                    return options.type != Options.FileType.FINAL;
                case TXN_LOG:
                    return options.oplogs;
                default:
                    throw new AssertionError();
            }
        };
    }

    private static class Options
    {
        public enum FileType
        {
            ALL("all", "list all files, final or temporary"),
            TMP("tmp", "list temporary files only"),
            FINAL("final", "list final files only");

            public String option;
            public String descr;
            FileType(String option, String descr)
            {
                this.option = option;
                this.descr = descr;
            }

            static FileType fromOption(String option)
            {
                for (FileType fileType : FileType.values())
                {
                    if (fileType.option.equals(option))
                        return fileType;
                }

                return FileType.ALL;
            }

            static String descr()
            {
                StringBuilder str = new StringBuilder();
                for (FileType fileType : FileType.values())
                {
                    str.append(fileType.option);
                    str.append(" (");
                    str.append(fileType.descr);
                    str.append("), ");
                }
                return str.toString();
            }
        }

        public final String keyspaceName;
        public final String cfName;

        public boolean debug;
        public boolean verbose;
        public boolean oplogs;
        public boolean cleanup;
        public FileType type;

        private Options(String keyspaceName, String cfName)
        {
            this.keyspaceName = keyspaceName;
            this.cfName = cfName;
        }

        public static Options parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length != 2)
                {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    System.err.println(msg);
                    printUsage(options);
                    System.exit(1);
                }

                String keyspaceName = args[0];
                String cfName = args[1];

                Options opts = new Options(keyspaceName, cfName);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.type = FileType.fromOption(cmd.getOptionValue(TYPE_OPTION));
                opts.oplogs = cmd.hasOption(OP_LOG_OPTION);
                opts.cleanup = cmd.hasOption(CLEANUP_OPTION);

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static void errorMsg(String msg, CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static CmdLineOptions getCmdLineOptions()
        {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption("c", CLEANUP_OPTION, "clean-up any outstanding transactions");
            options.addOption("d", DEBUG_OPTION, "display stack traces");
            options.addOption("h", HELP_OPTION, "display this help message");
            options.addOption("o", OP_LOG_OPTION, "include operation logs");
            options.addOption("t", TYPE_OPTION, true, FileType.descr());
            options.addOption("v", VERBOSE_OPTION, "verbose output");

            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <column_family>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("List sstable files for the provided table." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
