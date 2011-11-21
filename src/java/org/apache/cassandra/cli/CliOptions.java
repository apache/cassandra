/**
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
package org.apache.cassandra.cli;

import org.apache.commons.cli.*;

/**
 *
 * Used to process, and act upon the arguments passed to the CLI.
 *
 */
public class CliOptions
{
    private static CLIOptions options = null; // Info about command line options

    // Name of the command line tool (used for error messages)
    private static final String TOOL_NAME = "cassandra-cli";

    // Command line options
    private static final String HOST_OPTION = "host";
    private static final String PORT_OPTION = "port";
    private static final String UNFRAME_OPTION = "unframed";
    private static final String DEBUG_OPTION = "debug";
    private static final String USERNAME_OPTION = "username";
    private static final String PASSWORD_OPTION = "password";
    private static final String KEYSPACE_OPTION = "keyspace";
    private static final String BATCH_OPTION = "batch";
    private static final String HELP_OPTION = "help";
    private static final String FILE_OPTION = "file";
    private static final String JMX_PORT_OPTION = "jmxport";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String SCHEMA_MIGRATION_WAIT_TIME = "schema-mwt";

    // Default values for optional command line arguments
    private static final int    DEFAULT_THRIFT_PORT = 9160;

    // Register the command line options and their properties (such as
    // whether they take an extra argument, etc.
    static
    {
        options = new CLIOptions();

        options.addOption("h",  HOST_OPTION,     "HOSTNAME", "cassandra server's host name");
        options.addOption("p",  PORT_OPTION,     "PORT",     "cassandra server's thrift port");
        options.addOption("u",  USERNAME_OPTION, "USERNAME", "user name for cassandra authentication");
        options.addOption("pw", PASSWORD_OPTION, "PASSWORD", "password for cassandra authentication");
        options.addOption("k",  KEYSPACE_OPTION, "KEYSPACE", "cassandra keyspace user is authenticated against");
        options.addOption("f",  FILE_OPTION,     "FILENAME", "load statements from the specific file");
        options.addOption(null, JMX_PORT_OPTION, "JMX-PORT", "JMX service port");
        options.addOption(null, SCHEMA_MIGRATION_WAIT_TIME,  "TIME", "Schema migration wait time (secs.), default is 10 secs");

        // options without argument
        options.addOption("B",  BATCH_OPTION,   "enabled batch mode (suppress output; errors are fatal)");
        options.addOption(null, UNFRAME_OPTION, "use cassandra server's unframed transport");
        options.addOption(null, DEBUG_OPTION,   "display stack-traces (NOTE: We print strack-traces in the places where it makes sense even without --debug)");
        options.addOption("?",  HELP_OPTION,    "usage help");
        options.addOption("v",  VERBOSE_OPTION, "verbose output when using batch mode");
    }

    private static void printUsage()
    {
        new HelpFormatter().printHelp(TOOL_NAME, options);
    }

    public void processArgs(CliSessionState css, String[] args)
    {
        CommandLineParser parser = new GnuParser();

        try
        {
            CommandLine cmd = parser.parse(options, args, false);

            if (cmd.hasOption(HOST_OPTION))
            {
                css.hostName = cmd.getOptionValue(HOST_OPTION);
            }
            else
            {
                // host name not specified in command line.
                // In this case, we don't implicitly connect at CLI startup. In this case,
                // the user must use the "connect" CLI statement to connect.
                css.hostName = null;
            }

            // Look to see if frame has been specified
            if (cmd.hasOption(UNFRAME_OPTION))
            {
                css.framed = false;
            }

            // Look to see if frame has been specified
            if (cmd.hasOption(DEBUG_OPTION))
            {
                css.debug = true;
            }

            // Look for optional args.
            if (cmd.hasOption(PORT_OPTION))
            {
                css.thriftPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
            }
            else
            {
                css.thriftPort = DEFAULT_THRIFT_PORT;
            }

            // Look for authentication credentials (username and password)
            if (cmd.hasOption(USERNAME_OPTION))
            {
            	css.username = cmd.getOptionValue(USERNAME_OPTION);
            }
            else
            {
                css.username = "default";
            }

            if (cmd.hasOption(PASSWORD_OPTION))
            {
            	css.password = cmd.getOptionValue(PASSWORD_OPTION);
            }
            else
            {
                css.password = "";
            }

            // Look for keyspace
            if (cmd.hasOption(KEYSPACE_OPTION))
            {
            	css.keyspace = cmd.getOptionValue(KEYSPACE_OPTION);
            }

            if (cmd.hasOption(BATCH_OPTION))
            {
                css.batch = true;
            }

            if (cmd.hasOption(FILE_OPTION))
            {
                css.filename = cmd.getOptionValue(FILE_OPTION);
            }

            if (cmd.hasOption(JMX_PORT_OPTION))
            {
                css.jmxPort = Integer.parseInt(cmd.getOptionValue(JMX_PORT_OPTION));
            }

            if (cmd.hasOption(HELP_OPTION))
            {
                printUsage();
                System.exit(1);
            }

            if (cmd.hasOption(VERBOSE_OPTION))
            {
                css.verbose = true;
            }

            if (cmd.hasOption(SCHEMA_MIGRATION_WAIT_TIME))
            {
                css.schema_mwt = Integer.parseInt(cmd.getOptionValue(SCHEMA_MIGRATION_WAIT_TIME)) * 1000;
            }
            else
            {
                css.schema_mwt = 10 * 1000;
            }

            // Abort if there are any unrecognized arguments left
            if (cmd.getArgs().length > 0)
            {
                System.err.printf("Unknown argument: %s\n", cmd.getArgs()[0]);
                System.err.println();
                printUsage();
                System.exit(1);
            }
        }
        catch (ParseException e)
        {
            System.err.println(e.getMessage());
            System.err.println();
            printUsage();
            System.exit(1);
        }
    }

    private static class CLIOptions extends Options
    {
        /**
         * Add option with argument and argument name
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }

        /**
         * Add option without argument
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description)
        {
            return addOption(new Option(opt, longOpt, false, description));
        }
    }

}
