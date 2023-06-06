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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.io.util.File;
import org.mindrot.jbcrypt.BCrypt;

public class HashPassword
{
    private static final String LOGROUNDS_OPTION = "logrounds";
    private static final String HELP_OPTION = "help";
    private static final String ENV_VAR = "environment-var";
    private static final String PLAIN = "plain";
    private static final String INPUT = "input";

    private static final int LOGROUNDS_DEFAULT = 10;
    private static final int MIN_PASS_LENGTH = 4;

    public static void main(String[] args)
    {
        try
        {
            Options options = getOptions();
            CommandLine cmd = parseCommandLine(args, options);

            String password = null;
            if (cmd.hasOption(ENV_VAR))
            {
                password = System.getenv(cmd.getOptionValue(ENV_VAR)); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
                if (password == null)
                {
                    System.err.println(String.format("Environment variable '%s' is undefined.", cmd.getOptionValue(ENV_VAR)));
                    System.exit(1);
                }
            }
            else if (cmd.hasOption(PLAIN))
            {
                password = cmd.getOptionValue(PLAIN);
            }
            else if (cmd.hasOption(INPUT))
            {
                String input = cmd.getOptionValue(INPUT);
                byte[] fileInput = null;
                if ("-".equals(input))
                {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    int rd;
                    while ((rd = System.in.read()) != -1)
                        os.write(rd);
                    fileInput = os.toByteArray();
                }
                else
                {
                    try
                    {
                        Path file = File.getPath(input);
                        fileInput = Files.readAllBytes(file);
                    }
                    catch (IOException e)
                    {
                        System.err.printf("Failed to read from '%s': %s%n", input, e);
                        System.exit(1);
                    }
                }
                password = new String(fileInput, StandardCharsets.UTF_8);
            }
            else
            {
                System.err.println(String.format("One of the options --%s, --%s or --%s must be used.",
                                                 ENV_VAR, PLAIN, INPUT));
                printUsage(options);
                System.exit(1);
            }

            if (password.chars().anyMatch(i -> i < 32))
                System.err.println("WARNING: The provided plain text password contains non-printable characters (ASCII<32).");

            if (password.length() < MIN_PASS_LENGTH)
                System.err.println("WARNING: The provided password is very short, probably too short to be secure.");

            int logRounds = cmd.hasOption(LOGROUNDS_OPTION) ? Integer.parseInt(cmd.getOptionValue(LOGROUNDS_OPTION)) : LOGROUNDS_DEFAULT;
            if (logRounds < 4 || logRounds > 30)
            {
                System.err.println(String.format("Bad value for --%s %d. " +
                                                 "Please use a value between 4 and 30 inclusively",
                        LOGROUNDS_OPTION, logRounds));
                System.exit(1);
            }

            // The number of rounds is in fact = 2^rounds.
            if (logRounds > 16)
                System.err.println(String.format("WARNING: Using a high number of hash rounds, as configured using '--%s %d' " +
                                                 "will consume a lot of CPU and likely cause timeouts. Note that the parameter defines the " +
                                                 "logarithmic number of rounds: %d becomes 2^%d = %d rounds",
                        LOGROUNDS_OPTION, logRounds,
                        logRounds, logRounds, 1 << logRounds));

            if (password.getBytes().length > 72)
                System.err.println(String.format("WARNING: The provided password has a length of %d bytes, but the underlying hash/crypt algorithm " +
                        "(bcrypt) can only compare up to 72 bytes. The password will be accepted and work, but only compared up to 72 bytes.",
                        password.getBytes().length));

            String hashed = escape(hashpw(password, logRounds));
            System.out.print(hashed);
            System.out.flush();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static CommandLine parseCommandLine(String[] args, Options options) throws ParseException
    {
        CommandLineParser parser = new GnuParser();

        CommandLine cmd = parser.parse(options, args, false);

        if (cmd.hasOption(HELP_OPTION))
        {
            printUsage(options);
            System.exit(0);
        }
        return cmd;
    }

    private static Options getOptions()
    {
        Options options = new Options();
        options.addOption("h", HELP_OPTION, false, "Display this help message");
        options.addOption("r", LOGROUNDS_OPTION, true, "Number of hash rounds (default: " + LOGROUNDS_DEFAULT + ").");
        OptionGroup group = new OptionGroup();
        group.addOption(new Option("e", ENV_VAR, true,
                                   "Use value of the specified environment variable as the password"));
        group.addOption(new Option("p", PLAIN, true,
                                   "Argument is the plain text password"));
        group.addOption(new Option("i", INPUT, true,
                                   "Input is a file (or - for stdin) to read the password from. " +
                                   "Make sure that the whole input including newlines is considered. " +
                                   "For example, the shell command 'echo -n foobar | hash_password -i -' will " +
                                   "work as intended and just hash 'foobar'."));
        options.addOptionGroup(group);
        return options;
    }

    private static String hashpw(String password, int rounds)
    {
        return BCrypt.hashpw(password, BCrypt.gensalt(rounds));
    }

    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    public static void printUsage(Options options)
    {
        String usage = "hash_password [options]";
        String header = "--\n" +
                        "Hashes a plain text password and prints the hashed password.\n" +
                        "Options are:";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
