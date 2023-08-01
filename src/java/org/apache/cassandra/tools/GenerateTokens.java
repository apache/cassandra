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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocator;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

public class GenerateTokens
{
    private static final String RF = "rf";
    private static final String TOKENS = "tokens";
    private static final String NODES = "nodes";
    private static final String PARTITIONER = "partitioner";
    private static final String RACKS = "racks";
    private static final String VERBOSE = "verbose";

    public static void main(String[] args)
    {
        Options options = null;
        int rf = 0;
        int tokens = 0;
        int nodes = 0;
        IPartitioner partitioner = null;
        int[] racksDef = null;

        OutputHandler logger = null;

        try
        {
            // disable the summary statistics logging, since this is a command-line tool with dedicated output
            ((Logger) LoggerFactory.getLogger(TokenAllocation.class)).setLevel(Level.ERROR);

            Util.initDatabaseDescriptor();
            options = getOptions();
            CommandLine cmd = parseCommandLine(args, options);

            rf = Integer.parseInt(cmd.getOptionValue(RF));
            tokens = Integer.parseInt(cmd.getOptionValue(TOKENS));
            nodes = Integer.parseInt(cmd.getOptionValue(NODES));
            logger = new OutputHandler.SystemOutput(cmd.hasOption(VERBOSE), true, true);

            partitioner = FBUtilities.newPartitioner(cmd.getOptionValue(PARTITIONER, Murmur3Partitioner.class.getSimpleName()));
            racksDef = getRacks(cmd.getOptionValue(RACKS, cmd.getOptionValue(NODES)));
            if (Arrays.stream(racksDef).sum() != nodes)
            {
                throw new AssertionError(String.format("The sum of nodes in each rack %s must equal total node count %s.",
                                                       cmd.getOptionValue(RACKS),
                                                       cmd.getOptionValue(NODES)));
            }
        }
        catch (NumberFormatException e)
        {
            System.err.println("Invalid integer " + e.getMessage());
            System.out.println();
            printUsage(options);
            System.exit(1);
        }
        catch (AssertionError | ConfigurationException | ParseException t)
        {
            System.err.println(t.getMessage());
            System.out.println();
            printUsage(options);
            System.exit(1);
        }

        try
        {
            logger.output(String.format("Generating tokens for %d nodes with %d vnodes each for replication factor %d and partitioner %s",
                                             nodes, tokens, rf, partitioner.getClass().getSimpleName()));

            for (OfflineTokenAllocator.FakeNode node : OfflineTokenAllocator.allocate(rf, tokens, racksDef, logger, partitioner))
                logger.output(String.format("Node %d rack %d: %s", node.nodeId(), node.rackId(), node.tokens().toString()));
        }
        catch (Throwable t)
        {
            logger.warn(t, "Error running tool.");
            System.exit(1);
        }
    }

    private static int[] getRacks(String racksDef)
    {
        return Arrays.stream(racksDef.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    private static CommandLine parseCommandLine(String[] args, Options options) throws ParseException
    {
        return new GnuParser().parse(options, args, false);
    }

    private static Options getOptions()
    {
        Options options = new Options();
        options.addOption(requiredOption("n", NODES, true, "Number of nodes."));
        options.addOption(requiredOption("t", TOKENS, true, "Number of tokens/vnodes per node."));
        options.addOption(requiredOption(null, RF, true, "Replication factor."));
        options.addOption("p", PARTITIONER, true, "Database partitioner, either Murmur3Partitioner or RandomPartitioner.");
        options.addOption(null, RACKS, true,
                          "Number of nodes per rack, separated by commas. Must add up to the total node count.\n" +
                          "For example, 'generatetokens -n 30 -t 8 --rf 3 --racks 10,10,10' will generate tokens for\n" +
                          "three racks of 10 nodes each.");
        options.addOption("v", VERBOSE, false, "Verbose logging.");
        return options;
    }

    private static Option requiredOption(String shortOpt, String longOpt, boolean hasArg, String description)
    {
        Option option = new Option(shortOpt, longOpt, hasArg, description);
        option.setRequired(true);
        return option;
    }

    public static void printUsage(Options options)
    {
        String usage = "generatetokens -n NODES -t TOKENS --rf REPLICATION_FACTOR";
        String header = "--\n" +
                        "Generates tokens for a datacenter with the given number of nodes using the token allocation algorithm.\n" +
                        "Options are:";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}