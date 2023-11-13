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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String KEY_OPTION = "k";
    private static final String DEBUG_OUTPUT_OPTION = "d";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String ENUMERATE_KEYS_OPTION = "e";
    private static final String RAW_TIMESTAMPS = "t";
    private static final String PARTITION_JSON_LINES = "l";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        DatabaseDescriptor.toolInitialization(!TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.getBoolean());

        Option optKey = new Option(KEY_OPTION, true, "List of included partition keys");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "List of excluded partition keys");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATE_KEYS_OPTION, false, "enumerate partition keys only");
        options.addOption(optEnumerate);

        Option debugOutput = new Option(DEBUG_OUTPUT_OPTION, false, "CQL row per line internal representation");
        options.addOption(debugOutput);

        Option rawTimestamps = new Option(RAW_TIMESTAMPS, false, "Print raw timestamps instead of iso8601 date strings");
        options.addOption(rawTimestamps);

        Option partitionJsonLines= new Option(PARTITION_JSON_LINES, false, "Output json lines, by partition");
        options.addOption(partitionJsonLines);
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file, export the contents of the SSTable to JSON.
     *
     * @param args
     *            command lines arguments
     * @throws ConfigurationException
     *             on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        String[] keys = cmd.getOptionValues(KEY_OPTION);
        HashSet<String> excludes = new HashSet<>(Arrays.asList(
                cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                        ? new String[0]
                        : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));

        if (cmd.getArgs().length != 1)
        {
            String msg = "You must supply exactly one sstable";
            if (cmd.getArgs().length == 0 && (keys != null && keys.length > 0 || !excludes.isEmpty()))
                msg += ", which should be before the -k/-x options so it's not interpreted as a partition key.";

            System.err.println(msg);
            printUsage();
            System.exit(1);
        }
        File ssTableFile = new File(cmd.getArgs()[0]);

        if (!ssTableFile.exists())
        {
            System.err.println("Cannot find file " + ssTableFile.absolutePath());
            System.exit(1);
        }
        Descriptor desc = Descriptor.fromFileWithComponent(ssTableFile, false).left;
        try
        {
            TableMetadata metadata = Util.metadataFromSSTable(desc);
            SSTableReader sstable = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
            if (cmd.hasOption(ENUMERATE_KEYS_OPTION))
            {
                try (KeyIterator iter = sstable.keyIterator())
                {
                    JsonTransformer.keysToJson(null, Util.iterToStream(iter),
                                               cmd.hasOption(RAW_TIMESTAMPS),
                                               metadata,
                                               System.out);
                }
            }
            else
            {
                IPartitioner partitioner = sstable.getPartitioner();
                final ISSTableScanner currentScanner;
                if ((keys != null) && (keys.length > 0))
                {
                    List<AbstractBounds<PartitionPosition>> bounds = Arrays.stream(keys)
                            .filter(key -> !excludes.contains(key))
                            .map(metadata.partitionKeyType::fromString)
                            .map(partitioner::decorateKey)
                            .sorted()
                            .map(DecoratedKey::getToken)
                            .map(token -> new Bounds<>(token.minKeyBound(), token.maxKeyBound())).collect(Collectors.toList());
                    currentScanner = sstable.getScanner(bounds.iterator());
                }
                else
                {
                    currentScanner = sstable.getScanner();
                }
                Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner).filter(i ->
                    excludes.isEmpty() || !excludes.contains(metadata.partitionKeyType.getString(i.partitionKey().getKey()))
                );
                if (cmd.hasOption(DEBUG_OUTPUT_OPTION))
                {
                    AtomicLong position = new AtomicLong();
                    partitions.forEach(partition ->
                    {
                        position.set(currentScanner.getCurrentPosition());

                        if (!partition.partitionLevelDeletion().isLive())
                        {
                            System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" +
                                               position.get() + " " + partition.partitionLevelDeletion());
                        }
                        if (!partition.staticRow().isEmpty())
                        {
                            System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" +
                                               position.get() + " " + partition.staticRow().toString(metadata, true));
                        }
                        partition.forEachRemaining(row ->
                        {
                            System.out.println(
                            "[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@"
                            + position.get() + " " + row.toString(metadata, false, true));
                            position.set(currentScanner.getCurrentPosition());
                        });
                    });
                }
                else if (cmd.hasOption(PARTITION_JSON_LINES))
                {
                    JsonTransformer.toJsonLines(currentScanner, partitions, cmd.hasOption(RAW_TIMESTAMPS), metadata, System.out);
                }
                else
                {
                    JsonTransformer.toJson(currentScanner, partitions, cmd.hasOption(RAW_TIMESTAMPS), metadata, System.out);
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }

    private static void printUsage()
    {
        String usage = String.format("sstabledump <sstable file path> <options>%n");
        String header = "Dump contents of given SSTable to standard output in JSON format.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
