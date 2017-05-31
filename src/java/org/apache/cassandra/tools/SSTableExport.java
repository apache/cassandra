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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.cli.*;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{

    private static final String KEY_OPTION = "k";
    private static final String DEBUG_OUTPUT_OPTION = "d";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String ENUMERATE_KEYS_OPTION = "e";
    private static final String RAW_TIMESTAMPS = "t";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        DatabaseDescriptor.toolInitialization();

        Option optKey = new Option(KEY_OPTION, true, "Partition key");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Excluded partition key");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATE_KEYS_OPTION, false, "enumerate partition keys only");
        options.addOption(optEnumerate);

        Option debugOutput = new Option(DEBUG_OUTPUT_OPTION, false, "CQL row per line internal representation");
        options.addOption(debugOutput);

        Option rawTimestamps = new Option(RAW_TIMESTAMPS, false, "Print raw timestamps instead of iso8601 date strings");
        options.addOption(rawTimestamps);
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored TableMetadata
     * @throws IOException when Stats.db cannot be read
     */
    public static TableMetadata metadataFromSSTable(Descriptor desc) throws IOException
    {
        if (!desc.version.isCompatible())
            throw new IOException("Cannot process old and unsupported SSTable version.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        TableMetadata.Builder builder = TableMetadata.builder("keyspace", "table").partitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKeyColumn("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    private static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
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

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            printUsage();
            System.exit(1);
        }

        String[] keys = cmd.getOptionValues(KEY_OPTION);
        HashSet<String> excludes = new HashSet<>(Arrays.asList(
                cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                        ? new String[0]
                        : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));
        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        if (!new File(ssTableFileName).exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }
        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        try
        {
            TableMetadata metadata = metadataFromSSTable(desc);
            if (cmd.hasOption(ENUMERATE_KEYS_OPTION))
            {
                try (KeyIterator iter = new KeyIterator(desc, metadata))
                {
                    JsonTransformer.keysToJson(null, iterToStream(iter),
                                               cmd.hasOption(RAW_TIMESTAMPS),
                                               metadata,
                                               System.out);
                }
            }
            else
            {
                SSTableReader sstable = SSTableReader.openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));
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
                Stream<UnfilteredRowIterator> partitions = iterToStream(currentScanner).filter(i ->
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
                else
                {
                    JsonTransformer.toJson(currentScanner, partitions, cmd.hasOption(RAW_TIMESTAMPS), metadata, System.out);
                }
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }

    private static void printUsage()
    {
        String usage = String.format("sstabledump <options> <sstable file path>%n");
        String header = "Dump contents of given SSTable to standard output in JSON format.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
