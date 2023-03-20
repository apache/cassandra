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
import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;

/**
 * Shows the contents of sstable metadata
 */
public class SSTableMetadataViewer
{
    private static final String GCGS_KEY = "gc_grace_seconds";

    /**
     * @param args a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        Option optGcgs = new Option(null, GCGS_KEY, true, "The "+GCGS_KEY+" to use when calculating droppable tombstones");

        Options options = new Options();
        options.addOption(optGcgs);
        CommandLine cmd = null;
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            printHelp(options, out);
        }

        if (cmd.getArgs().length == 0)
        {
            printHelp(options, out);
        }
        int gcgs = Integer.parseInt(cmd.getOptionValue(GCGS_KEY, "0"));
        Util.initDatabaseDescriptor();

        for (String fname : cmd.getArgs())
        {
            if (new File(fname).exists())
            {
                Descriptor descriptor = Descriptor.fromFilename(fname);
                Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
                ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
                StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
                CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);

                out.printf("SSTable: %s%n", descriptor);
                if (validation != null)
                {
                    out.printf("Partitioner: %s%n", validation.partitioner);
                    out.printf("Bloom Filter FP chance: %f%n", validation.bloomFilterFPChance);
                }
                if (stats != null)
                {
                    out.printf("Minimum timestamp: %s%n", stats.minTimestamp);
                    out.printf("Maximum timestamp: %s%n", stats.maxTimestamp);
                    out.printf("SSTable min local deletion time: %s%n", stats.minLocalDeletionTime);
                    out.printf("SSTable max local deletion time: %s%n", stats.maxLocalDeletionTime);
                    out.printf("Compression ratio: %s%n", stats.compressionRatio);
                    out.printf("Estimated droppable tombstones: %s%n", stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000) - gcgs));
                    out.printf("SSTable Level: %d%n", stats.sstableLevel);
                    out.printf("Repaired at: %d%n", stats.repairedAt);
                    out.printf("Originating host id: %s%n", stats.originatingHostId);
                    out.printf("Replay positions covered: %s\n", stats.commitLogIntervals);
                    out.println("Estimated tombstone drop times:");
                    for (Map.Entry<Double, Long> entry : stats.estimatedTombstoneDropTime.getAsMap().entrySet())
                    {
                        out.printf("%-10s:%10s%n",entry.getKey().intValue(), entry.getValue());
                    }
                    printHistograms(stats, out);
                }
                if (compaction != null)
                {
                    out.printf("Estimated cardinality: %s%n", compaction.cardinalityEstimator.cardinality());
                }
            }
            else
            {
                out.println("No such file: " + fname);
            }
        }
    }

    private static void printHelp(Options options, PrintStream out)
    {
        out.println();
        new HelpFormatter().printHelp("Usage: sstablemetadata [--"+GCGS_KEY+" n] <sstable filenames>", "Dump contents of given SSTable to standard output in JSON format.", options, "");
        System.exit(1);
    }

    private static void printHistograms(StatsMetadata metadata, PrintStream out)
    {
        long[] offsets = metadata.estimatedPartitionSize.getBucketOffsets();
        long[] ersh = metadata.estimatedPartitionSize.getBuckets(false);
        long[] ecch = metadata.estimatedColumnCount.getBuckets(false);

        out.println(String.format("%-10s%18s%18s",
                                  "Count", "Row Size", "Cell Count"));

        for (int i = 0; i < offsets.length; i++)
        {
            out.println(String.format("%-10d%18s%18s",
                                      offsets[i],
                                      (i < ersh.length ? ersh[i] : ""),
                                      (i < ecch.length ? ecch[i] : "")));
        }
    }
}
