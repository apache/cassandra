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

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.*;

/**
 * Shows the contents of sstable metadata
 */
public class SSTableMetadataViewer
{
    /**
     * @param args a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("Usage: sstablemetadata <sstable filenames>");
            System.exit(1);
        }

        for (String fname : args)
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
                    out.printf("SSTable max local deletion time: %s%n", stats.maxLocalDeletionTime);
                    out.printf("Compression ratio: %s%n", stats.compressionRatio);
                    out.printf("Estimated droppable tombstones: %s%n", stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000)));
                    out.printf("SSTable Level: %d%n", stats.sstableLevel);
                    out.printf("Repaired at: %d%n", stats.repairedAt);
                    out.println(stats.replayPosition);
                    out.println("Estimated tombstone drop times:%n");
                    for (Map.Entry<Double, Long> entry : stats.estimatedTombstoneDropTime.getAsMap().entrySet())
                    {
                        out.printf("%-10s:%10s%n",entry.getKey().intValue(), entry.getValue());
                    }
                    printHistograms(stats, out);
                }
                if (compaction != null)
                {
                    out.printf("Ancestors: %s%n", compaction.ancestors.toString());
                    out.printf("Estimated cardinality: %s%n", compaction.cardinalityEstimator.cardinality());

                }
            }
            else
            {
                out.println("No such file: " + fname);
            }
        }
    }

    private static void printHistograms(StatsMetadata metadata, PrintStream out)
    {
        long[] offsets = metadata.estimatedRowSize.getBucketOffsets();
        long[] ersh = metadata.estimatedRowSize.getBuckets(false);
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
