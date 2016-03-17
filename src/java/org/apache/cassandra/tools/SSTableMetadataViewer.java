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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

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

        Util.initDatabaseDescriptor();

        for (String fname : args)
        {
            if (new File(fname).exists())
            {
                Descriptor descriptor = Descriptor.fromFilename(fname);
                Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
                ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
                StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
                CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);
                CompressionMetadata compression = null;
                File compressionFile = new File(descriptor.filenameFor(Component.COMPRESSION_INFO));
                if (compressionFile.exists())
                    compression = CompressionMetadata.create(fname);
                SerializationHeader.Component header = (SerializationHeader.Component) metadata.get(MetadataType.HEADER);

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
                    out.printf("Compressor: %s%n", compression != null ? compression.compressor().getClass().getName() : "-");
                    if (compression != null)
                        out.printf("Compression ratio: %s%n", stats.compressionRatio);
                    out.printf("TTL min: %s%n", stats.minTTL);
                    out.printf("TTL max: %s%n", stats.maxTTL);

                    if (validation != null && header != null)
                        printMinMaxToken(descriptor, FBUtilities.newPartitioner(validation.partitioner), header.getKeyType(), out);

                    if (header != null && header.getClusteringTypes().size() == stats.minClusteringValues.size())
                    {
                        List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                        List<ByteBuffer> minClusteringValues = stats.minClusteringValues;
                        List<ByteBuffer> maxClusteringValues = stats.maxClusteringValues;
                        String[] minValues = new String[clusteringTypes.size()];
                        String[] maxValues = new String[clusteringTypes.size()];
                        for (int i = 0; i < clusteringTypes.size(); i++)
                        {
                            minValues[i] = clusteringTypes.get(i).getString(minClusteringValues.get(i));
                            maxValues[i] = clusteringTypes.get(i).getString(maxClusteringValues.get(i));
                        }
                        out.printf("minClustringValues: %s%n", Arrays.toString(minValues));
                        out.printf("maxClustringValues: %s%n", Arrays.toString(maxValues));
                    }
                    out.printf("Estimated droppable tombstones: %s%n", stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000)));
                    out.printf("SSTable Level: %d%n", stats.sstableLevel);
                    out.printf("Repaired at: %d%n", stats.repairedAt);
                    out.println(stats.replayPosition);
                    out.printf("totalColumnsSet: %s%n", stats.totalColumnsSet);
                    out.printf("totalRows: %s%n", stats.totalRows);
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
                if (header != null)
                {
                    EncodingStats encodingStats = header.getEncodingStats();
                    AbstractType<?> keyType = header.getKeyType();
                    List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                    Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
                    Map<String, String> statics = staticColumns.entrySet().stream()
                                                               .collect(Collectors.toMap(
                                                                e -> UTF8Type.instance.getString(e.getKey()),
                                                                e -> e.getValue().toString()));
                    Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
                    Map<String, String> regulars = regularColumns.entrySet().stream()
                                                                 .collect(Collectors.toMap(
                                                                 e -> UTF8Type.instance.getString(e.getKey()),
                                                                 e -> e.getValue().toString()));

                    out.printf("EncodingStats minTTL: %s%n", encodingStats.minTTL);
                    out.printf("EncodingStats minLocalDeletionTime: %s%n", encodingStats.minLocalDeletionTime);
                    out.printf("EncodingStats minTimestamp: %s%n", encodingStats.minTimestamp);
                    out.printf("KeyType: %s%n", keyType.toString());
                    out.printf("ClusteringTypes: %s%n", clusteringTypes.toString());
                    out.printf("StaticColumns: {%s}%n", FBUtilities.toString(statics));
                    out.printf("RegularColumns: {%s}%n", FBUtilities.toString(regulars));
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

    private static void printMinMaxToken(Descriptor descriptor, IPartitioner partitioner, AbstractType<?> keyType, PrintStream out) throws IOException
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (!summariesFile.exists())
            return;

        try (DataInputStream iStream = new DataInputStream(new FileInputStream(summariesFile)))
        {
            Pair<DecoratedKey, DecoratedKey> firstLast = new IndexSummary.IndexSummarySerializer().deserializeFirstLastKey(iStream, partitioner, descriptor.version.hasSamplingLevel());
            out.printf("First token: %s (key=%s)%n", firstLast.left.getToken(), keyType.getString(firstLast.left.getKey()));
            out.printf("Last token: %s (key=%s)%n", firstLast.right.getToken(), keyType.getString(firstLast.right.getKey()));
        }
    }

}
