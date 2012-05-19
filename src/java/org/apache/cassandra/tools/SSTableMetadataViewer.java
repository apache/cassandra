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
import java.io.PrintStream;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;

/**
 * Shows the contents of sstable metadata
 */
public class SSTableMetadataViewer
{
    /**
     * @param args a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException, ConfigurationException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("Usage: sstablemetadata <sstable filenames>");
            System.exit(1);
        }

        for (String fname : args)
        {
            Descriptor descriptor = Descriptor.fromFilename(fname);
            SSTableMetadata metadata = SSTableMetadata.serializer.deserialize(descriptor);

            out.printf("SSTable: %s%n", descriptor);
            out.printf("Partitioner: %s%n", metadata.partitioner);
            out.printf("Maximum timestamp: %s%n", metadata.maxTimestamp);
            out.printf("Compression ratio: %s%n", metadata.compressionRatio);
            out.printf("Estimated droppable tombstones: %s%n", metadata.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000)));
            out.println(metadata.replayPosition);
            printHistograms(metadata, out);
        }
    }

    private static void printHistograms(SSTableMetadata metadata, PrintStream out)
    {
        long[] offsets = metadata.estimatedRowSize.getBucketOffsets();
        long[] ersh = metadata.estimatedRowSize.getBuckets(false);
        long[] ecch = metadata.estimatedColumnCount.getBuckets(false);

        out.println(String.format("%-10s%18s%18s",
                                  "Count", "Row Size", "Column Count"));

        for (int i = 0; i < offsets.length; i++)
        {
            out.println(String.format("%-10d%18s%18s",
                                      offsets[i],
                                      (i < ersh.length ? ersh[i] : ""),
                                      (i < ecch.length ? ecch[i] : "")));
        }
    }
}
