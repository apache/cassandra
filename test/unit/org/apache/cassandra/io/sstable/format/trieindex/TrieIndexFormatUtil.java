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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FilterFactory;

import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

public class TrieIndexFormatUtil
{
    static
    {
        try
        {
            File f = FileUtils.createTempFile("empty-index", "db");
            try (SequentialWriter writer = new SequentialWriter(f);
                 FileHandle.Builder fhBuilder = new FileHandle.Builder(f.getPath());
                 PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder))
            {
                builder.complete();
                partitionIndex = PartitionIndex.load(fhBuilder, Util.testPartitioner(), false);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public static final PartitionIndex partitionIndex;

    public static SSTableReader emptyReader(Descriptor desc,
                                            Set<Component> components,
                                            TableMetadataRef metadata,
                                            FileHandle ifile,
                                            FileHandle dfile)
    {
        SerializationHeader header = SerializationHeader.make(metadata.get(), Collections.emptyList());
        StatsMetadata sstableMetadata = (StatsMetadata) new MetadataCollector(metadata.get().comparator)
                                                        .finalizeMetadata(metadata.get().partitioner.getClass().getCanonicalName(), metadata.get().params.bloomFilterFpChance, UNREPAIRED_SSTABLE, null, false, header)
                                                        .get(MetadataType.STATS);
        return TrieIndexSSTableReader.internalOpen(desc, components, metadata, ifile, dfile,
                                                   partitionIndex.sharedCopy(), FilterFactory.AlwaysPresent,
                                                   1, sstableMetadata, OpenReason.NORMAL, header);
    }
}
