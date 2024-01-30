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

package org.apache.cassandra.index.sai.accord.range;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.lucene.store.IndexInput;

public class GroupToCheckpointIntervals
{
    public static SegmentMetadata.ComponentMetadataMap writeCompleteSegment(IndexDescriptor indexDescriptor,
                                                                            IndexIdentifier indexIdentifier,
                                                                            Map<Group, SegmentMetadata.ComponentMetadataMap> map) throws IOException
    {
        List<Group> groups = new ArrayList<>(map.keySet());
        groups.sort(Comparator.naturalOrder());

        try (var indexOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.ACCORD_TABLES_TO_INDEX, indexIdentifier), true))
        {
            long root = indexOutput.getFilePointer();
            SAICodecUtils.writeHeader(indexOutput);
            long termsOffset = indexOutput.getFilePointer();
            var seq = indexOutput.asSequentialWriter();
            seq.writeUnsignedVInt32(groups.size());
            for (var group : groups)
            {
                seq.writeVInt32(group.storeId);
                seq.write(UUIDSerializer.instance.serialize(group.tableId.asUUID()));
                map.get(group).write(indexOutput);
            }
            long termsLength = indexOutput.getFilePointer() - termsOffset;

            // write footers/checksums
            SAICodecUtils.writeFooter(indexOutput);

            var metadataMap = new SegmentMetadata.ComponentMetadataMap();
            metadataMap.put(IndexComponent.ACCORD_TABLES_TO_INDEX, root == 0 ? -1 : root, termsOffset, termsLength, Map.of());
            return metadataMap;
        }
    }

    public static Map<Group, CheckpointIntervalArrayIndex.SegmentSearcher> read(PerColumnIndexFiles perIndexFiles, SegmentMetadata metadata) throws IOException
    {
        var pos = metadata.getIndexRoot(IndexComponent.ACCORD_TABLES_TO_INDEX);
        try (RandomAccessReader reader = perIndexFiles.accordTablesToIndex().createReader();
             IndexInput indexInput = IndexInputReader.create(reader))
        {
            SAICodecUtils.validate(indexInput);
            if (pos != -1)
                indexInput.seek(pos);

            int size = reader.readUnsignedVInt32();
            Map<Group, CheckpointIntervalArrayIndex.SegmentSearcher> tableReaders = Maps.newHashMapWithExpectedSize(size);
            byte[] uuidBuffer = new byte[8 + 8];
            for (int i = 0; i < size; i++)
            {
                int storeId = reader.readVInt32();
                indexInput.readBytes(uuidBuffer, 0, uuidBuffer.length);
                TableId table = TableId.fromUUID(UUIDSerializer.instance.deserialize(ByteBuffer.wrap(uuidBuffer)));
                Group group = new Group(storeId, table);
                SegmentMetadata.ComponentMetadataMap map = new SegmentMetadata.ComponentMetadataMap(indexInput);
                tableReaders.put(group, new CheckpointIntervalArrayIndex.SegmentSearcher(perIndexFiles.cintiaSortedList(), map.get(IndexComponent.CINTIA_SORTED_LIST).root,
                                                                                         perIndexFiles.cintiaCheckpoint(), map.get(IndexComponent.CINTIA_CHECKPOINTS).root));
            }
            return tableReaders;
        }
    }
}
