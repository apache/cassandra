/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.sstable;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.utils.EstimatedHistogram;

public class SSTableMetadataSerializerTest
{
    @Test
    public void testSerialization() throws IOException
    {
        EstimatedHistogram rowSizes = new EstimatedHistogram(
            new long[] { 1L, 2L },
            new long[] { 3L, 4L, 5L });
        EstimatedHistogram columnCounts = new EstimatedHistogram(
            new long[] { 6L, 7L },
            new long[] { 8L, 9L, 10L });
        ReplayPosition rp = new ReplayPosition(11L, 12);
        long maxTimestamp = 4162517136L;

        SSTableMetadata.Collector collector = SSTableMetadata.createCollector()
                                                             .estimatedRowSize(rowSizes)
                                                             .estimatedColumnCount(columnCounts)
                                                             .replayPosition(rp);
        collector.updateMaxTimestamp(maxTimestamp);
        SSTableMetadata originalMetadata = collector.finalizeMetadata(RandomPartitioner.class.getCanonicalName());

        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteOutput);

        SSTableMetadata.serializer.serialize(originalMetadata, dos);

        ByteArrayInputStream byteInput = new ByteArrayInputStream(byteOutput.toByteArray());
        DataInputStream dis = new DataInputStream(byteInput);
        Descriptor desc = new Descriptor(Descriptor.CURRENT_VERSION, new File("."), "", "", 0, false);
        SSTableMetadata stats = SSTableMetadata.serializer.deserialize(dis, desc);

        assert stats.estimatedRowSize.equals(originalMetadata.estimatedRowSize);
        assert stats.estimatedRowSize.equals(rowSizes);
        assert stats.estimatedColumnCount.equals(originalMetadata.estimatedColumnCount);
        assert stats.estimatedColumnCount.equals(columnCounts);
        assert stats.replayPosition.equals(originalMetadata.replayPosition);
        assert stats.replayPosition.equals(rp);
        assert stats.maxTimestamp == maxTimestamp;
        assert stats.maxTimestamp == originalMetadata.maxTimestamp;
        assert RandomPartitioner.class.getCanonicalName().equals(stats.partitioner);
    }
}
