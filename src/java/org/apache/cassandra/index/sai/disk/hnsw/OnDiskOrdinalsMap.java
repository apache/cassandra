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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Pair;

class OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskOrdinalsMap.class);

    private final RandomAccessReader reader;
    private final int size;
    private final long rowOrdinalOffset;

    public OnDiskOrdinalsMap(File file) throws IOException
    {
        this.reader = RandomAccessReader.open(file);
        this.size = reader.readInt();
        reader.seek(reader.length() - 8);
        this.rowOrdinalOffset = reader.readLong();
    }

    public int[] getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
    {
        Preconditions.checkArgument(vectorOrdinal < size, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, size);

        // read index entry
        reader.seek(4L + vectorOrdinal * 8L);
        var offset = reader.readLong();
        // seek to and read ordinals
        reader.seek(offset);
        var postingsSize = reader.readInt();
        var ordinals = new int[postingsSize];
        for (var i = 0; i < ordinals.length; i++)
        {
            ordinals[i] = reader.readInt();
        }
        return ordinals;
    }

    public int getOrdinalForRowId(int rowId) throws IOException
    {
        // Compute the offset of the start of the rowId to vectorOrdinal mapping
        var high = (reader.length() - 8 - rowOrdinalOffset) / 8;
        DiskBinarySearch.searchInt(0, Math.toIntExact(high), rowId, i -> {
            try
            {
                long offset = rowOrdinalOffset + i * 8;
                reader.seek(offset);
                return reader.readInt();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        return reader.readInt();
    }

    public void close()
    {
        reader.close();
    }

    static void writeOrdinalToRowMapping(CassandraOnHeapHnsw graph, File file, Map<PrimaryKey, Integer> keyToRowId) throws IOException
    {
        Preconditions.checkState(keyToRowId.size() == graph.rowCount(),
                                 "Expected %s rows, but found %s", keyToRowId.size(), graph.rowCount());
        Preconditions.checkState(graph.postingsMap.size() == graph.vectorValues.size(),
                                 "Postings entries %s do not match vectors entries %s", graph.postingsMap.size(), graph.vectorValues.size());
        try (var iow = IndexFileUtils.instance.openOutput(file)) {
            var out = iow.asSequentialWriter();

            writeOrdinalToRowMapping(out, graph, keyToRowId);
            writeRowToOrdinalMapping(out, graph, keyToRowId);
        }
    }

    private static void writeOrdinalToRowMapping(SequentialWriter out, CassandraOnHeapHnsw graph, Map<PrimaryKey, Integer> keyToRowId) throws IOException
    {
        // total number of vectors
        out.writeInt(graph.vectorValues.size());

        // Write the offsets of the postings for each ordinal
        var offset = 4L + 8L * graph.vectorValues.size();
        for (var i = 0; i < graph.vectorValues.size(); i++) {
            // (ordinal is implied; don't need to write it)
            out.writeLong(offset);
            var postings = graph.postingsMap.get(graph.vectorValues.bufferValue(i));
            offset += 4 + (postings.keys.size() * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }

        // Write postings lists
        for (var i = 0; i < graph.vectorValues.size(); i++) {
            var postings = graph.postingsMap.get(graph.vectorValues.bufferValue(i));
            out.writeInt(postings.keys.size());
            for (var key : postings.keys) {
                out.writeInt(keyToRowId.get(key));
            }
        }
    }

    private static void writeRowToOrdinalMapping(SequentialWriter out, CassandraOnHeapHnsw graph, Map<PrimaryKey, Integer> keyToRowId) throws IOException
    {
        List<Pair<Integer, Integer>> pairs = new ArrayList<>();

        // Collect all (rowId, vectorOrdinal) pairs
        for (var i = 0; i < graph.vectorValues.size(); i++) {
            var postings = graph.postingsMap.get(graph.vectorValues.bufferValue(i));
            for (var key : postings.keys) {
                int rowId = keyToRowId.get(key);
                pairs.add(Pair.create(rowId, i));
            }
        }

        // Sort the pairs by rowId
        pairs.sort(Comparator.comparingInt(Pair::left));

        // Write the pairs to the file
        long startOffset = out.position();
        for (var pair : pairs) {
            out.writeInt(pair.left);
            out.writeInt(pair.right);
        }
        out.writeLong(startOffset);
    }
}
