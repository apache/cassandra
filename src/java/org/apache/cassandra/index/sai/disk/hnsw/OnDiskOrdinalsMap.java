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
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;

class OnDiskOrdinalsMap
{
    private final RandomAccessReader reader;
    private final int size;
    private Map<Integer, int[]> ordinalToRowIdMap;
    private Map<Integer, Integer> rowIdToOrdinalMap;

    public OnDiskOrdinalsMap(File file) throws IOException
    {
        this.reader = RandomAccessReader.open(file);
        this.size = reader.readInt();
        readAllOrdinals();
    }

// FIXME do we bring this back or just accept that the mapping lives in memory?
//        public int[] getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
//        {
//            Preconditions.checkArgument(vectorOrdinal < size, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, size);
//
//            // read index entry
//            reader.seek(4L + vectorOrdinal * 8L);
//            long offset = reader.readLong();
//            // seek to and read ordinals
//            reader.seek(offset);
//            int postingsSize = reader.readInt();
//            int[] ordinals = new int[postingsSize];
//            for (int i = 0; i < ordinals.length; i++)
//            {
//                ordinals[i] = reader.readInt();
//            }
//            return ordinals;
//        }

    public int[] getSegmentRowIdsMatching(int ordinal)
    {
        return ordinalToRowIdMap.get(ordinal);
    }

    public Integer getOrdinalForRowId(int rowId)
    {
        return rowIdToOrdinalMap.get(rowId);
    }

    private void readAllOrdinals() throws IOException
    {
        ordinalToRowIdMap = new HashMap<>();
        rowIdToOrdinalMap = new HashMap<>();
        for (int vectorOrdinal = 0; vectorOrdinal < size; vectorOrdinal++)
        {
            reader.seek(4L + vectorOrdinal * 8L);
            long offset = reader.readLong();
            reader.seek(offset);
            int postingsSize = reader.readInt();
            var rowIds = new int[postingsSize];
            for (int i = 0; i < postingsSize; i++)
            {
                int rowId = reader.readInt();
                rowIds[i] = rowId;
                rowIdToOrdinalMap.put(rowId, vectorOrdinal);
            }
            ordinalToRowIdMap.put(vectorOrdinal, rowIds);
        }
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
            // total number of vectors
            out.writeInt(graph.vectorValues.size());

            // Write the offsets of the postings for each ordinal
            long offset = 4L + 8L * graph.vectorValues.size();
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
    }
}
