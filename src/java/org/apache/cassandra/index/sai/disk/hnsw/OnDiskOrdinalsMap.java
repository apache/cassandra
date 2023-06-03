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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileHandle;

public class OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskOrdinalsMap.class);

    private final FileHandle fh;
    private final long segmentOffset;
    private final long segmentLength;
    private final int size;
    private final long rowOrdinalOffset;

    public OnDiskOrdinalsMap(FileHandle fh, long segmentOffset, long segmentLength)
    {
        this.segmentOffset = segmentOffset;
        this.segmentLength = segmentLength;
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            this.size = reader.readInt();
            reader.seek(segmentOffset + segmentLength - 8);
            this.rowOrdinalOffset = reader.readLong();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int[] getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
    {
        Preconditions.checkArgument(vectorOrdinal < size, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, size);

        try (var reader = fh.createReader())
        {
            // read index entry
            reader.seek(segmentOffset + 4L + vectorOrdinal * 8L);
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
    }

    /**
     * @return order if given row id is found; otherwise return -1
     */
    public int getOrdinalForRowId(int rowId) throws IOException
    {
        try (var reader = fh.createReader())
        {
            // Compute the offset of the start of the rowId to vectorOrdinal mapping
            var high = (segmentOffset + segmentLength - 8 - rowOrdinalOffset) / 8;
            long index = DiskBinarySearch.searchInt(0, Math.toIntExact(high), rowId, i -> {
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

            // not found
            if (index < 0)
                return -1;

            return reader.readInt();
        }
    }

    public void close()
    {
        fh.close();
    }
}
