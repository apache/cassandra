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

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class OnDiskVectors implements RandomAccessVectorValues<float[]>, AutoCloseable
{
    private final RandomAccessReader reader;
    private final long segmentOffset;
    private final int dimension;
    private final int size;
    private final float[] vector;

    public OnDiskVectors(FileHandle fh, long segmentOffset)
    {
        try
        {
            this.reader = fh.createReader();
            reader.seek(segmentOffset);
            this.segmentOffset = segmentOffset;

            this.size = reader.readInt();
            this.dimension = reader.readInt();
            this.vector = new float[dimension];
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error initializing OnDiskVectors at segment offset" + segmentOffset, e);
        }
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public int dimension()
    {
        return dimension;
    }

    @Override
    public float[] vectorValue(int i) throws IOException
    {
        readVector(i, vector);
        return vector;
    }

    void readVector(int i, float[] v) throws IOException
    {
        reader.readFloatsAt(segmentOffset + 8L + i * dimension * 4L, v);
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
    {
        // this is only necessary if we need to build a new graph from this vector source.
        // (the idea is that if you are re-using float[] between calls, like we are here,
        //  you can make a copy of the source so you can compare different neighbors' scores
        //  as you build the graph.)
        // since we only build new graphs during insert and compaction, when we get the vectors from the source rows,
        // we don't need to worry about this.
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        reader.close();
    }
}
