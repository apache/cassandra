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

import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class MemtableFloat32VectorValues implements RandomAccessVectorValues<float[]>
{
    ArrayList<float[]> vectors;

    private MemtableFloat32VectorValues(ArrayList<float[]> vectors)
    {
        this.vectors = vectors;
    }

    @Override
    public int size()
    {
        return vectors.size();
    }

    @Override
    public int dimension()
    {
        try
        {
            return vectorValue(0).length;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static float[] vectorFrom(ByteComparable bc) {
        var source = bc.asComparableBytes(ByteComparable.Version.OSS41);
        // get the length as big-endian encoded 32bit int
        var length = (source.next() << 24) | (source.next() << 16) | (source.next() << 8) | (source.next());
        var vector = new float[length];
        for (int i = 0; i < length; i++) {
            // float serialization (ByteBuffer.putFloat) is also byte-order-sensitive
            vector[i] = Float.intBitsToFloat((source.next() << 24) | (source.next() << 16) | (source.next() << 8) | (source.next()));
        }
        return vector;
    }

    @Override
    public float[] vectorValue(int vectorIndex) throws IOException
    {
        return vectors.get(vectorIndex);
    }

    @Override
    public MemtableFloat32VectorValues copy()
    {
        return new MemtableFloat32VectorValues((ArrayList<float[]>) vectors.clone());
    }

    public static MemtableFloat32VectorValues from(MemtableTermsIterator terms)
    {
        var vectors = new ArrayList<float[]>();
        while (terms.hasNext())
        {
            vectors.add(vectorFrom(terms.next()));
        }
        return new MemtableFloat32VectorValues(vectors);
    }
}
