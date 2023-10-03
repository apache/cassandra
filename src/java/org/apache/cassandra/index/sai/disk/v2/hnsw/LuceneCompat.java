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

package org.apache.cassandra.index.sai.disk.v2.hnsw;

import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

public class LuceneCompat
{
    public static org.apache.lucene.util.Bits bits(Bits bits)
    {
        if (bits == null)
            return null;

        if (bits instanceof org.apache.lucene.util.Bits)
            return (org.apache.lucene.util.Bits) bits;

        return new org.apache.lucene.util.Bits()
        {
            @Override
            public boolean get(int index)
            {
                return bits.get(index);
            }

            @Override
            public int length()
            {
                return bits.length();
            }
        };
    }

    public static org.apache.lucene.index.VectorSimilarityFunction vsf(VectorSimilarityFunction similarityFunction)
    {
        switch (similarityFunction) {
            case EUCLIDEAN:
                return org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
            case COSINE:
                return org.apache.lucene.index.VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT:
                return org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
            default:
                throw new IllegalArgumentException("Unknown similarity function: " + similarityFunction);
        }
    }
}
