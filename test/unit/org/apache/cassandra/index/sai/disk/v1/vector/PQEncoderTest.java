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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import static org.assertj.core.api.Assertions.assertThat;

public class PQEncoderTest
{
    private static final int SIZE = 2000;
    private static final int CLUSTERS = 256;

    @Test
    public void testTwoFloatSubvectors()
    {
        checkMatchesJVector(8, 4, 1);
    }

    @Test
    public void testThreeFloatSubvector()
    {
        // 9 / 4 leaves a remainder, so the first subvector has 3 floats
        checkMatchesJVector(9, 4, 2);
    }

    @Test
    public void testSAISubspacesOfLargeVectors()
    {
        checkMatchesJVector(768, 384, 3);
    }

    @Test
    public void testSubvectorsLeftToJVector()
    {
        // 4 floats per subvector, then 1, which PQEncoder leaves to jvector
        checkMatchesJVector(16, 4, 4);
        checkMatchesJVector(8, 8, 5);
    }

    /**
     * Checks that PQEncoder gives every vector the codes jvector does, for unit and non-unit vectors, duplicates and a
     * zero vector.
     */
    private static void checkMatchesJVector(int dimension, int subspaces, long seed)
    {
        Random random = new Random(seed);
        List<VectorFloat<?>> vectors = new ArrayList<>(SIZE);
        for (int i = 0; i < SIZE; i++)
        {
            if (i % 100 == 99)
            {
                vectors.add(vectors.get(random.nextInt(i)));
                continue;
            }
            float scale = i % 2 == 0 ? 1 : 10;
            float[] vector = new float[dimension];
            double norm = 0;
            for (int d = 0; d < dimension; d++)
            {
                vector[d] = (float) random.nextGaussian();
                norm += vector[d] * vector[d];
            }
            for (int d = 0; d < dimension; d++)
                vector[d] *= (float) (scale / Math.sqrt(norm));
            vectors.add(JVectorUtils.toVectorFloat(vector));
        }
        vectors.set(SIZE / 2, JVectorUtils.toVectorFloat(new float[dimension]));
        ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, dimension);

        ProductQuantization pq = ProductQuantization.compute(ravv, subspaces, CLUSTERS, false);
        PQVectors expected = (PQVectors) pq.encodeAll(ravv);
        CompressedVectors actual = PQEncoder.encodeAll(pq, ravv);

        assertThat(actual).isInstanceOf(PQVectors.class);
        PQVectors actualPQ = (PQVectors) actual;
        assertThat(actualPQ.count()).isEqualTo(SIZE);
        for (int i = 0; i < SIZE; i++)
        {
            for (int m = 0; m < subspaces; m++)
                assertThat(actualPQ.get(i).get(m)).as("vector %d, subspace %d", i, m).isEqualTo(expected.get(i).get(m));
        }
        assertThat(actualPQ).isEqualTo(expected);
    }
}
