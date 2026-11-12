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

import java.util.stream.IntStream;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

/**
 * Encodes vectors with the codebooks of a {@link ProductQuantization}, producing the same codes as
 * {@link ProductQuantization#encodeAll}.
 * <p>
 * jvector computes each distance to a centroid through the Vector API. When C2 inlines jvector's encoding into a
 * caller deep enough to pass {@code MaxInlineLevel}, it stops intrinsifying those Vector API calls, and every distance
 * runs through the API's boxing fallback instead, tens of times slower. Whether that happens depends on how the JIT
 * compiles the code, so it differs between JVMs. SAI's subvectors are 2 or 3 floats, too few for SIMD to help, so this
 * computes the distances with plain arithmetic, adding the squared differences in the same order jvector does.
 */
public final class PQEncoder
{

    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private PQEncoder()
    {
    }

    /**
     * Encodes all of {@code vectors} in parallel. The product quantization must be unweighted, i.e. not anisotropic,
     * as the one {@link OnHeapGraph} trains is.
     */
    public static CompressedVectors encodeAll(ProductQuantization pq, RandomAccessVectorValues vectors)
    {
        int subspaces = pq.getSubspaceCount();
        if (pq.getGlobalCentroid() != null)
            return pq.encodeAll(vectors);
        // jvector adds the squared differences of subvectors shorter than 4 floats in index order, as this does. It
        // adds longer ones lane by lane, in an order that depends on the SIMD width, so those are left to jvector.
        for (int m = 0; m < subspaces; m++)
        {
            int size = pq.getSubvectorSize(m);
            if (size != 2 && size != 3)
                return pq.encodeAll(vectors);
        }

        int clusters = pq.getClusterCount();
        int[] sizes = new int[subspaces];
        int[] offsets = new int[subspaces];
        float[][] codebooks = new float[subspaces][];
        for (int m = 0, offset = 0; m < subspaces; offset += sizes[m], m++)
        {
            sizes[m] = pq.getSubvectorSize(m);
            offsets[m] = offset;
            codebooks[m] = toArray(pq.getCodebookVector(m), clusters * sizes[m]);
        }

        ByteSequence<?>[] codes = IntStream.range(0, vectors.size())
                                           .parallel()
                                           .mapToObj(i -> vts.createByteSequence(encode(vectors.getVector(i), codebooks, sizes, offsets, clusters)))
                                           .toArray(ByteSequence<?>[]::new);
        return pq.createCompressedVectors(codes);
    }

    private static byte[] encode(VectorFloat<?> vector, float[][] codebooks, int[] sizes, int[] offsets, int clusters)
    {
        byte[] code = new byte[codebooks.length];
        // like jvector, a missing vector gets all-zero codes
        if (vector == null)
            return code;

        float[] v = toArray(vector, vector.length());
        for (int m = 0; m < codebooks.length; m++)
        {
            int nearest = sizes[m] == 2
                          ? nearest2(v[offsets[m]], v[offsets[m] + 1], codebooks[m], clusters)
                          : nearest3(v[offsets[m]], v[offsets[m] + 1], v[offsets[m] + 2], codebooks[m], clusters);
            code[m] = (byte) nearest;
        }
        return code;
    }

    // In both methods below, the first centroid at the minimum distance wins, as in jvector.

    private static int nearest2(float x0, float x1, float[] codebook, int clusters)
    {
        int nearest = 0;
        float nearestDistance = Float.MAX_VALUE;
        for (int c = 0; c < clusters; c++)
        {
            float d0 = x0 - codebook[2 * c];
            float d1 = x1 - codebook[2 * c + 1];
            float distance = d0 * d0 + d1 * d1;
            if (distance < nearestDistance)
            {
                nearestDistance = distance;
                nearest = c;
            }
        }
        return nearest;
    }

    private static int nearest3(float x0, float x1, float x2, float[] codebook, int clusters)
    {
        int nearest = 0;
        float nearestDistance = Float.MAX_VALUE;
        for (int c = 0; c < clusters; c++)
        {
            float d0 = x0 - codebook[3 * c];
            float d1 = x1 - codebook[3 * c + 1];
            float d2 = x2 - codebook[3 * c + 2];
            float distance = d0 * d0 + d1 * d1 + d2 * d2;
            if (distance < nearestDistance)
            {
                nearestDistance = distance;
                nearest = c;
            }
        }
        return nearest;
    }

    private static float[] toArray(VectorFloat<?> vector, int length)
    {
        float[] array = new float[length];
        for (int i = 0; i < length; i++)
            array[i] = vector.get(i);
        return array;
    }
}
