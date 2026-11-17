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

import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

public final class JVectorUtils
{
    /**
     * The jvector on-disk format version used for graphs and compressed vectors. Version 2 is the format written by
     * jvector 1.x, which is what the SAI {@code aa} on-disk format contains. Writing a newer version would produce
     * files that older nodes cannot read, so changing this requires a new SAI on-disk format version.
     */
    public static final int ON_DISK_FORMAT_VERSION = 2;

    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private JVectorUtils()
    {
    }

    /**
     * Wraps the given array without copying it.
     */
    public static VectorFloat<?> toVectorFloat(float[] vector)
    {
        return vts.createFloatVector(vector);
    }

    public static float compare(VectorSimilarityFunction function, float[] v1, float[] v2)
    {
        return function.compare(toVectorFloat(v1), toVectorFloat(v2));
    }

    /**
     * Copies the remaining nodes of {@code iterator}, because a view may reuse the iterator's storage for its next call.
     */
    public static int[] toArray(NodesIterator iterator)
    {
        int[] nodes = new int[iterator.size()];
        for (int i = 0; i < nodes.length; i++)
            nodes[i] = iterator.nextInt();
        return nodes;
    }
}
