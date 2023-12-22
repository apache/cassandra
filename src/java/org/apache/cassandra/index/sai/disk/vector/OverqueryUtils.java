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

package org.apache.cassandra.index.sai.disk.vector;

import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;

import static java.lang.Math.max;
import static java.lang.Math.pow;

/**
 * Utility methods for dealing with overquerying.
 */
public class OverqueryUtils
{
    /**
     * @return the topK >= `limit` results to ask the index to search for.  This allows
     * us to compensate for using lossily-compressed vectors during the search, by
     * searching deeper in the graph.
     */
    public static int topKFor(int limit, CompressedVectors cv)
    {
        // compute the factor `n` to multiply limit by to increase the number of results from the index.
        var n = 0.509 + 9.491 * pow(limit, -0.402); // f(1) = 10.0, f(100) = 2.0, f(1000) = 1.1
        // The function becomes less than 1 at limit ~= 1583.4
        n = max(1.0, n);

        // uncompressed indexes.
        // Some overquery will be needed for uncompressed indexes,
        // but not as much as for compressed indexes.
        if (cv == null)
            return (int) (n * limit);

        // 2x results at limit=100 is enough for all our tested data sets to match uncompressed recall,
        // except for the ada002 vectors that compress at a 32x ratio.  For ada002, we need 3x results
        // with PQ, and 4x for BQ.
        if (cv instanceof BinaryQuantization)
            n *= 2;
        else if ((double) cv.getOriginalSize() / cv.getCompressedSize() > 16.0)
            n *= 1.5;

        return (int) (n * limit);
    }
}
