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

package org.apache.cassandra.index.sai.cql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.vector.ConcurrentVectorValues;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.junit.Before;

public class VectorTester extends SAITester
{
    @Before
    public void setup() throws Throwable
    {
        // override maxBruteForceRows to a random number between 0 and 4 so that we make sure
        // the non-brute-force path gets called during tests (which mostly involve small numbers of rows)
        var n = getRandom().nextIntBetween(0, 4);
        var limitToTopResults = InvokePointBuilder.newInvokePoint()
                                                  .onClass("org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher")
                                                  .onMethod("limitToTopResults")
                                                  .atEntry();
        var bitsOrPostingListForKeyRange = InvokePointBuilder.newInvokePoint()
                                                             .onClass("org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher")
                                                             .onMethod("bitsOrPostingListForKeyRange")
                                                             .atEntry();
        var ab = ActionBuilder.newActionBuilder()
                              .actions()
                              .doAction("$this.globalBruteForceRows = " + n);
        var changeBruteForceThreshold = Injections.newCustom("force_non_bruteforce_queries")
                                                  .add(limitToTopResults)
                                                  .add(bitsOrPostingListForKeyRange)
                                                  .add(ab)
                                                  .build();
        Injections.inject(changeBruteForceThreshold);
    }

    public static double rawIndexedRecall(Collection<float[]> vectors, float[] query, List<float[]> result, int topK) throws IOException
    {
        ConcurrentVectorValues vectorValues = new ConcurrentVectorValues(query.length);
        int ordinal = 0;

        var graphBuilder = new GraphIndexBuilder<>(vectorValues,
                                                   VectorEncoding.FLOAT32,
                                                   VectorSimilarityFunction.COSINE,
                                                   16,
                                                   100,
                                                   1.2f,
                                                   1.4f);

        for (float[] vector : vectors)
        {
            vectorValues.add(ordinal, vector);
            graphBuilder.addGraphNode(ordinal++, vectorValues);
        }

        var results = GraphSearcher.search(query,
                                           topK,
                                           vectorValues,
                                           VectorEncoding.FLOAT32,
                                           VectorSimilarityFunction.COSINE,
                                           graphBuilder.getGraph(),
                                           null);

        List<float[]> nearestNeighbors = new ArrayList<>();
        for (var ns : results.getNodes())
            nearestNeighbors.add(vectorValues.vectorValue(ns.node));

        return recallMatch(nearestNeighbors, result, topK);
    }

    public static double recallMatch(List<float[]> expected, List<float[]> actual, int topK)
    {
        if (expected.isEmpty() && actual.isEmpty())
            return 1.0;

        int matches = 0;
        for (float[] in : expected)
        {
            for (float[] out : actual)
            {
                if (Arrays.compare(in, out) == 0)
                {
                    matches++;
                    break;
                }
            }
        }

        return (double) matches / topK;
    }
}
