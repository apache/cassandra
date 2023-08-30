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

package org.apache.cassandra.index.sai.disk.v1.vector.hnsw;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public abstract class CassandraHnswGraphBuilder<T>
{
    public abstract long addGraphNode(int ordinal, RandomAccessVectorValues<T> vectors) throws IOException;

    public abstract ExtendedHnswGraph getGraph();

    public abstract boolean isConcurrent();

    public abstract int insertsInProgress();

    public static class ConcurrentBuilder<T> extends CassandraHnswGraphBuilder<T>
    {
        private final ConcurrentHnswGraphBuilder<T> builder;

        public ConcurrentBuilder(RandomAccessVectorValues<T> vectorValues,
                                 VectorEncoding vectorEncoding,
                                 VectorSimilarityFunction similarityFunction,
                                 int maximumNodeConnections,
                                 int constructionBeamWidth)
        {
            try
            {
                builder = ConcurrentHnswGraphBuilder.create(vectorValues, vectorEncoding, similarityFunction, maximumNodeConnections, constructionBeamWidth);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long addGraphNode(int ordinal, RandomAccessVectorValues<T> vectors) throws IOException
        {
            return builder.addGraphNode(ordinal, vectors);
        }

        @Override
        public ExtendedHnswGraph getGraph()
        {
            return new ExtendedConcurrentHnswGraph(builder.getGraph());
        }

        @Override
        public int insertsInProgress()
        {
            return builder.insertsInProgress();
        }

        @Override
        public boolean isConcurrent()
        {
            return true;
        }
    }

    public static class SerialBuilder<T> extends CassandraHnswGraphBuilder<T>
    {
        private final HnswGraphBuilder<T> builder;

        public SerialBuilder(RandomAccessVectorValues<T> vectorValues,
                             VectorEncoding vectorEncoding,
                             VectorSimilarityFunction similarityFunction,
                             int maximumNodeConnections,
                             int constructionBeamWidth)
        {
            try
            {
                builder = HnswGraphBuilder.create(vectorValues,
                                                  vectorEncoding,
                                                  similarityFunction,
                                                  maximumNodeConnections,
                                                  constructionBeamWidth,
                                                  ThreadLocalRandom.current().nextLong());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long addGraphNode(int ordinal, RandomAccessVectorValues<T> vectors) throws IOException
        {
            builder.addGraphNode(ordinal, vectors);
            return 0;
        }

        @Override
        public ExtendedHnswGraph getGraph()
        {
            return new ExtendedSerialHnswGraph(builder.getGraph());
        }

        @Override
        public int insertsInProgress()
        {
            return 0;
        }

        @Override
        public boolean isConcurrent()
        {
            return false;
        }
    }
}
