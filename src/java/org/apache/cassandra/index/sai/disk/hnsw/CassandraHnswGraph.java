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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.ConcurrentHnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/**
 * This class needs to
 * (1) Map vectors in the HnswGraph, to ordinals
 * (2) Map vectors to row keys -- potentially more than one row has a given vector value
 * (3) Map ordinals to row keys (this is what allows us to search the graph)
 * (4) When writing to disk, remap row keys to row IDs, while retaining the vector-to-row mapping
 * (5) Do all this in a thread-safe api
 */
@SuppressWarnings("com.google.common.annotations.Beta")
public class CassandraHnswGraph
{
    private final ByteBufferVectorValues vectorValues;
    private final ConcurrentHnswGraphBuilder<float[]> builder;
    private final AtomicInteger cachedDimensions = new AtomicInteger();
    private final TypeSerializer<float[]> serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final Map<ByteBuffer, VectorPostings> postingsMap;
    private final AtomicInteger nextOrdinal = new AtomicInteger();

    public CassandraHnswGraph(IndexContext indexContext)
    {
        vectorValues = new ByteBufferVectorValues();
        serializer = (TypeSerializer<float[]>) indexContext.getValidator().getSerializer();
        similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        postingsMap = new ConcurrentSkipListMap<>((left, right) -> {
            return ValueAccessor.compare(left, ByteBufferAccessor.instance, right, ByteBufferAccessor.instance);
        });

        try
        {
            builder = ConcurrentHnswGraphBuilder.create(vectorValues,
                                            VectorEncoding.FLOAT32,
                                            similarityFunction,
                                            indexContext.getIndexWriterConfig().getMaximumNodeConnections(),
                                            indexContext.getIndexWriterConfig().getConstructionBeamWidth());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void put(PrimaryKey key, ByteBuffer value)
    {
        var postings = postingsMap.computeIfAbsent(value, v -> {
            var ordinal = nextOrdinal.getAndIncrement();
            vectorValues.add(ordinal, value);
            try
            {
                builder.addGraphNode(ordinal, vectorValues);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return new VectorPostings(ordinal);
        });
        postings.append(key);
    }

    public boolean isEmpty()
    {
        return vectorValues.size() == 0;
    }

    public Collection<PrimaryKey> keysFromOrdinal(int node)
    {
        return postingsMap.get(vectorValues.bufferValue(node)).keys;
    }

    public NeighborQueue search(float[] queryVector, int topK, VectorEncoding encoding, Bits acceptBits, int vistLimit)
    {
        try
        {
            return HnswGraphSearcher.search(queryVector,
                                            topK,
                                            vectorValues,
                                            encoding,
                                            similarityFunction,
                                            builder.getGraph().getView(),
                                            acceptBits,
                                            vistLimit);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long ramBytesUsed()
    {
        return builder.getGraph().ramBytesUsed(); // TODO close enough?
    }

    public int size()
    {
        return vectorValues.size();
    }

    private static class VectorPostings
    {
        public final int ordinal;
        public final List<PrimaryKey> keys;

        private VectorPostings(int ordinal)
        {
            this.ordinal = ordinal;
            // we expect that the overwhelmingly most common cardinality will be 1, so optimize for reads
            keys = new CopyOnWriteArrayList<>();
        }

        public void append(PrimaryKey key)
        {
            keys.add(key);
        }
    }

    private class ByteBufferVectorValues implements RandomAccessVectorValues<float[]>
    {
        private final Map<Integer, ByteBuffer> values = new ConcurrentSkipListMap<>();

        @Override
        public int size()
        {
            return values.size();
        }

        @Override
        public int dimension()
        {
            int i = cachedDimensions.get();
            if (i == 0)
            {
                i = vectorValue(0).length;
                cachedDimensions.set(i);
            }
            return i;
        }

        @Override
        public float[] vectorValue(int i)
        {
            return serializer.deserialize(values.get(i));
        }

        public void add(int ordinal, ByteBuffer buffer)
        {
            values.put(ordinal, buffer);
        }

        @Override
        public RandomAccessVectorValues<float[]> copy()
        {
            return this;
        }

        public ByteBuffer bufferValue(int node)
        {
            return values.get(node);
        }
    }
}
