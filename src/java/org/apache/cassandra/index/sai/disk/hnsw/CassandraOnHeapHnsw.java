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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.File;
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
public class CassandraOnHeapHnsw
{
    private final ByteBufferVectorValues vectorValues;
    private final ConcurrentHnswGraphBuilder<float[]> builder;
    private final AtomicInteger cachedDimensions = new AtomicInteger();
    private final TypeSerializer<float[]> serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final Map<ByteBuffer, VectorPostings> postingsMap;
    private final AtomicInteger nextOrdinal = new AtomicInteger();

    // FIXME this is disgusting and possibly unnecessary
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public CassandraOnHeapHnsw(IndexContext indexContext)
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
        lock.readLock().lock();
        try
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
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty()
    {
        return vectorValues.size() == 0;
    }

    public Collection<PrimaryKey> keysFromOrdinal(int node)
    {
        return postingsMap.get(vectorValues.bufferValue(node)).keys;
    }

    /**
     * @return PrimaryKeys associated with the topK vectors near the query
     */
    public PriorityQueue<PrimaryKey> search(float[] queryVector, int topK, Bits acceptBits, int vistLimit)
    {
        NeighborQueue queue;
        try
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             topK,
                                             vectorValues,
                                             VectorEncoding.FLOAT32,
                                             similarityFunction,
                                             builder.getGraph().getView(),
                                             acceptBits,
                                             vistLimit);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        var pq = new PriorityQueue<PrimaryKey>();
        while (queue.size() > 0)
        {
            for (var pk : keysFromOrdinal(queue.pop()))
            {
                pq.add(pk);
            }
        }
        return pq;
    }

    public long ramBytesUsed()
    {
        return builder.getGraph().ramBytesUsed(); // TODO close enough?
    }

    public int size()
    {
        return vectorValues.size();
    }

    private int rowCount()
    {
        return postingsMap.values().stream().mapToInt(p -> p.keys.size()).sum();
    }

    private void writeOrdinalToRowMapping(File file, Map<PrimaryKey, Integer> keyToRowId) throws IOException
    {
        Preconditions.checkState(keyToRowId.size() == rowCount(),
                                 "Expected %s rows, but found %s", keyToRowId.size(), rowCount());
        Preconditions.checkState(postingsMap.size() == vectorValues.size(),
                                 "Postings entries %s do not match vectors entries %s", postingsMap.size(), vectorValues.size());
        try (var iow = IndexFileUtils.instance.openOutput(file)) {
            var out = iow.asSequentialWriter();
            // total number of vectors
            out.writeInt(vectorValues.size());

            // Write the offsets of the postings for each ordinal
            long offset = 4L + 8L * vectorValues.size();
            for (var i = 0; i < vectorValues.size(); i++) {
                // (ordinal is implied; don't need to write it)
                out.writeLong(offset);
                var postings = postingsMap.get(vectorValues.bufferValue(i));
                offset += 4 + (postings.keys.size() * 4L); // 4 bytes for size and 4 bytes for each integer in the list
            }

            // Write postings lists
            for (var i = 0; i < vectorValues.size(); i++) {
                var postings = postingsMap.get(vectorValues.bufferValue(i));
                out.writeInt(postings.keys.size());
                for (var key : postings.keys) {
                    out.writeInt(keyToRowId.get(key));
                }
            }
        }
    }

    private void writeGraph(File file) throws IOException
    {
        new ConcurrentHnswGraphWriter(builder.getGraph()).write(file);
    }

    // TODO should we just save references to the vectors in the sstable itself?
    private void writeVectors(File file) throws IOException
    {
        try (var iow = IndexFileUtils.instance.openOutput(file)) {
            var out = iow.asSequentialWriter();
            out.writeInt(vectorValues.size());
            out.writeInt(vectorValues.dimension());

            for (var i = 0; i < vectorValues.size(); i++) {
                var buffer = vectorValues.bufferValue(i);
                out.write(buffer);
            }
        }
    }

    public void write(IndexDescriptor descriptor, IndexContext context, Map<PrimaryKey, Integer> keyToRowId) throws IOException
    {
        lock.writeLock().lock();
        try
        {
            writeVectors(descriptor.fileFor(IndexComponent.VECTOR, context));
            writeOrdinalToRowMapping(descriptor.fileFor(IndexComponent.POSTING_LISTS, context), keyToRowId);
            writeGraph(descriptor.fileFor(IndexComponent.TERMS_DATA, context));
        }
        finally
        {
            lock.writeLock().unlock();
        }
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
