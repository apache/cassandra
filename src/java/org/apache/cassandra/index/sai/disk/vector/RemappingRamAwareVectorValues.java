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

import java.util.function.IntUnaryOperator;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;

/**
 * Remaps the ordinals of a {@link RamAwareVectorValues} using the provided {@link IntUnaryOperator}.
 */
public class RemappingRamAwareVectorValues implements RamAwareVectorValues
{
    private final IntUnaryOperator reverseOrdinalMapper;
    private final RamAwareVectorValues vectors;

    public RemappingRamAwareVectorValues(RamAwareVectorValues vectors, IntUnaryOperator reverseOrdinalMapper)
    {
        this.vectors = vectors;
        this.reverseOrdinalMapper = reverseOrdinalMapper;
    }

    @Override
    public int size()
    {
        return vectors.size();
    }

    @Override
    public int dimension()
    {
        return vectors.dimension();
    }

    @Override
    public float[] vectorValue(int i)
    {
        int originalNode = reverseOrdinalMapper.applyAsInt(i);
        return vectors.vectorValue(originalNode);
    }

    @Override
    public boolean isValueShared()
    {
        return vectors.isValueShared();
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
    {
        return this;
    }

    @Override
    public long ramBytesUsed()
    {
        return vectors.ramBytesUsed();
    }
}
