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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.io.IOException;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;

/**
 * This is the base class for adding values to an on-disk balanced tree. It comes in 2 forms:
 * 1) A mutable form that has out or order values that need sorting.
 * 2) An immutable form that has values in order that don't need sorting.
 */
public abstract class IntersectingPointValues extends MutablePointValues
{
    public boolean needsSorting()
    {
        return size() > 1;
    }

    public abstract void intersect(IntersectVisitor visitor) throws IOException;

    @Override
    public int getDocCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getValue(int i, BytesRef packedValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByteAt(int i, int k)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDocID(int i)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void swap(int i, int j)
    {
        throw new IllegalStateException("unexpected sorting");
    }

    @Override
    public void intersect(PointValues.IntersectVisitor visitor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long estimatePointCount(PointValues.IntersectVisitor visitor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMinPackedValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMaxPackedValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumDimensions()
    {
        return 1;
    }

    public interface IntersectVisitor
    {
        /**
         * Called for all values in a leaf cell that crosses the query.  The consumer should scrutinize the
         * packedValue to decide whether to accept it. Values are visited in increasing order, and in the case of ties,
         * in increasing rowID order.
         */
        void visit(long rowID, byte[] packedValue) throws IOException;
    }
}
