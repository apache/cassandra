/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;

public abstract class MutableOneDimPointValues extends MutablePointValues
{
    private static final byte[] EMPTY = new byte[0];

    abstract public void intersect(IntersectVisitor visitor) throws IOException;

    @Override
    public int getDocCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size()
    {
        // hack to skip sorting in Lucene
        return 1;
    }

    @Override
    public void getValue(int i, BytesRef packedValue)
    {
        // no-op
    }

    @Override
    public byte getByteAt(int i, int k)
    {
        return 0;
    }

    @Override
    public int getDocID(int i)
    {
        return 0;
    }

    @Override
    public void swap(int i, int j)
    {
        throw new IllegalStateException("unexpected sorting");
    }

    @Override
    public void intersect(PointValues.IntersectVisitor visitor) throws IOException
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
        return EMPTY;
    }

    @Override
    public byte[] getMaxPackedValue()
    {
        return EMPTY;
    }

    @Override
    public int getNumDimensions()
    {
        return 1;
    }

    @Override
    public int getBytesPerDimension()
    {
        return 0;
    }

    public interface IntersectVisitor
    {
        /** Called for all documents in a leaf cell that crosses the query.  The consumer
         *  should scrutinize the packedValue to decide whether to accept it.  In the 1D case,
         *  values are visited in increasing order, and in the case of ties, in increasing
         *  docID order. */
        void visit(long docID, byte[] packedValue) throws IOException;
    }
}
