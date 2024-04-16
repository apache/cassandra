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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;

/**
 * Base type for the types being composed of multi-elements like Collections, Tuples, UDTs or Vectors.
 * This class unifies the methods used by the CQL layer to work with those types,
 * and it includes both frozen and non-frozen multi-element types.
 */
public abstract class MultiElementType<T> extends AbstractType<T>
{
    protected MultiElementType(ComparisonType comparisonType)
    {
        super(comparisonType);
    }

    /**
     * Returns the serialized representation of the value composed of the specified elements.
     *
     * @param elements the serialized values of the elements
     * @return the serialized representation of the value composed of the specified elements.
     */
    public abstract ByteBuffer pack(List<ByteBuffer> elements);

    /**
     * Returns the serialized representation of the elements composing the specified value.
     *
     * @param value a serialized value of this type
     * @return the serialized representation of the elements composing the specified value.
     */
    public abstract List<ByteBuffer> unpack(ByteBuffer value);

    /**
     * Checks if this type supports bind markers for its elements when the type value is provided through a literal.
     * @return {@code true} if this type supports bind markers for its elements, {@code false} otherwise.
     */
    public boolean supportsElementBindMarkers()
    {
        return true;
    }

    /**
     * Filter and sort the elements, if needed, before validating them.
     * <p>
     * This method takes as input a list of elements, eliminates duplicates and reorders them if needed (e.g. {@code SetType} and {@code MapType}) and validate them.
     * @param buffers the elements of this type
     * @return the elements filtered and sorted as they are used for serialization.
     */
    public abstract List<ByteBuffer> filterSortAndValidateElements(List<ByteBuffer> buffers);

    /**
     * Compares the multicell value represensted by the column data with the specified elements.
     * @param columnData the column data representing the multicell value
     * @param elements the elements to compare
     * @return a negative integer, zero, or a positive integer as the column data is less than, equal to, or greater than the elements.
     * @throws UnsupportedOperationException if the comparison is not supported by this type.
     */
    public abstract int compareCQL(ComplexColumnData columnData, List<ByteBuffer> elements);

    /**
     * Returns the type of the element at the specified key or index (optional operation).
     * @param keyOrIndex the key or index of the element
     * @return the type of the element at the specified key or index.
     * @throws UnsupportedOperationException if this method is not supported by this type.
     */
    public AbstractType<?> elementType(ByteBuffer keyOrIndex)
    {
        throw new UnsupportedOperationException(this + " does not support retrieving element types by key or index");
    }

    /**
     * Returns the element of the column data at the specified key or index (optional operation).
     * @param columnData the column data representing the multicell value
     * @param keyOrIndex the key or index of the element to return
     * @return the element of the column data at the specified key or index.
     * @throws UnsupportedOperationException if this method is not supported by this type.
     */
    public ByteBuffer getElement(@Nullable ColumnData columnData, ByteBuffer keyOrIndex)
    {
        throw new UnsupportedOperationException(this + " does not support retrieving elements by key or index");
    }
}

