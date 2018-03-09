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
package org.apache.cassandra.db.rows;

/**
 * An iterator over rows belonging to a partition.
 *
 * A RowIterator is an UnfilteredRowIterator to which any deletion information has been
 * filtered out. As such, all cell of all rows returned by this iterator are,
 * by definition, live, and hence code using a RowIterator don't have to worry
 * about tombstones and other deletion information.
 *
 * Note that as for UnfilteredRowIterator, the rows returned must be in clustering order (or
 * reverse clustering order if isReverseOrder is true), and the Row objects returned
 * by next() are only valid until the next call to hasNext() or next().
 */
public interface RowIterator extends BaseRowIterator<Row>
{
    /**
     * Returns whether the provided iterator has no data.
     */
    public default boolean isEmpty()
    {
        return staticRow().isEmpty() && !hasNext();
    }
}
