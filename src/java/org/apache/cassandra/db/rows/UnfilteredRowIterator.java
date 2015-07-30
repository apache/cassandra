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

import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * An iterator over the rows of a given partition that also includes deletion informations.
 * <p>
 * An {@code UnfilteredRowIterator} contains a few partition top-level informations and is an
 * iterator of {@code Unfiltered}, that is of either {@code Row} or {@code RangeTombstoneMarker}.
 * An implementation of {@code UnfilteredRowIterator} <b>must</b> provide the following
 * guarantees:
 *   1. the returned {@code Unfiltered} must be in clustering order, or in reverse clustering
 *      order iff {@link #isReverseOrder} returns true.
 *   2. the iterator should not shadow its own data. That is, no deletion
 *      (partition level deletion, row deletion, range tombstone, complex
 *      deletion) should delete anything else returned by the iterator (cell, row, ...).
 *   3. every "start" range tombstone marker should have a corresponding "end" marker, and no other
 *      marker should be in-between this start-end pair of marker. Note that due to the
 *      previous rule this means that between a "start" and a corresponding "end" marker there
 *      can only be rows that are not deleted by the markers. Also note that when iterating
 *      in reverse order, "end" markers are returned before their "start" counterpart (i.e.
 *      "start" and "end" are always in the sense of the clustering order).
 *
 * Note further that the objects returned by next() are only valid until the
 * next call to hasNext() or next(). If a consumer wants to keep a reference on
 * the returned objects for longer than the iteration, it must make a copy of
 * it explicitly.
 */
public interface UnfilteredRowIterator extends BaseRowIterator<Unfiltered>
{
    /**
     * The partition level deletion for the partition this iterate over.
     */
    public DeletionTime partitionLevelDeletion();

    /**
     * Return "statistics" about what is returned by this iterator. Those are used for
     * performance reasons (for delta-encoding for instance) and code should not
     * expect those to be exact.
     */
    public EncodingStats stats();

    /**
     * Returns whether this iterator has no data (including no deletion data).
     */
    public default boolean isEmpty()
    {
        return partitionLevelDeletion().isLive()
            && staticRow().isEmpty()
            && !hasNext();
    }
}
