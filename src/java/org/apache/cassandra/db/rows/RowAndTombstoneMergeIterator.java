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

import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.db.*;

public class RowAndTombstoneMergeIterator extends UnmodifiableIterator<Unfiltered> implements PeekingIterator<Unfiltered>
{
    private final Comparator<Clusterable> comparator;
    private final boolean reversed;

    private Iterator<Row> rowIter;
    private Row nextRow;

    private Iterator<RangeTombstone> tombstoneIter;
    private RangeTombstone nextTombstone;
    private boolean inTombstone;

    private Unfiltered next;

    public RowAndTombstoneMergeIterator(ClusteringComparator comparator, boolean reversed)
    {
        this.comparator = reversed ? comparator.reversed() : comparator;
        this.reversed = reversed;
    }

    public RowAndTombstoneMergeIterator setTo(Iterator<Row> rowIter, Iterator<RangeTombstone> tombstoneIter)
    {
        this.rowIter = rowIter;
        this.tombstoneIter = tombstoneIter;
        this.nextRow = null;
        this.nextTombstone = null;
        this.next = null;
        this.inTombstone = false;
        return this;
    }

    public boolean isSet()
    {
        return rowIter != null;
    }

    private void prepareNext()
    {
        if (next != null)
            return;

        if (nextTombstone == null && tombstoneIter.hasNext())
            nextTombstone = tombstoneIter.next();
        if (nextRow == null && rowIter.hasNext())
            nextRow = rowIter.next();

        if (nextTombstone == null)
        {
            if (nextRow == null)
                return;

            next = nextRow;
            nextRow = null;
        }
        else if (nextRow == null)
        {
            if (inTombstone)
            {
                RangeTombstone rt = nextTombstone;
                nextTombstone = tombstoneIter.hasNext() ? tombstoneIter.next() : null;
                // An end and a start makes a boundary if they sort similarly
                if (nextTombstone != null
                        && comparator.compare(rt.deletedSlice().close(reversed), nextTombstone.deletedSlice().open(reversed)) == 0)
                {
                    next = RangeTombstoneBoundaryMarker.makeBoundary(reversed,
                                                                     rt.deletedSlice().close(reversed),
                                                                     nextTombstone.deletedSlice().open(reversed),
                                                                     rt.deletionTime(),
                                                                     nextTombstone.deletionTime());
                }
                else
                {
                    inTombstone = false;
                    next = new RangeTombstoneBoundMarker(rt.deletedSlice().close(reversed), rt.deletionTime());
                }
            }
            else
            {
                inTombstone = true;
                next = new RangeTombstoneBoundMarker(nextTombstone.deletedSlice().open(reversed), nextTombstone.deletionTime());
            }
        }
        else if (inTombstone)
        {
            if (comparator.compare(nextTombstone.deletedSlice().close(reversed), nextRow.clustering()) < 0)
            {
                RangeTombstone rt = nextTombstone;
                nextTombstone = tombstoneIter.hasNext() ? tombstoneIter.next() : null;
                if (nextTombstone != null
                        && comparator.compare(rt.deletedSlice().close(reversed), nextTombstone.deletedSlice().open(reversed)) == 0)
                {
                    next = RangeTombstoneBoundaryMarker.makeBoundary(reversed,
                                                                     rt.deletedSlice().close(reversed),
                                                                     nextTombstone.deletedSlice().open(reversed),
                                                                     rt.deletionTime(),
                                                                     nextTombstone.deletionTime());
                }
                else
                {
                    inTombstone = false;
                    next = new RangeTombstoneBoundMarker(rt.deletedSlice().close(reversed), rt.deletionTime());
                }
            }
            else
            {
                next = nextRow;
                nextRow = null;
            }
        }
        else
        {
            if (comparator.compare(nextTombstone.deletedSlice().open(reversed), nextRow.clustering()) < 0)
            {
                inTombstone = true;
                next = new RangeTombstoneBoundMarker(nextTombstone.deletedSlice().open(reversed), nextTombstone.deletionTime());
            }
            else
            {
                next = nextRow;
                nextRow = null;
            }
        }
    }

    public boolean hasNext()
    {
        prepareNext();
        return next != null;
    }

    public Unfiltered next()
    {
        prepareNext();
        Unfiltered toReturn = next;
        next = null;
        return toReturn;
    }

    public Unfiltered peek()
    {
        prepareNext();
        return next();
    }
}
