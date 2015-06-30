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

import java.util.NoSuchElementException;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * Class that makes it easier to write unfiltered iterators that filter or modify
 * the returned unfiltered.
 *
 * The methods you want to override are {@code computeNextStatic} and the {@code computeNext} methods.
 * All of these methods are allowed to return a {@code null} value with the meaning of ignoring
 * the entry.
 */
public abstract class AlteringUnfilteredRowIterator extends WrappingUnfilteredRowIterator
{
    private Row staticRow;
    private Unfiltered next;

    protected AlteringUnfilteredRowIterator(UnfilteredRowIterator wrapped)
    {
        super(wrapped);
    }

    protected Row computeNextStatic(Row row)
    {
        return row;
    }

    protected Row computeNext(Row row)
    {
        return row;
    }

    protected RangeTombstoneMarker computeNext(RangeTombstoneMarker marker)
    {
        return marker;
    }

    public Row staticRow()
    {
        if (staticRow == null)
        {
            Row row = computeNextStatic(super.staticRow());
            staticRow = row == null ? Rows.EMPTY_STATIC_ROW : row;
        }
        return staticRow;
    }

    public boolean hasNext()
    {
        while (next == null && super.hasNext())
        {
            Unfiltered unfiltered = super.next();
            if (unfiltered.isRow())
            {
                Row row = computeNext((Row)unfiltered);
                if (row != null && !row.isEmpty())
                    next = row;
            }
            else
            {
                next = computeNext((RangeTombstoneMarker)unfiltered);
            }
        }
        return next != null;
    }

    public Unfiltered next()
    {
        if (!hasNext())
            throw new NoSuchElementException();

        Unfiltered toReturn = next;
        next = null;
        return toReturn;
    }
}
