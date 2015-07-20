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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;

/**
 * A utility class for writing partition iterators that filter/modify other
 * partition iterators.
 *
 * This work a little bit like Guava's AbstractIterator in that you only need
 * to implement the computeNext() method, though that method takes as argument
 * the UnfilteredRowIterator to filter from the wrapped partition iterator.
 */
public abstract class WrappingUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
{
    protected final UnfilteredPartitionIterator wrapped;

    private UnfilteredRowIterator next;

    protected WrappingUnfilteredPartitionIterator(UnfilteredPartitionIterator wrapped)
    {
        this.wrapped = wrapped;
    }

    public boolean isForThrift()
    {
        return wrapped.isForThrift();
    }

    public CFMetaData metadata()
    {
        return wrapped.metadata();
    }

    public boolean hasNext()
    {
        prepareNext();
        return next != null;
    }

    public UnfilteredRowIterator next()
    {
        prepareNext();
        assert next != null;

        UnfilteredRowIterator toReturn = next;
        next = null;
        return toReturn;
    }

    private void prepareNext()
    {
        while (next == null && wrapped.hasNext())
        {
            @SuppressWarnings("resource") // Closed on exception, right away if empty or ignored by computeNext, or if assigned to 'next',
                                          // either by the caller to next(), or in close().
            UnfilteredRowIterator wrappedNext = wrapped.next();
            try
            {
                UnfilteredRowIterator maybeNext = computeNext(wrappedNext);

                // As the wrappd iterator shouldn't return an empty iterator, if computeNext
                // gave us back it's input we save the isEmpty check.
                if (maybeNext != null && (isForThrift() || maybeNext == wrappedNext || !maybeNext.isEmpty()))
                {
                    next = maybeNext;
                    return;
                }
                else
                {
                    wrappedNext.close();
                }
            }
            catch (RuntimeException | Error e)
            {
                wrappedNext.close();
                throw e;
            }
        }
    }

    /**
     * Given the next UnfilteredRowIterator from the wrapped partition iterator, return
     * the (potentially modified) UnfilteredRowIterator to return. Please note that the
     * result will be skipped if it's either {@code null} of if it's empty.
     *
     * The default implementation return it's input unchanged to make it easier
     * to write wrapping partition iterators that only change the close method.
     */
    protected UnfilteredRowIterator computeNext(UnfilteredRowIterator iter)
    {
        return iter;
    }

    @Override
    public void close()
    {
        try
        {
            wrapped.close();
        }
        finally
        {
            if (next != null)
                next.close();
        }
    }
}
