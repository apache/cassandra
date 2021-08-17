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

package org.apache.cassandra.fqltool;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.AbstractIterator;

import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * represents a resultset defined by the format in ResultStore on disk
 *
 * todo: Currently all iterators need to be consumed fully while iterating over result sets
 *       if this is created from a tailer. This can probably be improved, but for all current uses it is fine.
 */
public class StoredResultSet implements ResultHandler.ComparableResultSet
{
    private final ResultHandler.ComparableColumnDefinitions defs;
    public final boolean hasMoreResultSets;
    private final Supplier<Iterator<ResultHandler.ComparableRow>> rowIteratorSupplier;
    private final boolean wasFailed;
    private final Throwable failureException;

    /**
     * create a new StoredResultSet
     *
     * note that we use an iteratorSupplier to be able to iterate over the same in-memory rows several times *in tests*
     */
    public StoredResultSet(ResultHandler.ComparableColumnDefinitions defs,
                           boolean hasMoreResultSets,
                           boolean wasFailed,
                           Throwable failure,
                           Supplier<Iterator<ResultHandler.ComparableRow>> iteratorSupplier)
    {
        this.defs = defs;
        this.hasMoreResultSets = hasMoreResultSets;
        this.wasFailed = wasFailed;
        this.failureException = failure;
        this.rowIteratorSupplier = iteratorSupplier;
    }

    /**
     * creates a ComparableResultSet based on the data in tailer
     */
    public static StoredResultSet fromTailer(ExcerptTailer tailer)
    {
        ResultStore.ColumnDefsReader reader = new ResultStore.ColumnDefsReader();
        boolean hasMoreResultSets = tailer.readDocument(reader);
        ResultHandler.ComparableColumnDefinitions defs = new StoredComparableColumnDefinitions(reader.columnDefinitions,
                                                                                               reader.wasFailed,
                                                                                               new RuntimeException(reader.failureMessage));


        Iterator<ResultHandler.ComparableRow> rowIterator = new AbstractIterator<ResultHandler.ComparableRow>()
        {
            protected ResultHandler.ComparableRow computeNext()
            {
                ResultStore.RowReader rowReader = new ResultStore.RowReader();
                tailer.readDocument(rowReader);
                if (rowReader.isFinished)
                    return endOfData();
                return new StoredComparableRow(rowReader.rows, defs);
            }
        };

        return new StoredResultSet(defs,
                                   hasMoreResultSets,
                                   reader.wasFailed,
                                   new RuntimeException(reader.failureMessage),
                                   () -> rowIterator);
    }

    public static ResultHandler.ComparableResultSet failed(String failureMessage)
    {
        return new FailedComparableResultSet(new RuntimeException(failureMessage));
    }

    public Iterator<ResultHandler.ComparableRow> iterator()
    {
        return rowIteratorSupplier.get();
    }

    public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
    {
        return defs;
    }

    public boolean wasFailed()
    {
        return wasFailed;
    }

    public Throwable getFailureException()
    {
        return failureException;
    }

    static class StoredComparableRow implements ResultHandler.ComparableRow
    {
        private final List<ByteBuffer> row;
        private final ResultHandler.ComparableColumnDefinitions cds;

        public StoredComparableRow(List<ByteBuffer> row, ResultHandler.ComparableColumnDefinitions cds)
        {
            this.row = row;
            this.cds = cds;
        }

        public ByteBuffer getBytesUnsafe(int i)
        {
            return row.get(i);
        }

        public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
        {
            return cds;
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof StoredComparableRow))
                return false;
            return row.equals(((StoredComparableRow)other).row);
        }

        public int hashCode()
        {
            return Objects.hash(row, cds);
        }

        public String toString()
        {
            return row.stream().map(ByteBufferUtil::bytesToHex).collect(Collectors.joining(","));
        }
    }

    static class StoredComparableColumnDefinitions implements ResultHandler.ComparableColumnDefinitions
    {
        private final List<ResultHandler.ComparableDefinition> defs;
        private final boolean wasFailed;
        private final Throwable failureException;

        public StoredComparableColumnDefinitions(List<Pair<String, String>> cds, boolean wasFailed, Throwable failureException)
        {
            defs = cds != null ? cds.stream().map(StoredComparableDefinition::new).collect(Collectors.toList()) : Collections.emptyList();
            this.wasFailed = wasFailed;
            this.failureException = failureException;
        }
        public List<ResultHandler.ComparableDefinition> asList()
        {
            return wasFailed() ? Collections.emptyList() : defs;
        }

        public boolean wasFailed()
        {
            return wasFailed;
        }

        public Throwable getFailureException()
        {
            return failureException;
        }

        public int size()
        {
            return asList().size();
        }

        public Iterator<ResultHandler.ComparableDefinition> iterator()
        {
            return defs.iterator();
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof StoredComparableColumnDefinitions))
                return false;
            return defs.equals(((StoredComparableColumnDefinitions)other).defs);
        }

        public int hashCode()
        {
            return Objects.hash(defs, wasFailed, failureException);
        }

        public String toString()
        {
            return defs.toString();
        }
    }

    private static class StoredComparableDefinition implements ResultHandler.ComparableDefinition
    {
        private final Pair<String, String> p;

        public StoredComparableDefinition(Pair<String, String> p)
        {
            this.p = p;
        }
        public String getType()
        {
            return p.right;
        }

        public String getName()
        {
            return p.left;
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof StoredComparableDefinition))
                return false;
            return p.equals(((StoredComparableDefinition)other).p);
        }

        public int hashCode()
        {
            return Objects.hash(p);
        }

        public String toString()
        {
            return getName() + ':' + getType();
        }
    }

    private static class FailedComparableResultSet implements ResultHandler.ComparableResultSet
    {
        private final Throwable exception;

        public FailedComparableResultSet(Throwable exception)
        {
            this.exception = exception;
        }
        public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
        {
            return new ResultHandler.ComparableColumnDefinitions()
            {
                public List<ResultHandler.ComparableDefinition> asList()
                {
                    return Collections.emptyList();
                }

                public boolean wasFailed()
                {
                    return true;
                }

                public Throwable getFailureException()
                {
                    return exception;
                }

                public int size()
                {
                    return 0;
                }

                public Iterator<ResultHandler.ComparableDefinition> iterator()
                {
                    return asList().iterator();
                }
            };
        }

        public boolean wasFailed()
        {
            return true;
        }

        public Throwable getFailureException()
        {
            return new RuntimeException();
        }

        public Iterator<ResultHandler.ComparableRow> iterator()
        {
            return Collections.emptyListIterator();
        }
    }
}
