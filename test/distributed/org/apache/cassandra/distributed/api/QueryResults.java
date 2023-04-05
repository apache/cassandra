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
package org.apache.cassandra.distributed.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

public final class QueryResults
{
    private static final SimpleQueryResult EMPTY = new SimpleQueryResult(new String[0], null);

    private QueryResults() {}

    public static SimpleQueryResult empty()
    {
        return EMPTY;
    }

    public static QueryResult fromIterator(String[] names, Iterator<Row> iterator)
    {
        Objects.requireNonNull(names, "names");
        Objects.requireNonNull(iterator, "iterator");
        return new IteratorQueryResult(names, iterator);
    }

    public static QueryResult fromObjectArrayIterator(String[] names, Iterator<Object[]> iterator)
    {
        Row row = new Row(names);
        return fromIterator(names, new Iterator<Row>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Row next()
            {
                row.setResults(iterator.next());
                return row;
            }
        });
    }

    public static QueryResult filter(QueryResult result, Predicate<Row> fn)
    {
        return new FilterQueryResult(result, fn);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private static final int UNSET = -1;

        private int numColumns = UNSET;
        private String[] names;
        private final List<Object[]> results = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();

        public Builder columns(String... columns)
        {
            if (columns != null)
            {
                if (numColumns == UNSET)
                    numColumns = columns.length;

                if (numColumns != columns.length)
                    throw new AssertionError("Attempted to add column names with different column count; " +
                                             "expected " + numColumns + " columns but given " + Arrays.toString(columns));
            }

            names = columns;
            return this;
        }

        public Builder row(Object... values)
        {
            if (numColumns == UNSET)
                numColumns = values.length;

            if (numColumns != values.length)
                throw new AssertionError("Attempted to add row with different column count; " +
                                         "expected " + numColumns + " columns but given " + Arrays.toString(values));
            results.add(values);
            return this;
        }

        public Builder warning(String message)
        {
            warnings.add(message);
            return this;
        }

        public SimpleQueryResult build()
        {
            if (names == null)
            {
                if (numColumns == UNSET)
                    return QueryResults.empty();
                names = new String[numColumns];
                for (int i = 0; i < numColumns; i++)
                    names[i] = "unknown";
            }
            
            return new SimpleQueryResult(names, results.toArray(new Object[0][]), warnings);
        }
    }

    private static final class IteratorQueryResult implements QueryResult
    {
        private final List<String> names;
        private final Iterator<Row> iterator;

        private IteratorQueryResult(String[] names, Iterator<Row> iterator)
        {
            this(Collections.unmodifiableList(Arrays.asList(names)), iterator);
        }

        private IteratorQueryResult(List<String> names, Iterator<Row> iterator)
        {
            this.names = names;
            this.iterator = iterator;
        }

        @Override
        public List<String> names()
        {
            return names;
        }

        @Override
        public List<String> warnings()
        {
            throw new UnsupportedOperationException("Warnings are not yet supported for " + getClass().getSimpleName());
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public Row next()
        {
            return iterator.next();
        }
    }

    private static final class FilterQueryResult implements QueryResult
    {
        private final QueryResult delegate;
        private final Predicate<Row> filter;
        private Row current;

        private FilterQueryResult(QueryResult delegate, Predicate<Row> filter)
        {
            this.delegate = delegate;
            this.filter = filter;
        }

        @Override
        public List<String> names()
        {
            return delegate.names();
        }
        
        @Override 
        public List<String> warnings()
        {
            return delegate.warnings();
        }

        @Override
        public boolean hasNext()
        {
            while (delegate.hasNext())
            {
                Row row = delegate.next();
                if (filter.test(row))
                {
                    current = row;
                    return true;
                }
            }
            current = null;
            return false;
        }

        @Override
        public Row next()
        {
            if (current == null)
                throw new NoSuchElementException();
            return current;
        }
    }
}
