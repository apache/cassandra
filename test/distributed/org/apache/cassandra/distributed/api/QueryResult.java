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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A table of data representing a complete query result.
 *
 * A <code>QueryResult</code> is different from {@link java.sql.ResultSet} in several key ways:
 *
 * <ul>
 *     <li>represents a complete result rather than a cursor</li>
 *     <li>returns a {@link Row} to access the current row of data</li>
 *     <li>relies on object pooling; {@link #hasNext()} may return the same object just with different data, accessing a
 *     {@link Row} from a previous {@link #hasNext()} call has undefined behavior.</li>
 *     <li>includes {@link #filter(Predicate)}, this will do client side filtering since Apache Cassandra is more
 *     restrictive on server side filtering</li>
 * </ul>
 *
 * <h2>Unsafe patterns</h2>
 *
 * Below are a few unsafe patterns which may lead to unexpected results
 *
 * <code>{@code
 * while (rs.hasNext()) {
 *   list.add(rs.next());
 * }
 * }</code>
 *
 * <code>{@code
 * rs.forEach(list::add)
 * }</code>
 *
 * Both cases have the same issue; reference to a row from a previous call to {@link #hasNext()}.  Since the same {@link Row}
 * object can be used accross different calls to {@link #hasNext()} this would mean any attempt to access after the fact
 * points to newer data.  If this behavior is not desirable and access is needed between calls, then {@link Row#copy()}
 * should be used; this will clone the {@link Row} and return a new object pointing to the same data.
 */
public class QueryResult implements Iterator<Row>
{
    public static final QueryResult EMPTY = new QueryResult(new String[0], null);

    private final String[] names;
    private final Object[][] results;
    private final Predicate<Row> filter;
    private final Row row;
    private int offset = -1;

    public QueryResult(String[] names, Object[][] results)
    {
        this.names = Objects.requireNonNull(names, "names");
        this.results = results;
        this.row = new Row(names);
        this.filter = ignore -> true;
    }

    private QueryResult(String[] names, Object[][] results, Predicate<Row> filter, int offset)
    {
        this.names = names;
        this.results = results;
        this.filter = filter;
        this.offset = offset;
        this.row = new Row(names);
    }

    public String[] getNames()
    {
        return names;
    }

    public boolean isEmpty()
    {
        return results.length == 0;
    }

    public int size()
    {
        return results.length;
    }

    public QueryResult filter(Predicate<Row> fn)
    {
        return new QueryResult(names, results, filter.and(fn), offset);
    }

    /**
     * Get all rows as a 2d array.  Any calls to {@link #filter(Predicate)} will be ignored and the array returned will
     * be the full set from the query.
     */
    public Object[][] toObjectArrays()
    {
        return results;
    }

    @Override
    public boolean hasNext()
    {
        if (results == null)
            return false;
        while ((offset += 1) < results.length)
        {
            row.setResults(results[offset]);
            if (filter.test(row))
            {
                return true;
            }
        }
        row.setResults(null);
        return false;
    }

    @Override
    public Row next()
    {
        if (offset < 0 || offset >= results.length)
            throw new NoSuchElementException();
        return row;
    }
}
