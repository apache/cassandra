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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A table of data representing a complete query result.
 * <p>
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
 * <p>
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
 * <p>
 * Both cases have the same issue; reference to a row from a previous call to {@link #hasNext()}.  Since the same {@link Row}
 * object can be used accross different calls to {@link #hasNext()} this would mean any attempt to access after the fact
 * points to newer data.  If this behavior is not desirable and access is needed between calls, then {@link Row#copy()}
 * should be used; this will clone the {@link Row} and return a new object pointing to the same data.
 */
public class SimpleQueryResult implements QueryResult
{
    private final String[] names;
    private final Object[][] results;
    private final List<String> warnings;
    private final Predicate<Row> filter;
    private final Row row;
    private int offset = -1;

    public SimpleQueryResult(String[] names, Object[][] results)
    {
        this(names, results, Collections.emptyList());
    }

    public SimpleQueryResult(String[] names, Object[][] results, List<String> warnings)
    {
        this.names = Objects.requireNonNull(names, "names");
        this.results = results;
        this.warnings = Objects.requireNonNull(warnings, "warnings");
        this.row = new Row(names);
        this.filter = ignore -> true;
    }

    private SimpleQueryResult(String[] names, Object[][] results, Predicate<Row> filter, int offset)
    {
        this.names = names;
        this.results = results;
        this.warnings = Collections.emptyList();
        this.filter = filter;
        this.offset = offset;
        this.row = new Row(names);
    }

    public List<String> names()
    {
        return Collections.unmodifiableList(Arrays.asList(names));
    }

    @Override
    public List<String> warnings()
    {
        return Collections.unmodifiableList(warnings);
    }

    public SimpleQueryResult filter(Predicate<Row> fn)
    {
        return new SimpleQueryResult(names, results, filter.and(fn), offset);
    }

    /**
     * Reset the cursor to the start of the query result; if the query result has not been iterated, this has no effect.
     */
    public void reset()
    {
        offset = -1;
        row.setResults(null);
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
        // no null check needed for results since offset only increments IFF results is not null
        if (offset < 0 || offset >= results.length)
            throw new NoSuchElementException();
        return row;
    }

    @Override
    public String toString() {
        if (results == null)
            return "[]";
        return Stream.of(results)
                     .map(Arrays::toString)
                     .collect(Collectors.joining(",", "[", "]"));
    }
}
