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
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

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
 * object can be used across different calls to {@link #hasNext()} this would mean any attempt to access after the fact
 * points to newer data.  If this behavior is not desirable and access is needed between calls, then {@link Row#copy()}
 * should be used; this will clone the {@link Row} and return a new object pointing to the same data.
 */
public interface QueryResult extends Iterator<Row>
{
    List<String> names();
    
    List<String> warnings();

    default QueryResult filter(Predicate<Row> fn)
    {
        return QueryResults.filter(this, fn);
    }

    default <A> Iterator<A> map(Function<? super Row, ? extends A> fn)
    {
        return new Iterator<A>()
        {
            @Override
            public boolean hasNext()
            {
                return QueryResult.this.hasNext();
            }

            @Override
            public A next()
            {
                return fn.apply(QueryResult.this.next());
            }
        };
    }
}
