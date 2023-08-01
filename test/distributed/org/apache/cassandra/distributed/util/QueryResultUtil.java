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
package org.apache.cassandra.distributed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import com.google.monitoring.runtime.instrumentation.common.collect.Iterators;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Index;

public class QueryResultUtil
{
    private QueryResultUtil()
    {
    }

    public static void orderBy(SimpleQueryResult qr, String... columns)
    {
        if (columns == null || columns.length == 0)
            throw new IllegalArgumentException("No columns defined");
        int[] index = new int[columns.length];
        {
            int offset = 0;
            for (String name : columns)
            {
                int idx = qr.names().indexOf(name);
                if (idx == -1)
                    throw new IllegalArgumentException("Unknown column " + name);
                index[offset++] = idx;
            }
        }
        qr.reset();
        Arrays.sort(qr.toObjectArrays(), (a, b) -> {
            for (int i = 0; i < index.length; i++)
            {
                int idx = index[i];
                Object ao = a[idx];
                Object bo = b[idx];
                if (ao == null && bo == null) return 0;
                if (ao == null) return 1;
                if (bo == null) return -1;
                int rc = ((Comparable) ao).compareTo(bo);
                if (rc != 0) return rc;
            }
            return 0;
        });
    }

    public static boolean contains(SimpleQueryResult qr, Object... values)
    {
        return contains(qr, a -> equals(a, values));
    }

    public static boolean contains(SimpleQueryResult qr, Row row)
    {
        return contains(qr, a -> equals(a, row));
    }

    public static boolean contains(SimpleQueryResult qr, Predicate<Row> fn)
    {
        while (qr.hasNext())
        {
            if (fn.test(qr.next()))
                return true;
        }
        return false;
    }

    private static boolean equals(Row a, Row b)
    {
        return equals(a, b.toObjectArray());
    }

    private static boolean equals(Row a, Object[] bs)
    {
        Object[] as = a.toObjectArray();
        if (as.length != bs.length)
            return false;
        for (int i = 0; i < as.length; i++)
        {
            if (!Objects.equals(as[i], bs[i]))
                return false;
        }
        return true;
    }

    public static SimpleQueryResultAssertHelper assertThat(SimpleQueryResult qr)
    {
        return new SimpleQueryResultAssertHelper(qr);
    }

    public static RowAssertHelper assertThat(Row row)
    {
        return new RowAssertHelper(row);
    }

    public static String expand(SimpleQueryResult qr)
    {
        StringBuilder sb = new StringBuilder();
        int rowNum = 1;
        while (qr.hasNext())
        {
            sb.append("@ Row ").append(rowNum).append('\n');
            TableBuilder table = new TableBuilder('|');
            Row next = qr.next();
            for (String column : qr.names())
            {
                Object value = next.get(column);
                table.add(column, value == null ? null : value.toString());
            }
            sb.append(table);
        }
        return sb.toString();
    }

    public static QueryBuilder query(SimpleQueryResult result)
    {
        return new QueryBuilder(result);
    }

    public static class RowAssertHelper
    {
        private final Row row;

        public RowAssertHelper(Row row)
        {
            this.row = row;
        }

        public RowAssertHelper isEqualTo(String column, Object expected)
        {
            Object actual = row.get(column);
            Assertions.assertThat(actual).describedAs("Column %s had unexpected value", column).isEqualTo(expected);
            return this;
        }

        public RowAssertHelper columnsEqualTo(String first, String... others)
        {
            Object expected = row.get(first);
            for (String other : others)
                Assertions.assertThat(row.<Object>get(other)).describedAs("Columns %s and %s are not equal", first, other).isEqualTo(expected);
            return this;
        }
    }

    public static class SimpleQueryResultAssertHelper
    {
        private final SimpleQueryResult qr;

        private SimpleQueryResultAssertHelper(SimpleQueryResult qr)
        {
            this.qr = qr;
        }

        public SimpleQueryResultAssertHelper contains(Object... values)
        {
            qr.reset();
            if (!QueryResultUtil.contains(qr, a -> QueryResultUtil.equals(a, values)))
                throw new AssertionError("Row " + Arrays.asList(values) + " is not present");
            return this;
        }

        public SimpleQueryResultAssertHelper contains(Row row)
        {
            qr.reset();
            if (!QueryResultUtil.contains(qr, a -> QueryResultUtil.equals(a, row)))
                throw new AssertionError("Row " + row + " is not present");
            return this;
        }

        public SimpleQueryResultAssertHelper contains(Predicate<Row> fn)
        {
            qr.reset();
            if (!QueryResultUtil.contains(qr, fn))
                throw new AssertionError("Row  is not present");
            return this;
        }

        public SimpleQueryResultAssertHelper isEqualTo(Object... values)
        {
            Assertions.assertThat(qr.toObjectArrays())
                      .hasNumberOfRows(1)
                      .contains(values, Index.atIndex(0));
            return this;
        }

        public SimpleQueryResultAssertHelper hasSize(int size)
        {
            Assertions.assertThat(qr.toObjectArrays()).hasNumberOfRows(size);
            return this;
        }

        public SimpleQueryResultAssertHelper hasSizeGreaterThan(int size)
        {
            Assertions.assertThat((qr.toObjectArrays()).length).isGreaterThan(size);
            return this;
        }

        public void isEmpty()
        {
            int size = Iterators.size(qr);
            Assertions.assertThat(size).describedAs("QueryResult is not empty").isZero();
        }
    }

    public static class QueryBuilder
    {
        private final SimpleQueryResult input;
        private final List<String> names = new ArrayList<>();
        private Predicate<Row> filter = ignore -> true;

        public QueryBuilder(SimpleQueryResult input)
        {
            this.input = input;
        }

        public QueryBuilder select(String... names)
        {
            for (String name : names)
            {
                if (!input.names().contains(name))
                    throw new IllegalArgumentException("Unknown column " + name);
            }
            this.names.clear();
            // if names is empty, then this becomes "SELECT *"
            this.names.addAll(Arrays.asList(names));
            return this;
        }

        public QueryBuilder filter(Predicate<Row> fn)
        {
            this.filter = fn;
            return this;
        }

        public SimpleQueryResult build()
        {
            QueryResults.Builder builder = QueryResults.builder();
            if (names.isEmpty())
            {
                builder.columns(input.names().toArray(new String[0]));
                while (input.hasNext())
                {
                    Row row = input.next();
                    if (filter.test(row))
                        builder.row(row.toObjectArray());
                }
            }
            else
            {
                String[] names = this.names.toArray(new String[0]);
                builder.columns(names);
                int[] index = new int[names.length];
                {
                    int offset = 0;
                    for (String name : names)
                        index[offset++] = input.names().indexOf(name);
                }
                Row row = new Row(names);
                while (input.hasNext())
                {
                    Object[] raw = input.next().toObjectArray();
                    Object[] updated = new Object[index.length];
                    for (int i = 0; i < index.length; i++)
                        updated[i] = raw[index[i]];
                    row.setResults(updated);
                    if (filter.test(row))
                        builder.row(updated);
                }
            }
            return builder.build();
        }
    }
}
