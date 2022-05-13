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
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.monitoring.runtime.instrumentation.common.collect.Iterators;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.assertj.core.api.Assertions;

public class QueryResultUtil
{
    private QueryResultUtil()
    {
    }

    @SuppressWarnings("unchecked")
    public static SimpleQueryResult map(SimpleQueryResult input, Map<String, ? extends Function<?, Object>> mapper)
    {
        if (input.toObjectArrays().length == 0 || mapper == null || mapper.isEmpty())
            return input;
        for (String name : mapper.keySet())
        {
            if (!input.names().contains(name))
                throw new IllegalArgumentException("Unable to find column " + name);
        }
        Object[][] rows = input.toObjectArrays().clone();
        List<String> names = new ArrayList<>(mapper.keySet());
        int[] idxes = names.stream().mapToInt(input.names()::indexOf).toArray();
        for (int i = 0; i < rows.length; i++)
        {
            Object[] row = rows[i].clone();
            for (int j = 0; j < idxes.length; j++)
            {
                @SuppressWarnings("rawtypes") Function map = mapper.get(names.get(j));
                int idx = idxes[j];
                row[idx] = map.apply(row[idx]);
            }
            rows[i] = row;
        }
        return new SimpleQueryResult(input.names().toArray(new String[0]), rows, input.warnings());
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

        public SimpleQueryResultAssertHelper isEqualTo(SimpleQueryResult expectedResult)
        {
            qr.mark();
            expectedResult.mark();
            try
            {
                // org.apache.cassandra.distributed.shared.AssertUtils.assertRows has some issues with the error msg
                // so rewrite to make sure to have a nicer msg
                List<String> otherNames = qr.names().isEmpty() ? expectedResult.names() : qr.names();
                Assertions.assertThat(otherNames).describedAs("Column names do not match").isEqualTo(qr.names());
                int rowId = 0;
                while (qr.hasNext())
                {
                    if (!expectedResult.hasNext())
                        throw new AssertionError("Unexpected row at index " + rowId + "; found " + Arrays.toString(qr.next().toObjectArray()));
                    Row next = qr.next();
                    Row expected = expectedResult.next();
                    if (!Arrays.equals(next.toObjectArray(), expected.toObjectArray()))
                        throw new AssertionError("Expected row " + rowId + " to be " + Arrays.toString(expected.toObjectArray()) + " but was " + Arrays.toString(next.toObjectArray()));

                    rowId++;
                }
                if (expectedResult.hasNext())
                    throw new AssertionError("Expected row " + rowId + " to be " + Arrays.toString(expectedResult.next().toObjectArray()) + " but was missing");

                AssertUtils.assertRows(qr, expectedResult);
            }
            finally
            {
                qr.reset();
                expectedResult.reset();
            }
            return this;
        }

        public SimpleQueryResultAssertHelper isEqualTo(Object... values)
        {
            Assertions.assertThat(qr.toObjectArrays())
                      .hasSize(1)
                      .contains(values);
            return this;
        }

        public SimpleQueryResultAssertHelper hasSize(int size)
        {
            Assertions.assertThat(qr.toObjectArrays()).hasSize(size);
            return this;
        }

        public SimpleQueryResultAssertHelper hasSizeGreaterThan(int size)
        {
            Assertions.assertThat(qr.toObjectArrays()).hasSizeGreaterThan(size);
            return this;
        }

        public void isEmpty()
        {
            int size = Iterators.size(qr);
            Assertions.assertThat(size).describedAs("QueryResult is not empty").isEqualTo(0);
        }
    }
}
