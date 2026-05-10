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

package org.apache.cassandra.distributed.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RowUtil
{
    private static final ByteBuffer[][] EMPTY_BB_ROWS = new ByteBuffer[0][];

    public static SimpleQueryResult toQueryResult(ResultMessage res)
    {
        return toQueryResult(res, true);
    }

    public static SimpleQueryResult toQueryResult(ResultMessage res, boolean deserialize)
    {
        if (res != null && res.kind == ResultMessage.Kind.ROWS)
        {
            ResultMessage.Rows rows = (ResultMessage.Rows) res;
            String[] names = getColumnNames(rows.result.metadata.requestNames());
            Object[][] results = toObjects(rows, deserialize);

            // Warnings may be null here, due to ClientWarn#getWarnings() handling of empty warning lists.
            List<String> warnings = res.getWarnings();

            return new SimpleQueryResult(names, results, warnings == null ? Collections.emptyList() : warnings);
        }
        else
        {
            if (res != null)
            {
                List<String> warnings = res.getWarnings();
                return new SimpleQueryResult(new String[0], null, warnings == null ? Collections.emptyList() : warnings);
            }
            else
                return QueryResults.empty();
        }
    }

    public static String[] getColumnNames(List<ColumnSpecification> names)
    {
        return names.stream().map(c -> c.name.toString()).toArray(String[]::new);
    }

    public static Object[][] toObjects(ResultMessage.Rows rows, boolean deserialize)
    {
        return toObjects(rows.result.metadata.requestNames(), rows.result.rows, deserialize);
    }

    public static Object[][] toObjects(List<ColumnSpecification> specs, List<List<byte[]>> rows)
    {
        return toObjects(specs, rows, true);
    }

    public static Object[][] toObjects(List<ColumnSpecification> specs, List<List<byte[]>> rows, boolean deserialize)
    {
        Object[][] result = new Object[rows.size()][];
        for (int i = 0; i < rows.size(); i++)
        {
            List<byte[]> row = rows.get(i);
            result[i] = new Object[specs.size()];
            for (int j = 0; j < specs.size(); j++)
            {
                byte[] bytes = row.get(j);

                if (bytes != null)
                {
                    result[i][j] = deserialize ? specs.get(j).type.getSerializer().deserialize(bytes, ByteArrayAccessor.instance) : ByteBuffer.wrap(bytes);
                }
            }
        }
        return result;
    }

    /**
     * When {@code deserialize=false} then all values are {@link ByteBuffer}, so this function can be used to convert the {@code Object[][]} to
     * its {@link ByteBuffer} form.
     *
     * Calling this function if {@code  deserialize=true} should be expected to fail, but should not be relied on; it is
     * safer to assume the behavior is undefined in this case.
     */
    public static ByteBuffer[][] toByteBuffer(Object[][] rows)
    {
        if (rows.length == 0) return EMPTY_BB_ROWS;
        ByteBuffer[][] result = new ByteBuffer[rows.length][];
        for (int i = 0; i < rows.length; i++)
        {
            Object[] in = rows[i];
            ByteBuffer[] out = new ByteBuffer[in.length];
            for (int j = 0; j < in.length; j++)
                out[j] = (ByteBuffer) in[j];
            result[i] = out;
        }
        return result;
    }

    public static Iterator<Object[]> toObjects(ResultSet rs)
    {
        return Iterators.transform(rs.iterator(), (Row row) -> {
            final int numColumns = rs.getColumnDefinitions().size();
            Object[] objectRow = new Object[numColumns];
            for (int i = 0; i < numColumns; i++)
            {
                objectRow[i] = row.getObject(i);
            }
            return objectRow;
        });
    }

    public static Iterator<Object[]> toIter(UntypedResultSet rs)
    {
        return toIter(rs.metadata(), rs.iterator());
    }

    public static Iterator<Object[]> toIter(ResultMessage.Rows rows)
    {
        return toIterInternal(rows.result.metadata.names, rows.result.rows);
    }

    public static Iterator<Object[]> toIter(List<ColumnSpecification> columnSpecs, Iterator<UntypedResultSet.Row> rs)
    {
        Iterator<List<byte[]>> iter = Iterators.transform(rs,
                                                          (row) -> {
                                                              List<byte[]> bbs = new ArrayList<>(columnSpecs.size());
                                                              for (int i = 0; i < columnSpecs.size(); i++)
                                                              {
                                                                  ColumnSpecification columnSpec = columnSpecs.get(i);
                                                                  ByteBuffer bb = row.getBytes(columnSpec.name.toString());
                                                                  bbs.add(ByteBufferUtil.getArrayUnsafeNullable(bb));
                                                              }
                                                              return bbs;
                                                          });
        return toIterInternal(columnSpecs, Lists.newArrayList(iter));
    }

    private static Iterator<Object[]> toIterInternal(List<ColumnSpecification> columnSpecs, List<List<byte[]>> rs)
    {
        return Iterators.transform(rs.iterator(),
                                   (row) -> {
                                       Object[] objectRow = new Object[columnSpecs.size()];
                                       for (int i = 0; i < columnSpecs.size(); i++)
                                       {
                                           ColumnSpecification columnSpec = columnSpecs.get(i);
                                           byte[] bytes = row.get(i);

                                           if (bytes != null)
                                               objectRow[i] = columnSpec.type.getSerializer().deserialize(bytes, ByteArrayAccessor.instance);

                                       }
                                       return objectRow;
                                   });
    }
}
