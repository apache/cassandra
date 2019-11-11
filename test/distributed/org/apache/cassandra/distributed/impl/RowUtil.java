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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.transport.messages.ResultMessage;

public class RowUtil
{
    public static Object[][] toObjects(ResultMessage.Rows rows)
    {
        Object[][] result = new Object[rows.result.rows.size()][];
        List<ColumnSpecification> specs = rows.result.metadata.names;
        for (int i = 0; i < rows.result.rows.size(); i++)
        {
            List<ByteBuffer> row = rows.result.rows.get(i);
            result[i] = new Object[row.size()];
            for (int j = 0; j < row.size(); j++)
            {
                ByteBuffer bb = row.get(j);

                if (bb != null)
                    result[i][j] = specs.get(j).type.getSerializer().deserialize(bb);
            }
        }
        return result;
    }

    public static Iterator<Object[]> toObjects(UntypedResultSet rs)
    {
        return toObjects(rs.metadata(), rs.iterator());
    }

    public static Iterator<Object[]> toObjects(List<ColumnSpecification> columnSpecs, Iterator<UntypedResultSet.Row> rs)
    {
        return Iterators.transform(rs,
                                   (row) -> {
                                       Object[] objectRow = new Object[columnSpecs.size()];
                                       for (int i = 0; i < columnSpecs.size(); i++)
                                       {
                                           ColumnSpecification columnSpec = columnSpecs.get(i);
                                           ByteBuffer bb = row.getBytes(columnSpec.name.toString());

                                           if (bb != null)
                                               objectRow[i] = columnSpec.type.getSerializer().deserialize(bb);
                                       }
                                       return objectRow;
                                   });
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


}
