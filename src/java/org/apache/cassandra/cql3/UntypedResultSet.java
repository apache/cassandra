/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.cql3.ResultSet;

/** a utility for doing internal cql-based queries */
public class UntypedResultSet implements Iterable<UntypedResultSet.Row>
{
    private final ResultSet cqlRows;

    public UntypedResultSet(ResultSet cqlRows)
    {
        this.cqlRows = cqlRows;
    }

    public boolean isEmpty()
    {
        return cqlRows.size() == 0;
    }

    public int size()
    {
        return cqlRows.size();
    }

    public Row one()
    {
        if (cqlRows.rows.size() != 1)
            throw new IllegalStateException("One row required, " + cqlRows.rows.size() + " found");
        return new Row(cqlRows.metadata.names, cqlRows.rows.get(0));
    }

    public Iterator<Row> iterator()
    {
        return new AbstractIterator<Row>()
        {
            Iterator<List<ByteBuffer>> iter = cqlRows.rows.iterator();

            protected Row computeNext()
            {
                if (!iter.hasNext())
                    return endOfData();
                return new Row(cqlRows.metadata.names, iter.next());
            }
        };
    }

    public static class Row
    {
        Map<String, ByteBuffer> data = new HashMap<String, ByteBuffer>();

        public Row(List<ColumnSpecification> names, List<ByteBuffer> columns)
        {
            for (int i = 0; i < names.size(); i++)
                data.put(names.get(i).toString(), columns.get(i));
        }

        public boolean has(String column)
        {
            // Note that containsKey won't work because we may have null values
            return data.get(column) != null;
        }

        public String getString(String column)
        {
            return UTF8Type.instance.compose(data.get(column));
        }

        public boolean getBoolean(String column)
        {
            return BooleanType.instance.compose(data.get(column));
        }

        public int getInt(String column)
        {
            return Int32Type.instance.compose(data.get(column));
        }

        public double getDouble(String column)
        {
            return DoubleType.instance.compose(data.get(column));
        }

        public ByteBuffer getBytes(String column)
        {
            return data.get(column);
        }

        public InetAddress getInetAddress(String column)
        {
            return InetAddressType.instance.compose(data.get(column));
        }

        public UUID getUUID(String column)
        {
            return UUIDType.instance.compose(data.get(column));
        }

        @Override
        public String toString()
        {
            return data.toString();
        }
    }
}
