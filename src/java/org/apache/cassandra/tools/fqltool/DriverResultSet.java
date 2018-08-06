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

package org.apache.cassandra.tools.fqltool;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.AbstractIterator;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * Wraps a result set from the driver so that we can reuse the compare code when reading
 * up a result set produced by ResultStore.
 */
public class DriverResultSet implements ResultHandler.ComparableResultSet
{
    private final ResultSet resultSet;

    public DriverResultSet(ResultSet resultSet)
    {
        this.resultSet = resultSet;
    }
    public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
    {
        return new DriverColumnDefinitions(resultSet.getColumnDefinitions());
    }

    public Iterator<ResultHandler.ComparableRow> iterator()
    {
        return new AbstractIterator<ResultHandler.ComparableRow>()
        {
            Iterator<Row> iter = resultSet.iterator();
            protected ResultHandler.ComparableRow computeNext()
            {
                if (iter.hasNext())
                {
                    return new DriverRow(iter.next());
                }
                return endOfData();
            }
        };
    }

    public static class DriverRow implements ResultHandler.ComparableRow
    {
        private final Row row;

        public DriverRow(Row row)
        {
            this.row = row;
        }

        public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
        {
            return new DriverColumnDefinitions(row.getColumnDefinitions());
        }

        public ByteBuffer getBytesUnsafe(int i)
        {
            return row.getBytesUnsafe(i);
        }

        @Override
        public boolean equals(Object oo)
        {
            if (!(oo instanceof ResultHandler.ComparableRow))
                return false;

            ResultHandler.ComparableRow o = (ResultHandler.ComparableRow)oo;
            if (getColumnDefinitions().size() != o.getColumnDefinitions().size())
                return false;

            for (int j = 0; j < getColumnDefinitions().size(); j++)
            {
                ByteBuffer b1 = getBytesUnsafe(j);
                ByteBuffer b2 = o.getBytesUnsafe(j);

                if (b1 != null && b2 != null && !b1.equals(b2))
                {
                    return false;
                }
                if (b1 == null && b2 != null || b2 == null && b1 != null)
                {
                    return false;
                }
            }
            return true;
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            List<ResultHandler.ComparableDefinition> colDefs = getColumnDefinitions().asList();
            for (int i = 0; i < getColumnDefinitions().size(); i++)
            {
                ByteBuffer bb = getBytesUnsafe(i);
                String row = bb != null ? ByteBufferUtil.bytesToHex(bb) : "NULL";
                sb.append(colDefs.get(i)).append(':').append(row).append(",");
            }
            return sb.toString();
        }
    }

    public static class DriverColumnDefinitions implements ResultHandler.ComparableColumnDefinitions
    {
        private final ColumnDefinitions columnDefinitions;

        public DriverColumnDefinitions(ColumnDefinitions columnDefinitions)
        {
            this.columnDefinitions = columnDefinitions;
        }

        public List<ResultHandler.ComparableDefinition> asList()
        {
            return columnDefinitions.asList().stream().map(DriverDefinition::new).collect(Collectors.toList());
        }

        public int size()
        {
            return columnDefinitions.size();
        }

        public Iterator<ResultHandler.ComparableDefinition> iterator()
        {
            return new AbstractIterator<ResultHandler.ComparableDefinition>()
            {
                Iterator<ColumnDefinitions.Definition> iter = columnDefinitions.iterator();
                protected ResultHandler.ComparableDefinition computeNext()
                {
                    if (iter.hasNext())
                        return new DriverDefinition(iter.next());
                    return endOfData();
                }
            };
        }

        public boolean equals(Object oo)
        {
            if (!(oo instanceof ResultHandler.ComparableColumnDefinitions))
                return false;

            ResultHandler.ComparableColumnDefinitions o = (ResultHandler.ComparableColumnDefinitions)oo;
            if (size() != o.size())
                return false;

            List<ResultHandler.ComparableDefinition> def1 = asList();
            List<ResultHandler.ComparableDefinition> def2 = o.asList();

            for (int j = 0; j < def1.size(); j++)
            {
                if (!def1.get(j).equals(def2.get(j)))
                {
                    return false;
                }
            }
            return true;
        }
    }

    public static class DriverDefinition implements ResultHandler.ComparableDefinition
    {
        private final ColumnDefinitions.Definition def;

        public DriverDefinition(ColumnDefinitions.Definition def)
        {
            this.def = def;
        }

        public String getType()
        {
            return def.getType().toString();
        }

        public String getName()
        {
            return def.getName();
        }

        public boolean equals(Object oo)
        {
            if (!(oo instanceof ResultHandler.ComparableDefinition))
                return false;

            return def.equals(((DriverDefinition)oo).def);
        }

        public String toString()
        {
            return getName() + ':' + getType();
        }
    }

}
