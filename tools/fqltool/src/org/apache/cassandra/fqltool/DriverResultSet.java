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

package org.apache.cassandra.fqltool;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
    private final Throwable failureException;

    public DriverResultSet(ResultSet resultSet)
    {
        this(resultSet, null);
    }

    private DriverResultSet(ResultSet res, Throwable failureException)
    {
        resultSet = res;
        this.failureException = failureException;
    }

    public static DriverResultSet failed(Throwable ex)
    {
        return new DriverResultSet(null, ex);
    }

    public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
    {
        if (wasFailed())
            return new DriverColumnDefinitions(null, true, failureException);

        return new DriverColumnDefinitions(resultSet.getColumnDefinitions());
    }

    public boolean wasFailed()
    {
        return failureException != null;
    }

    public Throwable getFailureException()
    {
        return failureException;
    }

    public Iterator<ResultHandler.ComparableRow> iterator()
    {
        if (wasFailed())
            return Collections.emptyListIterator();
        return new AbstractIterator<ResultHandler.ComparableRow>()
        {
            Iterator<Row> iter = resultSet.iterator();
            protected ResultHandler.ComparableRow computeNext()
            {
                if (iter.hasNext())
                    return new DriverRow(iter.next());
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

        public int hashCode()
        {
            return Objects.hash(row);
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
        private final boolean failed;
        private final Throwable failureException;

        public DriverColumnDefinitions(ColumnDefinitions columnDefinitions)
        {
            this(columnDefinitions, false, null);
        }

        private DriverColumnDefinitions(ColumnDefinitions columnDefinitions, boolean failed, Throwable failureException)
        {
            this.columnDefinitions = columnDefinitions;
            this.failed = failed;
            this.failureException = failureException;
        }

        public List<ResultHandler.ComparableDefinition> asList()
        {
            if (wasFailed())
                return Collections.emptyList();
            return columnDefinitions.asList().stream().map(DriverDefinition::new).collect(Collectors.toList());
        }

        public boolean wasFailed()
        {
            return failed;
        }

        public Throwable getFailureException()
        {
            return failureException;
        }

        public int size()
        {
            return columnDefinitions.size();
        }

        public Iterator<ResultHandler.ComparableDefinition> iterator()
        {
            return asList().iterator();
        }

        public boolean equals(Object oo)
        {
            if (!(oo instanceof ResultHandler.ComparableColumnDefinitions))
                return false;

            ResultHandler.ComparableColumnDefinitions o = (ResultHandler.ComparableColumnDefinitions)oo;
            if (wasFailed() && o.wasFailed())
                return true;

            if (size() != o.size())
                return false;

            return asList().equals(o.asList());
        }

        public int hashCode()
        {
            return Objects.hash(columnDefinitions, failed, failureException);
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

        public int hashCode()
        {
            return Objects.hash(def);
        }

        public String toString()
        {
            return getName() + ':' + getType();
        }
    }

}
