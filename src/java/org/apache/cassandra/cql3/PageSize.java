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
package org.apache.cassandra.cql3;

import java.util.Objects;
import java.util.StringJoiner;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.filter.DataLimits;

public class PageSize
{
    public static final PageSize NONE = new PageSize(DataLimits.NO_LIMIT, DataLimits.NO_LIMIT);

    public final int rows;
    public final int bytes;

    public PageSize(int rows, int bytes)
    {
        Preconditions.checkArgument(rows >= 0);
        Preconditions.checkArgument(bytes >= 0);
        this.rows = rows;
        this.bytes = bytes;
    }

    public static PageSize fromOptions(QueryOptions options)
    {
        int rows = options.getPageSize();
        if (rows <= 0)
            return NONE;
        return inRows(rows);
    }

    /**
     * Creates a page size representing {@code count} rows.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public static PageSize inRows(int rowsCount)
    {
        return new PageSize(rowsCount, DataLimits.NO_LIMIT);
    }

    /**
     * Creates a page size representing {@code size} bytes.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public static PageSize inBytes(int bytesCount)
    {
        return new PageSize(DataLimits.NO_LIMIT, bytesCount);
    }

    public boolean isDefined()
    {
        return bytes != DataLimits.NO_LIMIT || rows != DataLimits.NO_LIMIT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageSize pageSize = (PageSize) o;
        return rows == pageSize.rows && bytes == pageSize.bytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rows, bytes);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", PageSize.class.getSimpleName() + "[", "]")
               .add("rows=" + rows)
               .add("bytes=" + bytes)
               .toString();
    }
}
