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

import com.google.common.base.Preconditions;

public class PageSize
{
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public static final PageSize NONE = new PageSize(NO_LIMIT, PageUnit.ROWS);

    public enum PageUnit
    {
        ROWS, BYTES
    }

    private final int size;
    private final PageUnit unit;

    public PageSize(int size, PageUnit unit)
    {
        Preconditions.checkArgument(size >= 0);
        Preconditions.checkNotNull(unit);
        this.size = size;
        this.unit = unit;
    }

    public int getSize()
    {
        return size;
    }

    public PageUnit getUnit()
    {
        return unit;
    }

    public int bytes()
    {
        return unit == PageUnit.BYTES ? size : NO_LIMIT;
    }

    public int rows()
    {
        return unit == PageUnit.ROWS ? size : NO_LIMIT;
    }

    /**
     * Creates a page size representing {@code count} rows.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public static PageSize inRows(int rowsCount)
    {
        return new PageSize(rowsCount, PageUnit.ROWS);
    }

    /**
     * Creates a page size representing {@code size} bytes.
     *
     * @throws IllegalArgumentException if the size is not strictly positive.
     */
    public static PageSize inBytes(int bytesCount)
    {
        return new PageSize(bytesCount, PageUnit.BYTES);
    }

    /**
     * Returns the minimum number of rows for the given number and the number of rows represented by this page size.
     * If this page size is defined in bytes or undefined, it will just return the provided number of rows.
     */
    public int minRowsCount(int rowsCount)
    {
        return unit == PageUnit.ROWS ? Math.min(rowsCount, size) : rowsCount;
    }

    /**
     * Returns the minimum number of bytes for the given number and the number of bytes represented by this page size.
     * If this page size is defined in rows or undefined, it will just return the provided number of bytes.
     */
    public int minBytesCount(int bytesCount)
    {
        return unit == PageUnit.BYTES ? Math.min(bytesCount, size) : bytesCount;
    }

    public boolean isDefined()
    {
        return size < NO_LIMIT;
    }

    public PageSize withDecreasedRows(int rowsCount) {
        return unit == PageUnit.ROWS && size != NO_LIMIT
               ? inRows(Math.max(0, size - rowsCount))
               : this;
    }

    public PageSize withDecreasedBytes(int bytesCount) {
        return unit == PageUnit.BYTES && size != NO_LIMIT
               ? inBytes(Math.max(0, size - bytesCount))
               : this;
    }

    /**
     * Assuming we went through the provided number of rows/bytes, it returns whether the page is completed.
     * It will always return {@code false} if the page size is undefined (unlimited).
     */
    public boolean isCompleted(int count, PageUnit unit)
    {
        return this.unit == unit && this.size <= count;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageSize pageSize = (PageSize) o;
        return size == pageSize.size && unit == pageSize.unit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(size, unit);
    }

    @Override
    public String toString()
    {
        if (size == NO_LIMIT)
            return "unlimited";
        else
            return size + " " + unit.name().toLowerCase();
    }
}
