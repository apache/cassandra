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
package org.apache.cassandra.utils;

import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputPlus;

public final class PageAware
{
    public static final int PAGE_SIZE = 4096; // must be a power of two
    public static final int PAGE_SIZE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);

    /**
     * Calculate the end of the page identified by the given position.
     * Equivalent to floor(dstPos / PAGE_SIZE + 1) * PAGE_SIZE.
     * <p>
     * When the argument is equal to the page boundary, returns the next page boundary. E.g. pageLimit(0) == PAGE_SIZE.
     */
    public static long pageLimit(long dstPos)
    {
        return (dstPos | (PAGE_SIZE - 1)) + 1;
    }

    /**
     * Calculate the start of the page that contains the given position.
     * Equivalent to floor(dstPos / PAGE_SIZE) * PAGE_SIZE.
     */
    public static long pageStart(long dstPos)
    {
        return dstPos & -PAGE_SIZE;
    }

    /**
     * Calculate the earliest page boundary for the given position.
     * Equivalent to ceil(dstPos / PAGE_SIZE) * PAGE_SIZE.
     * <p>
     * When the argument is equal to a page boundary, returns the argument.
     */
    public static long padded(long dstPos)
    {
        return pageStart(dstPos + PAGE_SIZE - 1);
    }

    /**
     * Calculate the number of pages that fit in the given size, rounded up to a page if the size is not an exact multiple.
     *
     * @param size the size that needs to cover a number of pages
     * @return the number of pages, rounded up
     */
    public static int numPages(int size)
    {
        return (size + PAGE_SIZE - 1) >> PAGE_SIZE_SHIFT;
    }

    /**
     * Given a position relative to the start of a number of pages, determine the exact page number this
     * position falls into. For example, positions from 0 to {@link #PAGE_SIZE} -1 will fall into page zero
     * and so forth.
     *
     * @param dstPos the position
     * @return the page number, indexed at zero
     */
    public static int pageNum(long dstPos)
    {
        return Math.toIntExact(dstPos >> PAGE_SIZE_SHIFT);
    }

    /**
     * Pad the given output stream with zeroes until the next page boundary.
     * If the destination position is already at a page boundary, do not do anything.
     */
    public static void pad(DataOutputPlus dest) throws IOException
    {
        long position = dest.position();
        long bytesLeft = padded(position) - position;
        dest.write(EmptyPage.EMPTY_PAGE, 0, (int) bytesLeft);
    }

    static class EmptyPage
    {
        static final byte[] EMPTY_PAGE = new byte[PAGE_SIZE];
    }
}