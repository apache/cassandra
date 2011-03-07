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
package org.apache.cassandra.utils;

import org.apache.cassandra.utils.obs.OpenBitSet;

public class PageCacheMetrics
{
    private final OpenBitSet pageMap;
    private final long fileLength;
    public final int pageSize;

    public PageCacheMetrics(int pageSize, long fileLength)
    {
        assert pageSize > 0;
        assert fileLength > 0;

        this.pageSize = pageSize;
        this.fileLength = fileLength;

        long numPages = (fileLength + pageSize - 1) / pageSize;
        pageMap = new OpenBitSet(numPages);

        assert isEmpty();
    }

    public boolean isEmpty()
    {
        return pageMap.cardinality() == 0;
    }

    public void setPage(long position)
    {
        pageMap.fastSet(getPageNumber(position));
    }

    private long getPageNumber(long position)
    {
        assert position <= fileLength;

        return (position + pageSize - 1) / pageSize;
    }

    /**
     * Returns true if any page within this range is cached
     * @param start starting position
     * @param end the limit of the range
     * @return true if given range is in the cache, false otherwise
     */
    public boolean isRangeInCache(long start, long end)
    {
        long startPage = getPageNumber(start);
        long endPage = getPageNumber(end);

        assert startPage <= endPage;

        for (long i = startPage; i < endPage; i++)
        {
            if (pageMap.fastGet(i))
                return true;
        }

        return false;
    }
}
