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
package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;

public class NopCacheProvider implements CacheProvider<RowCacheKey, IRowCacheEntry>
{
    public ICache<RowCacheKey, IRowCacheEntry> create()
    {
        return new NopCache();
    }

    private static class NopCache implements ICache<RowCacheKey, IRowCacheEntry>
    {
        public long capacity()
        {
            return 0;
        }

        public void setCapacity(long capacity)
        {
            if (capacity != 0)
            {
                throw new UnsupportedOperationException("Setting capacity of " + NopCache.class.getSimpleName()
                                                        + " is not permitted as this cache is disabled. Check your yaml settings if you want to enable it.");
            }
        }

        public void put(RowCacheKey key, IRowCacheEntry value)
        {
        }

        public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value)
        {
            return false;
        }

        public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value)
        {
            return false;
        }

        public IRowCacheEntry get(RowCacheKey key)
        {
            return null;
        }

        public void remove(RowCacheKey key)
        {
        }

        public int size()
        {
            return 0;
        }

        public long weightedSize()
        {
            return 0;
        }

        public void clear()
        {
        }

        public Iterator<RowCacheKey> hotKeyIterator(int n)
        {
            return Collections.emptyIterator();
        }

        public Iterator<RowCacheKey> keyIterator()
        {
            return Collections.emptyIterator();
        }

        public boolean containsKey(RowCacheKey key)
        {
            return false;
        }
    }
}
