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
package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

// CQL: {'keys' : 'ALL'|'NONE', 'rows_per_partition': '200'|'NONE'|'ALL'}
public final class CachingParams
{
    public enum Option
    {
        KEYS,
        ROWS_PER_PARTITION;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private static final String ALL = "ALL";
    private static final String NONE = "NONE";

    static final boolean DEFAULT_CACHE_KEYS = true;
    static final int DEFAULT_ROWS_PER_PARTITION_TO_CACHE = 0;

    public static final CachingParams CACHE_NOTHING = new CachingParams(false, 0);
    public static final CachingParams CACHE_KEYS = new CachingParams(true, 0);
    public static final CachingParams CACHE_EVERYTHING = new CachingParams(true, Integer.MAX_VALUE);

    @VisibleForTesting
    public static CachingParams DEFAULT = new CachingParams(DEFAULT_CACHE_KEYS, DEFAULT_ROWS_PER_PARTITION_TO_CACHE);

    final boolean cacheKeys;
    final int rowsPerPartitionToCache;

    public CachingParams(boolean cacheKeys, int rowsPerPartitionToCache)
    {
        this.cacheKeys = cacheKeys;
        this.rowsPerPartitionToCache = rowsPerPartitionToCache;
    }

    public boolean cacheKeys()
    {
        return cacheKeys;
    }

    public boolean cacheRows()
    {
        return rowsPerPartitionToCache > 0;
    }

    public boolean cacheAllRows()
    {
        return rowsPerPartitionToCache == Integer.MAX_VALUE;
    }

    public int rowsPerPartitionToCache()
    {
        return rowsPerPartitionToCache;
    }

    public static CachingParams fromMap(Map<String, String> map)
    {
        Map<String, String> copy = new HashMap<>(map);

        String keys = copy.remove(Option.KEYS.toString());
        boolean cacheKeys = keys != null && keysFromString(keys);

        String rows = copy.remove(Option.ROWS_PER_PARTITION.toString());
        int rowsPerPartitionToCache = rows == null
                                    ? 0
                                    : rowsPerPartitionFromString(rows);

        if (!copy.isEmpty())
        {
            throw new ConfigurationException(format("Invalid caching sub-options %s: only '%s' and '%s' are allowed",
                                                    copy.keySet(),
                                                    Option.KEYS,
                                                    Option.ROWS_PER_PARTITION));
        }

        return new CachingParams(cacheKeys, rowsPerPartitionToCache);
    }

    public Map<String, String> asMap()
    {
        return ImmutableMap.of(Option.KEYS.toString(),
                               keysAsString(),
                               Option.ROWS_PER_PARTITION.toString(),
                               rowsPerPartitionAsString());
    }

    private static boolean keysFromString(String value)
    {
        if (value.equalsIgnoreCase(ALL))
            return true;

        if (value.equalsIgnoreCase(NONE))
            return false;

        throw new ConfigurationException(format("Invalid value '%s' for caching sub-option '%s': only '%s' and '%s' are allowed",
                                                value,
                                                Option.KEYS,
                                                ALL,
                                                NONE));
    }

    String keysAsString()
    {
        return cacheKeys ? ALL : NONE;
    }

    private static int rowsPerPartitionFromString(String value)
    {
        if (value.equalsIgnoreCase(ALL))
            return Integer.MAX_VALUE;

        if (value.equalsIgnoreCase(NONE))
            return 0;

        if (StringUtils.isNumeric(value))
            return Integer.parseInt(value);

        throw new ConfigurationException(format("Invalid value '%s' for caching sub-option '%s':"
                                                + " only '%s', '%s', and integer values are allowed",
                                                value,
                                                Option.ROWS_PER_PARTITION,
                                                ALL,
                                                NONE));
    }

    String rowsPerPartitionAsString()
    {
        if (rowsPerPartitionToCache == 0)
            return NONE;
        else if (rowsPerPartitionToCache == Integer.MAX_VALUE)
            return ALL;
        else
            return Integer.toString(rowsPerPartitionToCache);
    }

    @Override
    public String toString()
    {
        return format("{'%s' : '%s', '%s' : '%s'}",
                      Option.KEYS,
                      keysAsString(),
                      Option.ROWS_PER_PARTITION,
                      rowsPerPartitionAsString());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CachingParams))
            return false;

        CachingParams c = (CachingParams) o;

        return cacheKeys == c.cacheKeys && rowsPerPartitionToCache == c.rowsPerPartitionToCache;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(cacheKeys, rowsPerPartitionToCache);
    }
}
