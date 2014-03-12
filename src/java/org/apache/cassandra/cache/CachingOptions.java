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


import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.exceptions.ConfigurationException;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;

/*
CQL: { 'keys' : 'ALL|NONE', 'rows_per_partition': '200|NONE|ALL' }
 */
public class CachingOptions
{
    public static final CachingOptions KEYS_ONLY = new CachingOptions(new KeyCache(KeyCache.Type.ALL), new RowCache(RowCache.Type.NONE));
    public static final CachingOptions ALL = new CachingOptions(new KeyCache(KeyCache.Type.ALL), new RowCache(RowCache.Type.ALL));
    public static final CachingOptions ROWS_ONLY = new CachingOptions(new KeyCache(KeyCache.Type.NONE), new RowCache(RowCache.Type.ALL));
    public static final CachingOptions NONE = new CachingOptions(new KeyCache(KeyCache.Type.NONE), new RowCache(RowCache.Type.NONE));

    public final KeyCache keyCache;
    public final RowCache rowCache;
    private static final Set<String> legacyOptions = new HashSet<>(Arrays.asList("ALL", "NONE", "KEYS_ONLY", "ROWS_ONLY"));

    public CachingOptions(KeyCache kc, RowCache rc)
    {
        this.keyCache = kc;
        this.rowCache = rc;
    }

    public static CachingOptions fromString(String cache) throws ConfigurationException
    {
        if (legacyOptions.contains(cache.toUpperCase()))
            return fromLegacyOption(cache.toUpperCase());
        return fromMap(fromJsonMap(cache));
    }

    public static CachingOptions fromMap(Map<String, String> cacheConfig) throws ConfigurationException
    {
        validateCacheConfig(cacheConfig);
        if (!cacheConfig.containsKey("keys") && !cacheConfig.containsKey("rows_per_partition"))
            return CachingOptions.NONE;
        if (!cacheConfig.containsKey("keys"))
            return new CachingOptions(new KeyCache(KeyCache.Type.NONE), RowCache.fromString(cacheConfig.get("rows_per_partition")));
        if (!cacheConfig.containsKey("rows_per_partition"))
            return CachingOptions.KEYS_ONLY;

        return new CachingOptions(KeyCache.fromString(cacheConfig.get("keys")), RowCache.fromString(cacheConfig.get("rows_per_partition")));
    }

    private static void validateCacheConfig(Map<String, String> cacheConfig) throws ConfigurationException
    {
        for (Map.Entry<String, String> entry : cacheConfig.entrySet())
        {
            String value = entry.getValue().toUpperCase();
            if (entry.getKey().equals("keys"))
            {
                if (!(value.equals("ALL") || value.equals("NONE")))
                {
                    throw new ConfigurationException("'keys' can only have values 'ALL' or 'NONE'");
                }
            }
            else if (entry.getKey().equals("rows_per_partition"))
            {
                if (!(value.equals("ALL") || value.equals("NONE") || StringUtils.isNumeric(value)))
                {
                    throw new ConfigurationException("'rows_per_partition' can only have values 'ALL', 'NONE' or be numeric.");
                }
            }
            else
                throw new ConfigurationException("Only supported CachingOptions parameters are 'keys' and 'rows_per_partition'");
        }
    }

    @Override
    public String toString()
    {
        return String.format("{\"keys\":\"%s\", \"rows_per_partition\":\"%s\"}", keyCache.toString(), rowCache.toString());
    }

    private static CachingOptions fromLegacyOption(String cache)
    {
        if (cache.equals("ALL"))
            return ALL;
        if (cache.equals("KEYS_ONLY"))
            return KEYS_ONLY;
        if (cache.equals("ROWS_ONLY"))
            return ROWS_ONLY;
        return NONE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CachingOptions o2 = (CachingOptions) o;

        if (!keyCache.equals(o2.keyCache)) return false;
        if (!rowCache.equals(o2.rowCache)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = keyCache.hashCode();
        result = 31 * result + rowCache.hashCode();
        return result;
    }

    public static boolean isLegacy(String CachingOptions)
    {
        return legacyOptions.contains(CachingOptions.toUpperCase());
    }

    public static CachingOptions fromThrift(String caching, String cellsPerRow) throws ConfigurationException
    {

        RowCache rc = new RowCache(RowCache.Type.NONE);
        KeyCache kc = new KeyCache(KeyCache.Type.ALL);
        // if we get a caching string from thrift it is legacy, "ALL", "KEYS_ONLY" etc, fromString handles those
        if (caching != null)
        {
            CachingOptions givenOptions = CachingOptions.fromString(caching);
            rc = givenOptions.rowCache;
            kc = givenOptions.keyCache;
        }
        // if we get cells_per_row from thrift, it is either "ALL" or "<number of cells to cache>".
        if (cellsPerRow != null && rc.isEnabled())
            rc = RowCache.fromString(cellsPerRow);
        return new CachingOptions(kc, rc);
    }

    public String toThriftCaching()
    {
        if (rowCache.isEnabled() && keyCache.isEnabled())
            return "ALL";
        if (rowCache.isEnabled())
            return "ROWS_ONLY";
        if (keyCache.isEnabled())
            return "KEYS_ONLY";
        return "NONE";
    }

    public String toThriftCellsPerRow()
    {
        if (rowCache.cacheFullPartitions())
            return "ALL";
        return String.valueOf(rowCache.rowsToCache);
    }


    public static class KeyCache
    {
        public final Type type;
        public KeyCache(Type type)
        {
            this.type = type;
        }

        public enum Type
        {
            ALL, NONE
        }
        public static KeyCache fromString(String keyCache)
        {
            return new KeyCache(Type.valueOf(keyCache.toUpperCase()));
        }

        public boolean isEnabled()
        {
            return type.equals(Type.ALL);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyCache keyCache = (KeyCache) o;

            if (type != keyCache.type) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            return type.hashCode();
        }
        @Override
        public String toString()
        {
            return type.toString();
        }
    }

    public static class RowCache
    {
        public final Type type;
        public final int rowsToCache;

        public RowCache(Type type)
        {
            this(type, type.equals(Type.ALL) ? Integer.MAX_VALUE : 0);
        }
        public RowCache(Type type, int rowsToCache)
        {
            this.type = type;
            this.rowsToCache = rowsToCache;
        }

        public enum Type
        {
            ALL, NONE, HEAD
        }

        public static RowCache fromString(String rowCache)
        {
            if (rowCache == null || rowCache.equalsIgnoreCase("none"))
                return new RowCache(Type.NONE, 0);
            else if (rowCache.equalsIgnoreCase("all"))
                return new RowCache(Type.ALL, Integer.MAX_VALUE);
            return new RowCache(Type.HEAD, Integer.parseInt(rowCache));
        }
        public boolean isEnabled()
        {
            return type.equals(Type.ALL) || type.equals(Type.HEAD);
        }
        public boolean cacheFullPartitions()
        {
            return type.equals(Type.ALL);
        }
        @Override
        public String toString()
        {
            if (type.equals(Type.ALL)) return "ALL";
            if (type.equals(Type.NONE)) return "NONE";
            return String.valueOf(rowsToCache);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RowCache rowCache = (RowCache) o;

            if (rowsToCache != rowCache.rowsToCache) return false;
            if (type != rowCache.type) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = type.hashCode();
            result = 31 * result + rowsToCache;
            return result;
        }
    }
}
