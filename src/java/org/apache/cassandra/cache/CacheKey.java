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

import java.util.UUID;

public interface CacheKey extends IMeasurableMemory
{
    /**
     * @return The keyspace and ColumnFamily names to which this key belongs
     */
    public PathInfo getPathInfo();

    public static class PathInfo
    {
        public final String keyspace;
        public final String columnFamily;
        public final UUID cfId;

        public PathInfo(String keyspace, String columnFamily, UUID cfId)
        {
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
            this.cfId = cfId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PathInfo pathInfo = (PathInfo) o;

            return (cfId != null ? cfId.equals(pathInfo.cfId) : pathInfo.cfId == null) && columnFamily.equals(pathInfo.columnFamily) && keyspace.equals(pathInfo.keyspace);
        }

        @Override
        public int hashCode()
        {
            int result = keyspace.hashCode();
            result = 31 * result + columnFamily.hashCode();
            result = 31 * result + (cfId != null ? cfId.hashCode() : 0);
            return result;
        }
    }
}
