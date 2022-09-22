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

package org.apache.cassandra.hints;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.base.MoreObjects;

public class PendingHintsInfo
{
    public static final String HOST_ID = "host_id";
    public static final String TOTAL_FILES = "total_files";
    public static final String OLDEST_TIMESTAMP = "oldest_timestamp";
    public static final String NEWEST_TIMESTAMP = "newest_timestamp";

    public final UUID hostId;
    public final int totalFiles;
    public final long oldestTimestamp;
    public final long newestTimestamp;

    public PendingHintsInfo(UUID hostId, int totalFiles, long oldestTimestamp, long newestTimestamp)
    {
        this.hostId = hostId;
        this.totalFiles = totalFiles;
        this.oldestTimestamp = oldestTimestamp;
        this.newestTimestamp = newestTimestamp;
    }

    public Map<String, String> asMap()
    {
        Map<String, String> ret = new HashMap<>();
        ret.put(HOST_ID, hostId.toString());
        ret.put(TOTAL_FILES, String.valueOf(totalFiles));
        ret.put(OLDEST_TIMESTAMP, String.valueOf(oldestTimestamp));
        ret.put(NEWEST_TIMESTAMP, String.valueOf(newestTimestamp));
        return ret;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PendingHintsInfo that = (PendingHintsInfo) o;
        return totalFiles == that.totalFiles &&
               oldestTimestamp == that.oldestTimestamp &&
               newestTimestamp == that.newestTimestamp &&
               Objects.equals(hostId, that.hostId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hostId, totalFiles, oldestTimestamp, newestTimestamp);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("hostId", hostId)
                          .add("totalFiles", totalFiles)
                          .add("oldestTimestamp", oldestTimestamp)
                          .add("newestTimestamp", newestTimestamp)
                          .toString();
    }
}
