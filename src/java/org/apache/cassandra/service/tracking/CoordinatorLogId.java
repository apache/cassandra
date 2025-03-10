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
package org.apache.cassandra.service.tracking;

public class CoordinatorLogId implements Comparable<CoordinatorLogId>
{
    /** TCM host ID */
    public final int hostId;

    /**
     * Host log ID (unique within the host).
     * Allocated anew on host restart - one per token range replicated by the host.
     */
    public final int hostLogId;

    CoordinatorLogId(long id)
    {
        this(hostId(id), hostLogId(id));
    }

    CoordinatorLogId(int hostId, int hostLogId)
    {
        this.hostId = hostId;
        this.hostLogId = hostLogId;
    }

    @Override
    public int compareTo(CoordinatorLogId that)
    {
        return Long.compare(this.asLong(), that.asLong());
    }

    public long asLong()
    {
        return asLong(hostId, hostLogId);
    }

    static long asLong(int hostId, int hostLogId)
    {
        return ((long) hostId << 32) | hostLogId;
    }

    static int hostId(long coordinatorLogId)
    {
        return (int) (coordinatorLogId >>> 32);
    }

    static int hostLogId(long coordinatorLogId)
    {
        return (int) coordinatorLogId;
    }
}
