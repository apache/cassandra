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
package org.apache.cassandra.db;

public interface DeletionPurger
{
    public static final DeletionPurger PURGE_ALL = (ts, ldt) -> true;

    public boolean shouldPurge(long timestamp, int localDeletionTime);

    public default boolean shouldPurge(DeletionTime dt)
    {
        return !dt.isLive() && shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime());
    }

    public default boolean shouldPurge(LivenessInfo liveness, int nowInSec)
    {
        return !liveness.isLive(nowInSec) && shouldPurge(liveness.timestamp(), liveness.localExpirationTime());
    }
}
