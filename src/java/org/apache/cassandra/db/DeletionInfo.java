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

public class DeletionInfo
{
    public final long markedForDeleteAt;
    public final int localDeletionTime;

    public static final DeletionInfo LIVE = new DeletionInfo(Long.MIN_VALUE, Integer.MAX_VALUE);

    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        if (localDeletionTime == Integer.MIN_VALUE)
            localDeletionTime = Integer.MAX_VALUE;

        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
    }

    @Override
    public String toString()
    {
        return String.format("{deletedAt=%d, localDeletion=%d}", markedForDeleteAt, localDeletionTime);
    }
}
