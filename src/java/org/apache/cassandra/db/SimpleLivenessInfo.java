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

import java.util.Objects;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

public class SimpleLivenessInfo extends AbstractLivenessInfo
{
    private final long timestamp;
    private final int ttl;
    private final int localDeletionTime;

    // Note that while some code use this ctor, the two following static creation methods
    // are usually less error prone.
    SimpleLivenessInfo(long timestamp, int ttl, int localDeletionTime)
    {
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
    }

    public static SimpleLivenessInfo forUpdate(long timestamp, int ttl, int nowInSec, CFMetaData metadata)
    {
        if (ttl == NO_TTL)
            ttl = metadata.getDefaultTimeToLive();

        return new SimpleLivenessInfo(timestamp, ttl, ttl == NO_TTL ? NO_DELETION_TIME : nowInSec + ttl);
    }

    public static SimpleLivenessInfo forDeletion(long timestamp, int localDeletionTime)
    {
        return new SimpleLivenessInfo(timestamp, NO_TTL, localDeletionTime);
    }

    public long timestamp()
    {
        return timestamp;
    }

    public int ttl()
    {
        return ttl;
    }

    public int localDeletionTime()
    {
        return localDeletionTime;
    }

    @Override
    public LivenessInfo takeAlias()
    {
        return this;
    }
}
