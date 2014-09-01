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

public class ReusableLivenessInfo extends AbstractLivenessInfo
{
    private long timestamp;
    private int ttl;
    private int localDeletionTime;

    public ReusableLivenessInfo()
    {
        reset();
    }

    public LivenessInfo setTo(LivenessInfo info)
    {
        return setTo(info.timestamp(), info.ttl(), info.localDeletionTime());
    }

    public LivenessInfo setTo(long timestamp, int ttl, int localDeletionTime)
    {
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
        return this;
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

    public void reset()
    {
        this.timestamp = LivenessInfo.NO_TIMESTAMP;
        this.ttl = LivenessInfo.NO_TTL;
        this.localDeletionTime = LivenessInfo.NO_DELETION_TIME;
    }
}
