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

package org.apache.cassandra.service;

/**
 * When running on Accord each cell's timestamp is updated to be the transaction timestamp, but some use cases need a way
 * to opt out of this behavior and preserve the timestamp; examples are hints and batches that have some mutations using
 * the server's timestamp and others using USING TIMESTAMP.
 */
public enum PreserveTimestamp
{
    yes(true),
    /**
     * When working with a BATCH, different mutations can have different time sources (one mutation might be
     * server based, and another is USING TIMESTAMP based); "mixed" is for this case.
     */
    mixedTimeSource(true),
    no(false),
    ;

    public final boolean preserve;

    PreserveTimestamp(boolean preserve)
    {
        this.preserve = preserve;
    }

    public static PreserveTimestamp merge(PreserveTimestamp left, TimestampSource right)
    {
        if (right == TimestampSource.mixed)
            return mixedTimeSource;
        if (left.preserve)
            return left;
        if (right == TimestampSource.using)
            return yes;
        return left;
    }

    public PreserveTimestamp merge(TimestampSource right)
    {
        return merge(this, right);
    }
}
