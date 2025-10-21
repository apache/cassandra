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
 * Represents where a timestamp is coming from.
 */
public enum TimestampSource
{
    unknown, server, using,
    /**
     * When doing a BATCH some mutations use "server" and others use "using".
     */
    mixed;
    
    public static TimestampSource merge(TimestampSource left, TimestampSource right)
    {
        if (left == right) return left;
        switch (left)
        {
            case unknown:
                return right;
            case server:
                if (right == TimestampSource.unknown)
                {
                    return server;
                }
                else if (right == TimestampSource.using || right == TimestampSource.mixed)
                {
                    return mixed;
                }
                throw new UnsupportedOperationException(right.name());
            case using:
                if (right == TimestampSource.unknown)
                {
                    return using;
                }
                else if (right == TimestampSource.server || right == TimestampSource.mixed)
                {
                    return mixed;
                }
                throw new UnsupportedOperationException(right.name());
            case mixed:
                return left;
            default:
                throw new UnsupportedOperationException(left.name());
        }
    }

    public static class Collector
    {
        boolean hasServerTimestamp = false;
        boolean hasUserTimestamp = false;

        public void collect(TimestampSource source)
        {
            switch (source)
            {
                case unknown:
                case server:
                    hasServerTimestamp = true;
                    break;
                case using:
                    hasUserTimestamp = true;
                    break;
                case mixed:
                    hasServerTimestamp = true;
                    hasUserTimestamp = true;
                    break;
                default:
                    throw new UnsupportedOperationException(source.name());
            }
        }

        public TimestampSource get()
        {
            if (hasServerTimestamp && hasUserTimestamp) return mixed;
            if (hasServerTimestamp) return server;
            if (hasUserTimestamp) return using;
            throw new IllegalStateException(".get() called before .collect()");
        }
    }
}
