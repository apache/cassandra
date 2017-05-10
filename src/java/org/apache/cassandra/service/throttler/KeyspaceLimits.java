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

package org.apache.cassandra.service.throttler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * KeyspaceLimits encapsulates all the limits that can be imposed on a keyspace for throttling.
 */
public class KeyspaceLimits
{
    // The following limits are atomic integers because they are read and modified by multiple threads
    // serving reads and writes concurrently.
    public AtomicInteger singleReadLimit;
    public AtomicInteger serialReadLimit;
    public AtomicInteger rangeReadLimit;
    public AtomicInteger singleMutationLimit;
    public AtomicInteger serialMutationLimit;

    public KeyspaceLimits()
    {
        singleReadLimit = new AtomicInteger();
        serialReadLimit = new AtomicInteger();
        rangeReadLimit = new AtomicInteger();
        singleMutationLimit = new AtomicInteger();
        serialMutationLimit = new AtomicInteger();
    }

    public String toString()
    {
        return String.format("[singleReadLimit:%s,serialReadLimit:%s,rangeReadLimit:%s,singleMutationLimit:%s,serialMutationLimit:%s]",
                             singleReadLimit, serialReadLimit, rangeReadLimit, singleMutationLimit, serialMutationLimit);
    }

    /**
     * Sets the current limits to that of the original.
     *
     * @param original The original keyspace limits.
     */
    public void set(final KeyspaceLimits original)
    {
        this.serialReadLimit.set(original.serialReadLimit.get());
        this.singleReadLimit.set(original.singleReadLimit.get());
        this.rangeReadLimit.set(original.rangeReadLimit.get());
        this.singleMutationLimit.set(original.singleMutationLimit.get());
        this.serialMutationLimit.set(original.serialMutationLimit.get());
    }
}
