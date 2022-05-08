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
package org.apache.cassandra.service.reads;

import com.google.common.base.Objects;

import org.apache.cassandra.metrics.SnapshottingTimer;

public class NeverSpeculativeRetryPolicy implements SpeculativeRetryPolicy
{
    public static final NeverSpeculativeRetryPolicy INSTANCE = new NeverSpeculativeRetryPolicy();

    private NeverSpeculativeRetryPolicy()
    {
    }

    @Override
    public long calculateThreshold(SnapshottingTimer latency, long existingValue)
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Kind kind()
    {
        return Kind.NEVER;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof NeverSpeculativeRetryPolicy;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(kind());
    }

    @Override
    public String toString()
    {
        return Kind.NEVER.toString();
    }

    static boolean stringMatches(String str)
    {
        return str.equalsIgnoreCase("NEVER") || str.equalsIgnoreCase("NONE");
    }
}
