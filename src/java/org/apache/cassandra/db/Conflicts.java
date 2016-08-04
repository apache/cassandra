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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.context.CounterContext;

public abstract class Conflicts
{
    private Conflicts() {}

    public enum Resolution { LEFT_WINS, MERGE, RIGHT_WINS };

    public static Resolution resolveRegular(long leftTimestamp,
                                            boolean leftLive,
                                            int leftLocalDeletionTime,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            int rightLocalDeletionTime,
                                            ByteBuffer rightValue)
    {
        if (leftTimestamp != rightTimestamp)
            return leftTimestamp < rightTimestamp ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;

        if (leftLive != rightLive)
            return leftLive ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;

        int c = leftValue.compareTo(rightValue);
        if (c < 0)
            return Resolution.RIGHT_WINS;
        else if (c > 0)
            return Resolution.LEFT_WINS;

        // Prefer the longest ttl if relevant
        return leftLocalDeletionTime < rightLocalDeletionTime ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;
    }

    public static Resolution resolveCounter(long leftTimestamp,
                                            boolean leftLive,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            ByteBuffer rightValue)
    {
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (!leftLive)
            // left is a tombstone: it has precedence over right if either right is not a tombstone, or left has a greater timestamp
            return rightLive || leftTimestamp > rightTimestamp ? Resolution.LEFT_WINS : Resolution.RIGHT_WINS;

        // If right is a tombstone, since left isn't one, it has precedence
        if (!rightLive)
            return Resolution.RIGHT_WINS;

        // Handle empty values. Counters can't truly have empty values, but we can have a counter cell that temporarily
        // has one on read if the column for the cell is not queried by the user due to the optimization of #10657. We
        // thus need to handle this (see #11726 too).
        if (!leftValue.hasRemaining())
            return rightValue.hasRemaining() || leftTimestamp > rightTimestamp ? Resolution.LEFT_WINS : Resolution.RIGHT_WINS;

        if (!rightValue.hasRemaining())
            return Resolution.RIGHT_WINS;

        return Resolution.MERGE;
    }

    public static ByteBuffer mergeCounterValues(ByteBuffer left, ByteBuffer right)
    {
        return CounterContext.instance().merge(left, right);
    }

}
