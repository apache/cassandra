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

package org.apache.cassandra.net.async;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CoalescingStrategies;

/**
 *  A wrapper for outbound messages. All messages will be retried once.
 */
public class QueuedMessage implements CoalescingStrategies.Coalescable
{
    public final MessageOut<?> message;
    public final int id;
    public final long timestampNanos;
    public final boolean droppable;
    private final boolean retryable;

    public QueuedMessage(MessageOut<?> message, int id)
    {
        this(message, id, System.nanoTime(), MessagingService.DROPPABLE_VERBS.contains(message.verb), true);
    }

    @VisibleForTesting
    public QueuedMessage(MessageOut<?> message, int id, long timestampNanos, boolean droppable, boolean retryable)
    {
        this.message = message;
        this.id = id;
        this.timestampNanos = timestampNanos;
        this.droppable = droppable;
        this.retryable = retryable;
    }

    /** don't drop a non-droppable message just because it's timestamp is expired */
    public boolean isTimedOut()
    {
        return droppable && timestampNanos < System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(message.getTimeout());
    }

    public boolean shouldRetry()
    {
        return retryable;
    }

    public QueuedMessage createRetry()
    {
        return new QueuedMessage(message, id, System.nanoTime(), droppable, false);
    }

    public long timestampNanos()
    {
        return timestampNanos;
    }
}
