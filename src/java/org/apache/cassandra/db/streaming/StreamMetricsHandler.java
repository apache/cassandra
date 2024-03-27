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

package org.apache.cassandra.db.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.checkerframework.checker.nullness.qual.Nullable;

public class StreamMetricsHandler implements StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamMetricsHandler.class);
    @Override
    public void handleStreamEvent(StreamEvent event)
    {
        switch (event.eventType)
        {
            case STREAM_PREPARED:
                // No need to update metrics when preparing.
                break;
            case FILE_PROGRESS:
                handleProgressEvent((StreamEvent.ProgressEvent) event);
                break;
            case STREAM_COMPLETE:
                break;
        }
    }

    private void handleProgressEvent(StreamEvent.ProgressEvent progressEvent)
    {
        long delta = progressEvent.progress.deltaBytes;
        logger.debug("Adding {} bytes to stream counters in direction {}", delta, progressEvent.progress.direction);
        if (delta == 0)
            return;

        if (progressEvent.progress.direction == ProgressInfo.Direction.IN)
        {
            StreamingMetrics.get(progressEvent.progress.peer).incomingBytes.inc(delta);
            StreamingMetrics.totalIncomingBytes.inc(delta);
        }
        else
        {
            StreamingMetrics.get(progressEvent.progress.peer).outgoingBytes.inc(delta);
            StreamingMetrics.totalOutgoingBytes.inc(delta);
        }
    }

    @Override
    public void onSuccess(@Nullable StreamState streamState)
    {
    }

    @Override
    public void onFailure(Throwable throwable)
    {
    }
}
