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
package org.apache.cassandra.utils.progress;

/**
 * Progress event
 */
public class ProgressEvent
{
    private final ProgressEventType type;
    private final int progressCount;
    private final int total;
    private final String message;

    public static ProgressEvent createNotification(String message)
    {
        return new ProgressEvent(ProgressEventType.NOTIFICATION, 0, 0, message);
    }

    public ProgressEvent(ProgressEventType type, int progressCount, int total)
    {
        this(type, progressCount, total, null);
    }

    public ProgressEvent(ProgressEventType type, int progressCount, int total, String message)
    {
        this.type = type;
        this.progressCount = progressCount;
        this.total = total;
        this.message = message;
    }

    public ProgressEventType getType()
    {
        return type;
    }

    public int getProgressCount()
    {
        return progressCount;
    }

    public int getTotal()
    {
        return total;
    }

    public double getProgressPercentage()
    {
        return total != 0 ? progressCount * 100 / (double) total : 0;
    }

    /**
     * @return Message attached to this event. Can be null.
     */
    public String getMessage()
    {
        return message;
    }
}
