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

import io.netty.util.concurrent.Future;

/**
 * A simple, reusable struct that holds the unprocessed result of sending a message via netty. This object is intended
 * to be reusable to avoid creating a bunch of garbage (just for processing the results of sending a message).
 *
 * The intended use is to be a member field in a class, like {@link ChannelWriter}, repopulated on each message result,
 * and then immediately cleared (via {@link #clearAll()}) when done.
 */
public class MessageResult
{
    ChannelWriter writer;
    QueuedMessage msg;
    Future<? super Void> future;
    boolean allowReconnect;

    void setAll(ChannelWriter writer, QueuedMessage msg, Future<? super Void> future, boolean allowReconnect)
    {
        this.writer = writer;
        this.msg = msg;
        this.future = future;
        this.allowReconnect = allowReconnect;
    }

    void clearAll()
    {
        this.writer = null;
        this.msg = null;
        this.future = null;
    }
}
