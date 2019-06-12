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
package org.apache.cassandra.net;

import java.util.List;

import com.google.common.collect.ImmutableList;

public enum ConnectionType
{
    LEGACY_MESSAGES (0), // only used for inbound
    URGENT_MESSAGES (1),
    SMALL_MESSAGES  (2),
    LARGE_MESSAGES  (3),
    STREAMING       (4);

    public static final List<ConnectionType> MESSAGING_TYPES = ImmutableList.of(URGENT_MESSAGES, SMALL_MESSAGES, LARGE_MESSAGES);

    public final int id;

    ConnectionType(int id)
    {
        this.id = id;
    }

    public int twoBitID()
    {
        if (id < 0 || id > 0b11)
            throw new AssertionError();
        return id;
    }

    public boolean isStreaming()
    {
        return this == STREAMING;
    }

    public boolean isMessaging()
    {
        return !isStreaming();
    }

    public ConnectionCategory category()
    {
        return this == STREAMING ? ConnectionCategory.STREAMING : ConnectionCategory.MESSAGING;
    }

    private static final ConnectionType[] values = values();

    public static ConnectionType fromId(int id)
    {
        return values[id];
    }
}
