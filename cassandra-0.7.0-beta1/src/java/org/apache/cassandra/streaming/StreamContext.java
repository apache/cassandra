package org.apache.cassandra.streaming;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.net.InetAddress;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class StreamContext
{
    public final InetAddress host;
    public final long sessionId;

    public StreamContext(InetAddress host, long sessionId)
    {
        this.host = host;
        this.sessionId = sessionId;
    }

    public StreamContext(InetAddress host)
    {
        this.host = host;
        this.sessionId = generateSessionId();
    }

    private static long generateSessionId()
    {
        return System.nanoTime();
    }

    public String toString()
    {
        return new String(host + " - " + sessionId);
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }
        StreamContext context = (StreamContext) obj;
        return new EqualsBuilder().
                    append(host, context.host).
                    append(sessionId, context.sessionId).
                    isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(119, 5971).
                    append(host).
                    append(sessionId).
                    toHashCode();
    }
}
