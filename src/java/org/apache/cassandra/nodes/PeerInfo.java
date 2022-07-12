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
package org.apache.cassandra.nodes;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Throwables;

public final class PeerInfo extends NodeInfo
{
    private final InetAddressAndPort peer;

    private volatile InetAddressAndPort preferred;

    @JsonCreator
    public PeerInfo(@JsonProperty("peer") InetAddressAndPort peer)
    {
        this.peer = peer;
    }

    public InetAddressAndPort getPeer()
    {
        return peer;
    }

    @JsonProperty("preferred_ip")
    public InetAddressAndPort getPreferred()
    {
        return preferred;
    }

    public void setPreferred(InetAddressAndPort preferred)
    {
        if (Objects.equals(preferred, this.preferred))
            return;
        this.preferred = preferred;
        dirty();
    }

    @Override
    public String toString()
    {
        return "PeerInfo{" +
               "peer=" + peer +
               ", preferred=" + preferred +
               ", " + super.toString() +
               '}';
    }

    @Override
    public PeerInfo copy()
    {
        try
        {
            return (PeerInfo) clone();
        }
        catch (CloneNotSupportedException e)
        {
            throw Throwables.unchecked(e);
        }
    }
}
