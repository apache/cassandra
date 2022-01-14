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

import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.cassandra.locator.InetAddressAndPort;

public final class PeerInfo extends NodeInfo<PeerInfo> implements IPeerInfo
{
    private volatile InetAddressAndPort peerAddressAndPort;
    private volatile InetAddressAndPort preferredAddressAndPort;
    private volatile boolean removed;

    @Override
    public InetAddressAndPort getPeerAddressAndPort()
    {
        return peerAddressAndPort;
    }

    public PeerInfo setPeerAddressAndPort(InetAddressAndPort peerAddressAndPort)
    {
        this.peerAddressAndPort = peerAddressAndPort;
        return this;
    }

    @Override
    public InetAddressAndPort getPreferredAddressAndPort()
    {
        return preferredAddressAndPort;
    }

    public PeerInfo setPreferredAddressAndPort(InetAddressAndPort preferredAddressAndPort)
    {
        this.preferredAddressAndPort = preferredAddressAndPort;
        return this;
    }

    @Override
    public boolean isRemoved()
    {
        return removed;
    }

    @Override
    public boolean isExisting()
    {
        return !isRemoved();
    }

    PeerInfo setRemoved(boolean removed)
    {
        this.removed = removed;
        return this;
    }

    @Override
    public PeerInfo duplicate()
    {
        try
        {
            return (PeerInfo) clone();
        }
        catch (CloneNotSupportedException e)
        {
            throw new AssertionError(e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof PeerInfo)) return false;
        if (!super.equals(o)) return false;
        PeerInfo peerInfo = (PeerInfo) o;
        return isRemoved() == peerInfo.isRemoved()
               && Objects.equals(getPeerAddressAndPort(), peerInfo.getPeerAddressAndPort())
               && Objects.equals(getPreferredAddressAndPort(), peerInfo.getPreferredAddressAndPort());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(),
                            getPeerAddressAndPort(),
                            getPreferredAddressAndPort(),
                            isRemoved());
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
        .appendSuper(super.toString())
        .append("peer", getPeerAddressAndPort())
        .append("preferredIp", getPreferredAddressAndPort())
        .append("isRemoved", isRemoved())
        .toString();
    }
}
