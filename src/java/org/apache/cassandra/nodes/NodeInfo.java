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

import java.net.InetAddress;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;

@SuppressWarnings("unchecked")
public abstract class NodeInfo<T extends NodeInfo<T>> implements INodeInfo<T>
{
    private volatile UUID hostId;
    private volatile String dataCenter;
    private volatile String rack;
    private volatile CassandraVersion releaseVersion;
    private volatile UUID schemaVersion;

    @Nonnull
    private volatile ImmutableSet<Token> tokens = ImmutableSet.of();

    private volatile InetAddressAndPort nativeTransportAddressAndPort;

    @Override
    public UUID getHostId()
    {
        return hostId;
    }

    public T setHostId(UUID hostId)
    {
        this.hostId = hostId;
        return (T) this;
    }

    @Override
    public String getDataCenter()
    {
        return dataCenter;
    }

    public T setDataCenter(String dataCenter)
    {
        this.dataCenter = dataCenter;
        return (T) this;
    }

    @Override
    public String getRack()
    {
        return rack;
    }

    public T setRack(String rack)
    {
        this.rack = rack;
        return (T) this;
    }

    @Override
    public CassandraVersion getReleaseVersion()
    {
        return releaseVersion;
    }

    public T setReleaseVersion(CassandraVersion releaseVersion)
    {
        this.releaseVersion = releaseVersion;
        return (T) this;
    }

    @Override
    public UUID getSchemaVersion()
    {
        return schemaVersion;
    }

    public T setSchemaVersion(UUID schemaVersion)
    {
        this.schemaVersion = schemaVersion;
        return (T) this;
    }

    @Override
    public @Nonnull
    Collection<Token> getTokens()
    {
        return tokens;
    }

    public T setTokens(@Nonnull Iterable<Token> tokens)
    {
        Preconditions.checkNotNull(tokens);
        this.tokens = ImmutableSet.copyOf(tokens);
        return (T) this;
    }

    @Override
    public InetAddressAndPort getNativeTransportAddressAndPort()
    {
        return nativeTransportAddressAndPort;
    }

    public T setNativeTransportAddressAndPort(InetAddressAndPort nativeTransportAddressAndPort)
    {
        this.nativeTransportAddressAndPort = nativeTransportAddressAndPort;
        return (T) this;
    }

    public T setNativeTransportAddressOnly(InetAddress address, int defaultPort)
    {
        this.nativeTransportAddressAndPort = getAddressAndPort(getNativeTransportAddressAndPort(), address, defaultPort);
        return (T) this;
    }

    InetAddressAndPort getAddressAndPort(InetAddressAndPort current, InetAddress newAddress, int defaultPort)
    {
        if (newAddress == null)
        {
            return null;
        }
        else
        {
            int port = current != null && current.port > 0 ? current.port : defaultPort;
            return InetAddressAndPort.getByAddressOverrideDefaults(newAddress, port);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof NodeInfo<?>)) return false;
        NodeInfo<?> nodeInfo = (NodeInfo<?>) o;
        return Objects.equals(getHostId(), nodeInfo.getHostId())
               && Objects.equals(getDataCenter(), nodeInfo.getDataCenter())
               && Objects.equals(getRack(), nodeInfo.getRack())
               && Objects.equals(getReleaseVersion(), nodeInfo.getReleaseVersion())
               && Objects.equals(getSchemaVersion(), nodeInfo.getSchemaVersion())
               && Objects.equals(getTokens(), nodeInfo.getTokens())
               && Objects.equals(getNativeTransportAddressAndPort(), nodeInfo.getNativeTransportAddressAndPort());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getHostId(),
                            getDataCenter(),
                            getRack(),
                            getReleaseVersion(),
                            getSchemaVersion(),
                            getTokens(),
                            getNativeTransportAddressAndPort());
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
        .append("hostId", getHostId())
        .append("dataCenter", getDataCenter())
        .append("rack", getRack())
        .append("releaseVersion", getReleaseVersion())
        .append("schemaVersion", getSchemaVersion())
        .append("tokens", getTokens())
        .append("nativeTransportAddress", getNativeTransportAddressAndPort())
        .toString();
    }
}
