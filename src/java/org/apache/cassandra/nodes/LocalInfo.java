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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.cassandra.db.SystemKeyspace.BootstrapState;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ImmutableUtils;

@NotThreadSafe
public final class LocalInfo extends NodeInfo<LocalInfo>
{
    private volatile InetAddressAndPort broadcastAddressAndPort;
    private volatile BootstrapState bootstrapState;
    private volatile String clusterName;
    private volatile CassandraVersion cqlVersion;
    private volatile InetAddressAndPort listenAddressAndPort;
    private volatile ProtocolVersion nativeProtocolVersion;
    private volatile Class<? extends IPartitioner> partitionerClass;
    private volatile ImmutableMap<UUID, TruncationRecord> truncationRecords = ImmutableMap.of();

    public InetAddressAndPort getBroadcastAddressAndPort()
    {
        return broadcastAddressAndPort;
    }

    public LocalInfo setBroadcastAddressAndPort(InetAddressAndPort broadcastAddressAndPort)
    {
        this.broadcastAddressAndPort = broadcastAddressAndPort;
        return this;
    }

    public BootstrapState getBootstrapState()
    {
        return bootstrapState;
    }

    public LocalInfo setBootstrapState(BootstrapState bootstrapState)
    {
        this.bootstrapState = bootstrapState;
        return this;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public LocalInfo setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    public CassandraVersion getCqlVersion()
    {
        return cqlVersion;
    }

    public LocalInfo setCqlVersion(CassandraVersion cqlVersion)
    {
        this.cqlVersion = cqlVersion;
        return this;
    }

    public InetAddressAndPort getListenAddressAndPort()
    {
        return listenAddressAndPort;
    }

    public LocalInfo setListenAddressAndPort(InetAddressAndPort listenAddressAndPort)
    {
        this.listenAddressAndPort = listenAddressAndPort;
        return this;
    }

    public LocalInfo setListenAddressOnly(InetAddress address, int defaultPort)
    {
        this.listenAddressAndPort = getAddressAndPort(getListenAddressAndPort(), address, defaultPort);
        return this;
    }

    public ProtocolVersion getNativeProtocolVersion()
    {
        return nativeProtocolVersion;
    }

    public LocalInfo setNativeProtocolVersion(ProtocolVersion nativeProtocolVersion)
    {
        this.nativeProtocolVersion = nativeProtocolVersion;
        return this;
    }

    public Class<? extends IPartitioner> getPartitionerClass()
    {
        return partitionerClass;
    }

    public LocalInfo setPartitionerClass(Class<? extends IPartitioner> partitionerClass)
    {
        this.partitionerClass = partitionerClass;
        return this;
    }

    public ImmutableMap<UUID, TruncationRecord> getTruncationRecords()
    {
        return truncationRecords;
    }

    public LocalInfo setTruncationRecords(Map<UUID, TruncationRecord> truncationRecords)
    {
        this.truncationRecords = ImmutableMap.copyOf(truncationRecords);
        return this;
    }

    public LocalInfo removeTruncationRecord(UUID tableId)
    {
        return setTruncationRecords(ImmutableUtils.without(getTruncationRecords(), tableId));
    }

    public LocalInfo addTruncationRecord(UUID tableId, TruncationRecord truncationRecord)
    {
        return setTruncationRecords(ImmutableUtils.withAddedOrUpdated(getTruncationRecords(), tableId, truncationRecord));
    }

    @Override
    public LocalInfo duplicate()
    {
        try
        {
            return (LocalInfo) clone();
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
        if (!(o instanceof LocalInfo)) return false;
        if (!super.equals(o)) return false;
        LocalInfo localInfo = (LocalInfo) o;
        return Objects.equals(getBroadcastAddressAndPort(), localInfo.getBroadcastAddressAndPort())
               && getBootstrapState() == localInfo.getBootstrapState()
               && Objects.equals(getClusterName(), localInfo.getClusterName())
               && Objects.equals(getCqlVersion(), localInfo.getCqlVersion())
               && Objects.equals(getListenAddressAndPort(), localInfo.getListenAddressAndPort())
               && Objects.equals(getNativeProtocolVersion(), localInfo.getNativeProtocolVersion())
               && Objects.equals(getPartitionerClass(), localInfo.getPartitionerClass())
               && Objects.equals(getTruncationRecords(), localInfo.getTruncationRecords());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(),
                            getBroadcastAddressAndPort(),
                            getBootstrapState(),
                            getClusterName(),
                            getCqlVersion(),
                            getListenAddressAndPort(),
                            getNativeProtocolVersion(),
                            getPartitionerClass(),
                            getTruncationRecords());
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
        .appendSuper(super.toString())
        .append("broadcastAddress", getBroadcastAddressAndPort())
        .append("bootstrapState", getBootstrapState())
        .append("clusterName", getClusterName())
        .append("cqlVersion", getCqlVersion())
        .append("listenAddress", getListenAddressAndPort())
        .append("nativeProtocolVersion", getNativeProtocolVersion())
        .append("partitioner", getPartitionerClass())
        .append("truncationRecords", getTruncationRecords())
        .toString();
    }
}
