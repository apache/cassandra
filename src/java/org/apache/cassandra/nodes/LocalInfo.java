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

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Throwables;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class LocalInfo extends NodeInfo
{
    private final String key = "local";
    private volatile InetAddressAndPort broadcastAddressAndPort;
    private volatile BootstrapState bootstrapState = BootstrapState.NEEDS_BOOTSTRAP;
    private volatile String clusterName;
    private volatile CassandraVersion cqlVersion;
    private volatile Integer gossipGeneration = (int) System.currentTimeMillis() / 1000;
    private volatile InetAddressAndPort listenAddressAndPort;
    private volatile String nativeProtocolVersion;
    private volatile String partitioner;
    private volatile Map<UUID, TruncationRecord> truncationRecords = ImmutableMap.of();

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public String getKey()
    {
        return key;
    }

    @JsonProperty("broadcast_address_and_port")
    public InetAddressAndPort getBroadcastAddressAndPort()
    {
        return broadcastAddressAndPort;
    }

    public void setBroadcastAddressAndPort(InetAddressAndPort broadcastAddressAndPort)
    {
        if (Objects.equals(broadcastAddressAndPort, this.broadcastAddressAndPort))
            return;
        this.broadcastAddressAndPort = broadcastAddressAndPort;
        dirty();
    }

    @JsonProperty("bootstrapped")
    public BootstrapState getBootstrapState()
    {
        return bootstrapState;
    }

    public void setBootstrapState(BootstrapState bootstrapState)
    {
        if (Objects.equals(bootstrapState, this.bootstrapState))
            return;
        this.bootstrapState = bootstrapState;
        dirty();
    }

    @JsonProperty("cluster_name")
    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName(String clusterName)
    {
        if (Objects.equals(clusterName, this.clusterName))
            return;
        this.clusterName = clusterName;
        dirty();
    }

    @JsonProperty("cql_version")
    public CassandraVersion getCqlVersion()
    {
        return cqlVersion;
    }

    public void setCqlVersion(CassandraVersion cqlVersion)
    {
        if (Objects.equals(cqlVersion, this.cqlVersion))
            return;
        this.cqlVersion = cqlVersion;
        dirty();
    }

    @JsonProperty("gossip_generation")
    public Integer getGossipGeneration()
    {
        return gossipGeneration;
    }

    public void setGossipGeneration(Integer gossipGeneration)
    {
        if (Objects.equals(gossipGeneration, this.gossipGeneration))
            return;
        this.gossipGeneration = gossipGeneration;
        dirty();
    }

    @JsonProperty("listen_address_and_port")
    public InetAddressAndPort getListenAddressAndPort()
    {
        return listenAddressAndPort;
    }

    public void setListenAddressAndPort(InetAddressAndPort listenAddressAndPort)
    {
        if (Objects.equals(listenAddressAndPort, this.listenAddressAndPort))
            return;
        this.listenAddressAndPort = listenAddressAndPort;
        dirty();
    }

    @JsonProperty("native_protocol_version")
    public String getNativeProtocolVersion()
    {
        return nativeProtocolVersion;
    }

    public void setNativeProtocolVersion(String nativeProtocolVersion)
    {
        if (Objects.equals(nativeProtocolVersion, this.nativeProtocolVersion))
            return;
        this.nativeProtocolVersion = nativeProtocolVersion;
        dirty();
    }

    public String getPartitioner()
    {
        return partitioner;
    }

    public void setPartitioner(String partitioner)
    {
        if (Objects.equals(partitioner, this.partitioner))
            return;
        this.partitioner = partitioner;
        dirty();
    }

    @JsonProperty("truncated_at")
    public Map<UUID, TruncationRecord> getTruncationRecords()
    {
        return truncationRecords;
    }

    public void setTruncationRecords(Map<UUID, TruncationRecord> truncationRecords)
    {
        if (Objects.equals(truncationRecords, this.truncationRecords))
            return;
        truncationRecords.forEach((k, v) -> {
            if (k == null || v == null)
                throw new IllegalArgumentException();
        });
        this.truncationRecords = truncationRecords;
        dirty();
    }


    @Override
    public String toString()
    {
        return "LocalInfo{" +
               "broadcastAddressAndPort=" + broadcastAddressAndPort +
               ", bootstrapState=" + bootstrapState +
               ", clusterName='" + clusterName + '\'' +
               ", cqlVersion=" + cqlVersion +
               ", gossipGeneration=" + gossipGeneration +
               ", listenAddressAndPort=" + listenAddressAndPort +
               ", nativeProtocolVersion=" + nativeProtocolVersion +
               ", partitioner='" + partitioner + '\'' +
               ", truncationRecords=" + truncationRecords +
               ", " + super.toString() +
               '}';
    }

    @Override
    public LocalInfo copy()
    {
        try
        {
            return (LocalInfo) clone();
        }
        catch (CloneNotSupportedException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    public static final class TruncationRecord
    {
        public final CommitLogPosition position;
        public final long truncatedAt;

        public TruncationRecord(CommitLogPosition position, long truncatedAt)
        {
            this.position = position;
            this.truncatedAt = truncatedAt;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TruncationRecord that = (TruncationRecord) o;
            return truncatedAt == that.truncatedAt &&
                   Objects.equals(position, that.position);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(position, truncatedAt);
        }
    }
}
