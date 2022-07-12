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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;

public abstract class NodeInfo implements Cloneable
{
    private volatile UUID hostId;
    private volatile String dataCenter;
    private volatile String rack;
    private volatile CassandraVersion releaseVersion;
    private volatile UUID schemaVersion;
    private volatile Collection<Token> tokens;
    private volatile InetAddressAndPort nativeTransportAddressAndPort;
    private volatile boolean dirty;

    @JsonIgnore
    public boolean isDirty()
    {
        return dirty;
    }

    protected void dirty()
    {
        dirty = true;
    }

    public void resetDirty()
    {
        dirty = false;
    }

    @JsonProperty("host_id")
    public UUID getHostId()
    {
        return hostId;
    }

    public void setHostId(UUID hostId)
    {
        this.hostId = hostId;
        dirty();
    }

    @JsonProperty("data_center")
    public String getDataCenter()
    {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter)
    {
        if (Objects.equals(dataCenter, this.dataCenter))
            return;
        this.dataCenter = dataCenter;
        dirty();
    }

    public String getRack()
    {
        return rack;
    }

    public void setRack(String rack)
    {
        if (Objects.equals(rack, this.rack))
            return;
        this.rack = rack;
        dirty();
    }

    @JsonProperty("release_version")
    public CassandraVersion getReleaseVersion()
    {
        return releaseVersion;
    }

    public void setReleaseVersion(CassandraVersion releaseVersion)
    {
        if (Objects.equals(releaseVersion, this.releaseVersion))
            return;
        this.releaseVersion = releaseVersion;
        dirty();
    }

    @JsonProperty("schema_version")
    public UUID getSchemaVersion()
    {
        return schemaVersion;
    }

    public void setSchemaVersion(UUID schemaVersion)
    {
        if (Objects.equals(schemaVersion, this.schemaVersion))
            return;
        this.schemaVersion = schemaVersion;
        dirty();
    }

    public Collection<Token> getTokens()
    {
        return tokens;
    }

    public boolean hasTokens()
    {
        return tokens != null && !tokens.isEmpty();
    }

    public void setTokens(Collection<Token> tokens)
    {
        if (Objects.equals(tokens, this.tokens))
            return;
        if (tokens instanceof SortedSet)
        {
            ArrayList<Token> t = new ArrayList<>(tokens);
            t.sort(Comparator.naturalOrder());
            tokens = t;
        }
        this.tokens = tokens;
        dirty();
    }

    @JsonProperty("native_transport_address_and_port")
    public InetAddressAndPort getNativeTransportAddressAndPort()
    {
        return nativeTransportAddressAndPort;
    }

    public void setNativeTransportAddressAndPort(InetAddressAndPort nativeTransportAddressAndPort)
    {
        if (Objects.equals(nativeTransportAddressAndPort, this.nativeTransportAddressAndPort))
            return;
        this.nativeTransportAddressAndPort = nativeTransportAddressAndPort;
        dirty();
    }

    @Override
    public String toString()
    {
        return "hostId=" + hostId +
               ", dataCenter='" + dataCenter + '\'' +
               ", rack='" + rack + '\'' +
               ", releaseVersion=" + releaseVersion +
               ", schemaVersion=" + schemaVersion +
               ", tokens=" + tokens +
               ", nativeTransportAddressAndPort=" + nativeTransportAddressAndPort +
               ", dirty=" + dirty;
    }

    public abstract NodeInfo copy();
}
