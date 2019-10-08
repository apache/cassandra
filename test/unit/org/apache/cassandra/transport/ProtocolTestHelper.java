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

package org.apache.cassandra.transport;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.utils.FBUtilities;

public class ProtocolTestHelper
{
    static ExecutorService executor = MoreExecutors.newDirectExecutorService();
    static InetAddress setupPeer(String address, String version) throws Throwable
    {
        InetAddress peer = peer(address);
        updatePeerInfo(peer, version);
        return peer;
    }

    static void updatePeerInfo(InetAddress peer, String version) throws Throwable
    {
        SystemKeyspace.updatePeerInfo(peer, "release_version", version, executor);
    }

    static InetAddress peer(String address)
    {
        try
        {
            return InetAddress.getByName(address);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Error creating peer", e);
        }
    }

    static void cleanupPeers(InetAddress...peers) throws Throwable
    {
        for (InetAddress peer : peers)
            if (peer != null)
                SystemKeyspace.removeEndpoint(peer);
    }

    static void setStaticLimitInConfig(Integer version)
    {
        try
        {
            Field field = FBUtilities.getProtectedField(DatabaseDescriptor.class, "conf");
            ((Config)field.get(null)).native_transport_max_negotiable_protocol_version = version == null ? Integer.MIN_VALUE : version;
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException("Error setting native_transport_max_protocol_version on Config", e);
        }
    }

    static VersionedValue releaseVersion(String versionString)
    {
        try
        {
            Constructor<VersionedValue> ctor = VersionedValue.class.getDeclaredConstructor(String.class);
            ctor.setAccessible(true);
            return ctor.newInstance(versionString);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error constructing VersionedValue for release version", e);
        }
    }
}
