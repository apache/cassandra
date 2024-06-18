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

package org.apache.cassandra.locator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.RegistrationStatus;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Provides Location (datacenter & rack) information for endpoints. Usually this is obtained directly
 * from ClusterMetadata.directory, using either a specific directory instance or the most current
 * published one. During initial startup, location info for the local node may be derived from some
 * other source, such as a config file or cloud metadata api. This is then used to register the node
 * and its location in ClusterMetadata, which then becomes the ultimate source of truth.
 */
public class Locator
{
    private final InetAddressAndPort localEndpoint;

    // Indicates whether the *local node* is yet to register itself with ClusterMetadata.
    // This is relevant because once a node is registered, it's location is always derived
    // from ClusterMetadata. However, before registering the location is obtained from
    // configuration. This pre-registration location is what the node supplies to ClusterMetadata
    // when registering and is also used to determine DC locality of peers, for instance when
    // establishing initial internode connections during the discovery phase.
    private final RegistrationStatus state;

    // Source of truth for location lookups. This may be null, in which case every lookup will
    // use the most up to date version from ClusterMetadata.current().directory
    private final Directory directory;

    // Supplies the Location for this node only during its initial startup. This location will be
    // used to register the node with ClusterMetadata and is not used after that has occurred.
    // See DatabaseDescriptor::getInitialLocationProvider
    private final InitialLocationProvider locationProvider;

    // This is the Location used to register this node during its initial startup. It is lazily initialized
    // using the supplied InitialLocationProvider and memoized here as that may be a non-trivial operation.
    // Some providers fetch location metadata from remote services etc. It should usually be unnecessary to
    // access the initialization location after a node's first startup.
    private volatile Location initializationLocation;

    // Set from ClusterMetadata. After initial registration has happened, location of this node itself
    // is always taken from ClusterMetadata.
    private volatile VersionedLocation local;

    private static class VersionedLocation
    {
        final Epoch epoch;
        final Location location;
        private VersionedLocation(Epoch epoch, Location location)
        {
            this.epoch = epoch;
            this.location = location;
        }
    }

    public static Locator usingDirectory(Directory directory)
    {
        return new Locator(RegistrationStatus.instance,
                           FBUtilities.getBroadcastAddressAndPort(),
                           DatabaseDescriptor.getInitialLocationProvider(),
                           directory);
    }

    public static Locator forClients()
    {
        return new Locator(RegistrationStatus.instance,
                           FBUtilities.getBroadcastAddressAndPort(),
                           () -> Location.UNKNOWN,
                           null);
    }

    /**
     * Creates a Locator instance which always uses the Directory from the most current ClusterMetadata.
     * This means that the values returned from {@link this#location(InetAddressAndPort)} can change between
     * invocations, if interleaved with the publication of updated cluster metadata.
     */
    public Locator(RegistrationStatus state,
                   InetAddressAndPort localEndpoint,
                   InitialLocationProvider provider)
    {
        this(state, localEndpoint, provider, null);
    }

    /**
     * Creates a Locator instance which returns consistent results based on the supplied Directory instance.
     * Changes to RegistrationStatus could still have an effect i.e. the node transitioned from UNREGISTERED to
     * REGISTERED between two calls to local(), the first would return the Location according to
     * DatabaseDescriptor.getInitialLocationProvider, but the second would consult the supplied Directory.
     */
    public Locator(RegistrationStatus state,
                   InetAddressAndPort localEndpoint,
                   InitialLocationProvider provider,
                   Directory directory)
    {
        this.state = state;
        this.localEndpoint = localEndpoint;
        this.locationProvider = provider;
        this.directory = directory;
        this.local = new VersionedLocation(Epoch.EMPTY, Location.UNKNOWN);
    }

    public Location location(InetAddressAndPort endpoint)
    {
        switch (state.getCurrent())
        {
            case INITIAL:
                return endpoint.equals(localEndpoint) ? initialLocation() : Location.UNKNOWN;
            case UNREGISTERED:
                return endpoint.equals(localEndpoint) ? initialLocation() : fromDirectory(endpoint);
            default:
                return fromDirectory(endpoint);
        }
    }

    public Location local()
    {
        switch (state.getCurrent())
        {
            case INITIAL:
            case UNREGISTERED:
                return initialLocation();
            default:
                // For now, local location is immutable and once registered with cluster metadata, it cannot be
                // changed. Revisit this if that assumption changes.
                VersionedLocation location = local;
                if (location.epoch.isAfter(Epoch.EMPTY))
                    return location.location;

                local = versionedFromDirectory(localEndpoint);
                return local.location;
        }
    }

    // The distinction between versioned and unversioned may be removed if/when we allow
    // a node's Location to be modified after registration. This duplication should not
    // be necessary then.
    private VersionedLocation versionedFromDirectory(InetAddressAndPort endpoint)
    {
        Directory source = directory;
        if (source == null)
        {
            ClusterMetadata metadata = ClusterMetadata.currentNullable();
            if (metadata == null)
                return new VersionedLocation(Epoch.EMPTY, Location.UNKNOWN);
            source = metadata.directory;
        }
        NodeId nodeId = source.peerId(endpoint);
        Location location =  nodeId != null ? source.location(nodeId) : Location.UNKNOWN;
        return new VersionedLocation(source.lastModified(), location);
    }

    private Location fromDirectory(InetAddressAndPort endpoint)
    {
        Directory source = directory;
        if (source == null)
        {
            ClusterMetadata metadata = ClusterMetadata.currentNullable();
            if (metadata == null)
                return Location.UNKNOWN;
            source = metadata.directory;
        }
        NodeId nodeId = source.peerId(endpoint);
        return nodeId != null ? source.location(nodeId) : Location.UNKNOWN;
    }

    private Location initialLocation()
    {
        if (initializationLocation == null)
            initializationLocation = locationProvider.initialLocation();
        return initializationLocation;
    }
}
