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
package org.apache.cassandra.hints;

import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_HINTS_ENDPOINT_PROVIDER;

/**
 * Provide endpoint info and host info for hints. It's used by CNDB to support cross-region hints
 */
public interface HintsEndpointProvider
{
    static final Logger LOGGER = LoggerFactory.getLogger(HintsDispatcher.class);

    HintsEndpointProvider instance = CUSTOM_HINTS_ENDPOINT_PROVIDER.isPresent()
                                     ? FBUtilities.construct(CUSTOM_HINTS_ENDPOINT_PROVIDER.getString(),
                                                             "Hinted Handoff Endpoint Provider")
                                     : new DefaultHintsEndpointProvider();

    boolean isSameSchemaVersion(UUID hostId);

    boolean isAlive(UUID hostId);

    InetAddressAndPort endpointForHost(UUID hostId);

    UUID hostForEndpoint(InetAddressAndPort endpoint);

    Optional<Integer> versionForEndpoint(InetAddressAndPort endpoint);

    class DefaultHintsEndpointProvider implements HintsEndpointProvider
    {
        @Override
        public InetAddressAndPort endpointForHost(UUID hostId)
        {
            return StorageService.instance.getEndpointForHostId(hostId);
        }

        @Override
        public UUID hostForEndpoint(InetAddressAndPort endpoint)
        {
            return StorageService.instance.getHostIdForEndpoint(endpoint);
        }

        @Override
        public Optional<Integer> versionForEndpoint(InetAddressAndPort endpoint)
        {
            EndpointMessagingVersions versions = MessagingService.instance().versions;
            if (versions.knows(endpoint))
            {
                try
                {
                    return Optional.of(versions.getRaw(endpoint));
                }
                catch (Exception e)
                {
                    LOGGER.debug("Failed to get raw version for endpoint {}", endpoint, e);
                }
            }
            return Optional.empty();
        }

        @Override
        public boolean isSameSchemaVersion(UUID hostId)
        {
            InetAddressAndPort peer = this.endpointForHost(hostId);
            return Schema.instance.isSameVersion(Gossiper.instance.getSchemaVersion(peer));
        }

        @Override
        public boolean isAlive(UUID hostId)
        {
            InetAddressAndPort address = this.endpointForHost(hostId);
            return address != null && IFailureDetector.instance.isAlive(address);
        }
    }
}
