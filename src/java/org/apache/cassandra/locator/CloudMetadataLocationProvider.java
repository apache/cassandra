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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.membership.Location;

import static java.lang.String.format;

public class CloudMetadataLocationProvider implements InitialLocationProvider
{
    static final Logger logger = LoggerFactory.getLogger(CloudMetadataLocationProvider.class);

    protected final AbstractCloudMetadataServiceConnector connector;
    private final LocationResolver locationResolver;
    private volatile Location location;

    public CloudMetadataLocationProvider(AbstractCloudMetadataServiceConnector connector, LocationResolver locationResolver)
    {
        this.connector = connector;
        this.locationResolver = locationResolver;
    }

    @Override
    public final Location initialLocation()
    {
        if (location == null)
        {
            try
            {
                location = locationResolver.resolve(connector);
                logger.info(format("%s using datacenter: %s, rack: %s, connector: %s, properties: %s",
                                   getClass().getName(), location.datacenter, location.rack, connector, connector.getProperties()));
            }
            catch (IOException e)
            {
                throw new ConfigurationException("Unable to resolve initial location using cloud metadata service connector", e);
            }
        }
        return location;
    }

    public interface LocationResolver
    {
        Location resolve(AbstractCloudMetadataServiceConnector connector) throws IOException;
    }
}
