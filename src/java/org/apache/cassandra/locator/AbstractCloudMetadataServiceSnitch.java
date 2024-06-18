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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
abstract class AbstractCloudMetadataServiceSnitch extends AbstractNetworkTopologySnitch
{
    static final Logger logger = LoggerFactory.getLogger(AbstractCloudMetadataServiceSnitch.class);

    protected final CloudMetadataLocationProvider locationProvider;

    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints;

    public AbstractCloudMetadataServiceSnitch(CloudMetadataLocationProvider locationProvider)
    {
        this.locationProvider = locationProvider;
    }

    @Override
    public String getLocalRack()
    {
        return locationProvider.initialLocation().rack;
    }

    @Override
    public String getLocalDatacenter()
    {
        return locationProvider.initialLocation().datacenter;
    }
}
