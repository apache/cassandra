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

import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;

import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;
import static org.apache.cassandra.locator.AzureCloudLocationProvider.DEFAULT_METADATA_SERVICE_URL;

/**
 * AzureSnitch will resolve datacenter and rack by calling {@code /metadata/instance/compute} endpoint returning
 * the response in JSON format for API version {@code 2021-12-13}. The version of API is configurable via property
 * {@code azure_api_version} in cassandra-rackdc.properties.
 * <p>
 * A datacenter is resolved from {@code location} field and a rack is resolved by looking into {@code zone} field first.
 * When zone is not set, or it is empty string, it will look into {@code platformFaultDomain} field. Such resolved
 * value is prepended by {@code rack-} string.
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
public class AzureSnitch extends AbstractCloudMetadataServiceSnitch
{
    public AzureSnitch() throws IOException
    {
        this(new SnitchProperties());
    }

    public AzureSnitch(SnitchProperties properties) throws IOException
    {
        this(new DefaultCloudMetadataServiceConnector(properties.putIfAbsent(METADATA_URL_PROPERTY,
                                                                             DEFAULT_METADATA_SERVICE_URL)));
    }

    public AzureSnitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(new AzureCloudLocationProvider(connector));
    }
}
