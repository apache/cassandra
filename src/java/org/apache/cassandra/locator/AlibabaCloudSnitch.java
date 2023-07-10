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

/**
 * A snitch that assumes an ECS region is a DC and an ECS availability_zone
 * is a rack. This information is available in the config for the node. the
 * format of the zone-id is like 'cn-hangzhou-a' where cn means china, hangzhou
 * means the hangzhou region, a means the az id. We use 'cn-hangzhou' as the dc,
 * and 'a' as the zone-id.
 */
public class AlibabaCloudSnitch extends AbstractCloudMetadataServiceSnitch
{
    static final String DEFAULT_METADATA_SERVICE_URL = "http://100.100.100.200";
    static final String ZONE_NAME_QUERY_URL = "/latest/meta-data/zone-id";

    public AlibabaCloudSnitch() throws IOException
    {
        this(new SnitchProperties());
    }

    public AlibabaCloudSnitch(SnitchProperties properties) throws IOException
    {
        this(new DefaultCloudMetadataServiceConnector(properties.putIfAbsent(METADATA_URL_PROPERTY,
                                                                             DEFAULT_METADATA_SERVICE_URL)));
    }

    public AlibabaCloudSnitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector, SnitchUtils.parseDcAndRack(connector.apiCall(ZONE_NAME_QUERY_URL),
                                                    connector.getProperties().getDcSuffix()));
    }
}
