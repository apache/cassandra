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
import java.util.Properties;

/**
 * A snitch that assumes a Cloudstack Zone follows the typical convention
 * {@code country-location-availability zone} and uses the country/location
 * tuple as a datacenter and the availability zone as a rack
 *
 * This snitch is deprecated, and it is eligible for the removal in the next major release of Cassandra.
 *
 * @deprecated See CASSANDRA-18438
 */
@Deprecated(since = "5.0")
public class CloudstackSnitch extends AbstractCloudMetadataServiceSnitch
{
    public CloudstackSnitch() throws IOException
    {
        this(new SnitchProperties(new Properties()));
    }

    public CloudstackSnitch(SnitchProperties snitchProperties) throws IOException
    {
        super( new CloudstackLocationProvider(snitchProperties));
        logger.warn("{} is deprecated and not actively maintained. It will be removed in the next " +
                    "major version of Cassandra.", CloudstackSnitch.class.getName());
    }

    public CloudstackSnitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(new CloudstackLocationProvider(connector));
        logger.warn("{} is deprecated and not actively maintained. It will be removed in the next " +
                    "major version of Cassandra.", CloudstackSnitch.class.getName());
    }
}
